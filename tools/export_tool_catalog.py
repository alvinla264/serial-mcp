#!/usr/bin/env python3
"""Export canonical MCP tool metadata from the Python frontend for Rust builds."""
import asyncio
import copy
import inspect
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import serial_mcp_client


def _parameter_docs(function) -> dict[str, str]:
    """Read Google-style Args docs so FastMCP 2 exports the same metadata as 4.x."""
    lines = (inspect.getdoc(function) or "").splitlines()
    start = next((i + 1 for i, line in enumerate(lines) if line.strip() == "Args:"), None)
    if start is None:
        return {}

    docs: dict[str, str] = {}
    name = None
    parts: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            continue
        indentation = len(line) - len(line.lstrip())
        content = line.strip()
        if indentation <= 4 and ":" in content:
            if name is not None:
                docs[name] = "\n".join(parts).strip()
            name, description = content.split(":", 1)
            name = name.strip()
            parts = [description.strip()] if description.strip() else []
        elif name is not None and indentation > 4:
            parts.append(content)
        else:
            break
    if name is not None:
        docs[name] = "\n".join(parts).strip()
    return docs


def _close_object_schemas(schema: dict) -> None:
    if schema.get("type") == "object":
        schema.setdefault("additionalProperties", False)
    for value in schema.values():
        if isinstance(value, dict):
            _close_object_schemas(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _close_object_schemas(item)


def _fastmcp2_metadata(tool) -> tuple[str, dict]:
    """Normalize FastMCP 2's sparse export to the MCP metadata emitted by 4.x."""
    description = tool.description or ""
    args_section = description.find("\n\nArgs:\n")
    if args_section >= 0:
        description = description[:args_section]
    schema = copy.deepcopy(tool.parameters)
    docs = _parameter_docs(tool.fn)
    if docs:
        for name, value in docs.items():
            if name in schema.get("properties", {}):
                schema["properties"][name]["description"] = value
    _close_object_schemas(schema)
    return description, schema


async def main() -> None:
    if hasattr(serial_mcp_client.mcp, "get_tools"):
        tools = await serial_mcp_client.mcp.get_tools()
        tools = tools.values() if isinstance(tools, dict) else tools
        metadata = {tool.name: _fastmcp2_metadata(tool) for tool in tools}
    else:
        tools = await serial_mcp_client.mcp.list_tools()
        metadata = {tool.name: (tool.description, tool.parameters) for tool in tools}
    catalog = [
        {
            "name": tool.name,
            "description": metadata[tool.name][0],
            "inputSchema": metadata[tool.name][1],
        }
        for tool in tools
    ]
    output = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    target = ROOT / "rust-client" / "tools.json"
    if "--check" in sys.argv:
        if not target.exists() or target.read_text() != output:
            raise SystemExit("Rust tool catalog is stale; run tools/export_tool_catalog.py")
    else:
        target.write_text(output)


if __name__ == "__main__":
    asyncio.run(main())
