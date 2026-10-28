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


DEVICE_TOOLS = {
    "connect", "disconnect", "send_command", "read_output", "clear_output",
    "set_prompt_pattern", "login", "enter_bootloader", "upload_file",
    "download_file", "view_io",
}
REMOVED_TOOLS = {"attach", "tio_info", "lease_acquire", "lease_release", "lease_status"}


def _shared_app_catalog(catalog: list[dict]) -> list[dict]:
    """Adapt legacy-oracle metadata to the supported daemon-v2 MCP surface."""
    tools = {item["name"]: copy.deepcopy(item) for item in catalog
             if item["name"] not in REMOVED_TOOLS}
    connect = tools["connect"]
    connect["description"] = (
        "Open or join the selected serial device and request control. "
        "If another client controls it, join read-only and report the actual baudrate. "
        "The first opener chooses the baudrate; reconnect explicitly to reacquire control."
    )
    connect["inputSchema"]["properties"] = {
        "port": {"type": "string", "description": "Explicit serial device path."},
        "baudrate": {"type": "integer", "default": 115200,
                     "description": "Requested rate if this call opens the device; an existing connection keeps its actual rate."},
    }
    connect["inputSchema"]["required"] = ["port", "baudrate"]
    connect["inputSchema"]["additionalProperties"] = False

    tools["disconnect"]["description"] = (
        "Leave this client's membership of the selected port. Other participants "
        "and connected devices are unaffected."
    )
    tools["read_output"]["description"] += (
        " Uses this client's independent cursor and does not acquire or renew control."
    )
    tools["view_io"]["description"] = tools["view_io"]["description"].replace(
        "attached\nsession users", "human participants"
    )
    tools["file_transfer_capabilities"]["description"] = (
        "Describe supported Linux and U-Boot transfer methods, limits, prerequisites, "
        "and verification. Human input remains unrestricted and may interleave with "
        "a transfer; coordinate before starting operations that require an idle console."
    )
    for name in {"send_command", "clear_output", "set_prompt_pattern", "login",
                 "enter_bootloader", "upload_file", "download_file"}:
        tools[name]["description"] += (
            " Requires current control. If control is lost or expired, call "
            "connect(port, baudrate) explicitly; this call never acquires control."
        )
    for name in DEVICE_TOOLS - {"connect"}:
        schema = tools[name]["inputSchema"]
        schema.setdefault("properties", {})["port"] = {
            "type": "string", "description": "Explicit serial device path."
        }
        schema["required"] = list(dict.fromkeys(["port", *schema.get("required", [])]))
        if name in {"upload_file", "download_file"}:
            schema["properties"].pop("confirm_shared_quiescence", None)
            description = tools[name]["description"]
            for obsolete in (
                "Direct-owned sessions pause their own socket/pty writers during transfer.\n",
                "Attach mode requires confirm_shared_quiescence=true after all other tio users\n"
                "agree to stay idle; raw tio sockets cannot enforce arbitration. ",
            ):
                description = description.replace(obsolete, "")
            tools[name]["description"] = description

    tools["set_baudrate"] = {
        "name": "set_baudrate",
        "description": "Change the baudrate of a connected device. Requires current control; applies to all participants immediately.",
        "inputSchema": {
            "type": "object", "additionalProperties": False,
            "properties": {
                "port": {"type": "string", "description": "Explicit serial device path."},
                "baudrate": {"type": "integer", "description": "New baudrate accepted by the serial driver."},
            },
            "required": ["port", "baudrate"],
        },
    }
    ordered = [tools[item["name"]] for item in catalog
               if item["name"] in tools and item["name"] != "set_baudrate"]
    ordered.append(tools["set_baudrate"])
    return ordered


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
    catalog = _shared_app_catalog(catalog)
    output = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    target = ROOT / "rust-client" / "tools.json"
    if "--check" in sys.argv:
        if not target.exists() or target.read_text() != output:
            raise SystemExit("Rust tool catalog is stale; run tools/export_tool_catalog.py")
    else:
        target.write_text(output)


if __name__ == "__main__":
    asyncio.run(main())
