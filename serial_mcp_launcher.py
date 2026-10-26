"""Tiny console-script trampoline to the bundled native MCP frontend."""
from __future__ import annotations

import os
from pathlib import Path
import sys


def main() -> None:
    """Replace this process with the ready-built Rust frontend executable."""
    executable = Path(__file__).resolve().with_name("serial-mcp-rust-client")
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError(f"the bundled serial-mcp executable is missing: {executable}")
    os.execv(executable, [str(executable), *sys.argv[1:]])
