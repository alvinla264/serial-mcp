"""Compatibility console entrypoint for the install-time-built Rust TUI."""
from __future__ import annotations
import os
from pathlib import Path
import shutil
import sys
import sysconfig


def main() -> None:
    """Exec the terminal binary bundled at installation; never build at launch."""
    binary = Path(__file__).resolve().with_name("serial-mcp-terminal")
    if not binary.is_file():
        editable=Path(__file__).resolve().parent/"build"/"rust-target"/"release"/"serial-mcp-terminal"
        if editable.is_file(): binary=editable
    if not binary.is_file():
        installed=Path(sysconfig.get_paths()["purelib"])/"serial-mcp-terminal"
        if installed.is_file(): binary=installed
    if not binary.is_file():
        located = shutil.which("serial-mcp-terminal")
        if located and Path(located).resolve()!=Path(sys.argv[0]).resolve():
            binary = Path(located)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"the bundled Rust terminal executable is missing: {binary}")
    os.execv(str(binary), [str(binary), *sys.argv[1:]])


if __name__ == "__main__":
    main()
