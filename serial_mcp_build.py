"""PEP 517 wrapper that builds and installs the native Linux MCP executable."""
from __future__ import annotations

import base64
import csv
import hashlib
import os
from pathlib import Path
import platform
import subprocess
import sysconfig
import tempfile
import zipfile

from hatchling.build import build_editable as _build_editable
from hatchling.build import build_sdist as _build_sdist
from hatchling.build import build_wheel as _build_wheel
from hatchling.build import prepare_metadata_for_build_wheel as _prepare_metadata

ROOT = Path(__file__).resolve().parent
BINARY = ROOT / "build" / "rust-target" / "release" / "serial-mcp-rust-client"


def _build_binary() -> None:
    if platform.system() != "Linux":
        raise RuntimeError("serial-mcp Rust frontend wheels are supported on Linux/WSL only")
    subprocess.run(
        ["cargo", "build", "--release", "--locked", "--manifest-path", str(ROOT / "rust-client" / "Cargo.toml"), "--target-dir", str(ROOT / "build" / "rust-target")],
        cwd=ROOT,
        check=True,
    )
    if not BINARY.is_file():
        raise RuntimeError(f"cargo succeeded but executable is missing: {BINARY}")


def _record_line(path: str, data: bytes) -> tuple[str, str, str]:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode("ascii")
    return path, f"sha256={digest}", str(len(data))


def _inject_executable(wheel_path: Path) -> str:
    with zipfile.ZipFile(wheel_path, "r") as source:
        entries = [(item, source.read(item.filename)) for item in source.infolist()]
    record_name = next(name for name, _ in entries if name.filename.endswith(".dist-info/RECORD"))
    wheel_stem = wheel_path.name[:-4]
    distribution_version = "_".join(wheel_stem.split("-")[:2])
    script_name = f"{distribution_version}.data/scripts/serial-mcp"
    binary_name = f"{distribution_version}.data/data/bin/serial-mcp-rust-client"
    binary = BINARY.read_bytes()
    launcher = b"""#!/bin/sh
case \"$0\" in
  */*) _launcher=$0 ;;
  *) _launcher=$(command -v \"$0\") ;;
esac
case \"$_launcher\" in
  */*) _launcher_dir=${_launcher%/*} ;;
  *) _launcher_dir=. ;;
esac
exec \"$_launcher_dir/serial-mcp-rust-client\" \"$@\"
"""
    rows: list[tuple[str, str, str]] = []
    wheel_metadata = next(name for name, _ in entries if name.filename.endswith(".dist-info/WHEEL"))
    with tempfile.NamedTemporaryFile(dir=wheel_path.parent, suffix=".whl", delete=False) as temp:
        replacement = Path(temp.name)
    try:
        with zipfile.ZipFile(replacement, "w", compression=zipfile.ZIP_DEFLATED) as target:
            for item, data in entries:
                if item.filename == record_name.filename:
                    continue
                if item.filename == wheel_metadata.filename:
                    platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
                    metadata = data.decode("utf-8").replace("Root-Is-Purelib: true", "Root-Is-Purelib: false")
                    metadata = "\n".join(line for line in metadata.splitlines() if not line.startswith("Tag: "))
                    data = (metadata + f"\nTag: py3-none-{platform_tag}\n").encode()
                target.writestr(item, data)
                rows.append(_record_line(item.filename, data))
            launcher_info = zipfile.ZipInfo(script_name)
            launcher_info.create_system = 3
            launcher_info.external_attr = (0o100755 << 16)
            target.writestr(launcher_info, launcher)
            rows.append(_record_line(script_name, launcher))
            data_launcher_name = f"{distribution_version}.data/data/bin/serial-mcp"
            data_launcher_info = zipfile.ZipInfo(data_launcher_name)
            data_launcher_info.create_system = 3
            data_launcher_info.external_attr = (0o100755 << 16)
            target.writestr(data_launcher_info, launcher)
            rows.append(_record_line(data_launcher_name, launcher))
            binary_info = zipfile.ZipInfo(binary_name)
            binary_info.create_system = 3
            binary_info.external_attr = (0o100755 << 16)
            binary_info.compress_type = zipfile.ZIP_STORED
            target.writestr(binary_info, binary)
            rows.append(_record_line(binary_name, binary))
            record = ("\n".join(",".join(row) for row in rows) + f"\n{record_name.filename},,\n").encode()
            target.writestr(record_name, record)
        platform_tag = sysconfig.get_platform().replace("-", "_").replace(".", "_")
        new_name = wheel_path.name.replace("-py3-none-any.whl", f"-py3-none-{platform_tag}.whl")
        new_path = wheel_path.with_name(new_name)
        os.replace(replacement, new_path)
        wheel_path.unlink()
        return new_name
    finally:
        replacement.unlink(missing_ok=True)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _build_binary()
    filename = _build_wheel(wheel_directory, config_settings, metadata_directory)
    return _inject_executable(Path(wheel_directory) / filename)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _build_binary()
    filename = _build_editable(wheel_directory, config_settings, metadata_directory)
    return _inject_executable(Path(wheel_directory) / filename)


def build_sdist(sdist_directory, config_settings=None):
    return _build_sdist(sdist_directory, config_settings)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    return _prepare_metadata(metadata_directory, config_settings)


def get_requires_for_build_wheel(config_settings=None):
    from hatchling.build import get_requires_for_build_wheel as get_requires
    return get_requires(config_settings)


def get_requires_for_build_editable(config_settings=None):
    return get_requires_for_build_wheel(config_settings)


def prepare_metadata_for_build_editable(metadata_directory, config_settings=None):
    from hatchling.build import prepare_metadata_for_build_editable as prepare
    return prepare(metadata_directory, config_settings)


def get_requires_for_build_sdist(config_settings=None):
    from hatchling.build import get_requires_for_build_sdist as get_requires
    return get_requires(config_settings)
