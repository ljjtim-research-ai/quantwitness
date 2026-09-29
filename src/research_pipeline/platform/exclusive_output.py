"""不覆盖既有文件的同目录原子文本发布。"""

from __future__ import annotations

import os
from pathlib import Path
import uuid


def write_text_exclusive_atomic(
    destination: str | Path,
    content: str,
) -> Path:
    """以 UTF-8 原子发布文本；目标已存在时保持原字节不变。"""

    path = Path(destination).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"输出已存在: {path}")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise FileExistsError(f"输出已存在: {path}") from exc
        temporary.unlink()
    finally:
        if temporary.exists():
            temporary.unlink()
    return path


__all__ = ["write_text_exclusive_atomic"]
