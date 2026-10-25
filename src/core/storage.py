"""Shared path contract: relative paths beneath TARGET_ROOT."""

from pathlib import Path
from core.config import TARGET_ROOT


def target_path(relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Target path must be relative and stay inside storage")
    root = TARGET_ROOT.resolve()
    path = (root / relative).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError("Target path escapes storage root")
    return path
