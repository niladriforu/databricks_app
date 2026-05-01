"""Resolve repository root for config/SQL paths."""

from __future__ import annotations

import os
from pathlib import Path


def repo_root() -> Path:
    """Directory that contains ``config/`` and ``src/``.

    Uses ``paths.py``'s ``__file__`` when running as a normal module. In contexts
    where ``__file__`` is undefined (e.g. code pasted into a notebook cell, or
    ``exec``), uses ``$DATABRICKS_APP_ROOT`` if set, otherwise ``Path.cwd()``.
    """
    try:
        here = Path(__file__).resolve()
    except NameError:
        env = os.environ.get("DATABRICKS_APP_ROOT", "").strip()
        if env:
            return Path(env).resolve()
        return Path.cwd().resolve()
    return here.parent.parent
