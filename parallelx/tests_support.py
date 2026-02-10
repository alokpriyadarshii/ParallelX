"""Test helpers (kept in package so they are importable in subprocess workers)."""

from __future__ import annotations

import threading

_lock = threading.Lock()
_seen = False


def flaky_once() -> int:
    """Fail the first time, succeed the second time (in-process)."""
    global _seen
    with _lock:
        if not _seen:
            _seen = True
            raise RuntimeError("boom")
    return 123
