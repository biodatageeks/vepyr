"""Flushed, rate-limited progress lines for preparation work, including log files."""

from __future__ import annotations

import time
from collections.abc import Callable


class Progress:
    """Report measured work, at most once a second plus the start and end."""

    def __init__(
        self,
        label: str,
        total: int | None,
        *,
        unit: str = "bytes",
        out: Callable[[str], None] | None = print,
    ) -> None:
        self.label, self.total, self.unit, self.out = label, total, unit, out
        self.done = 0
        self._last: tuple[int, int | None] | None = None
        self._at = 0.0
        self.update(0, force=True)

    def update(self, done: int, *, force: bool = False) -> None:
        self.done = done
        now = time.monotonic()
        state = (done, self.total)
        if self.out is None or state == self._last:
            return
        if not force and done != self.total and now - self._at < 1:
            return
        self._last, self._at = state, now
        if self.total is None:
            bar, amount = "?" * 20, f"{done:,} {self.unit} (total unknown)"
        else:
            percent = min(100, done * 100 // self.total) if self.total else 100
            filled = percent // 5
            bar = "#" * filled + "-" * (20 - filled)
            amount = f"{done:,}/{self.total:,} {self.unit} ({percent}%)"
        line = f"{self.label} [{bar}] {amount}"
        if self.out is print:
            print(line, flush=True)
        else:
            self.out(line)

    def finish(self) -> None:
        if self.total is None:
            self.total = self.done
        self.update(self.done, force=True)
