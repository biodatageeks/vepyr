"""Stable exit codes for data comparison, installation and cache preparation."""

from __future__ import annotations

from enum import IntEnum

__all__ = ["Exit", "RunTestsError"]


class Exit(IntEnum):
    """One code per class of outcome reported by the Python CLI runner."""

    OK = 0
    USAGE = 2
    REVISION = 3
    INCOMPLETE = 4
    VERIFY = 5
    ENGINE = 6
    MISMATCH = 8
    """Data suite: all runs compared, at least one body md5 mismatched (#231)."""


class RunTestsError(Exception):
    """A failure with a definite :class:`Exit` code and a one-line message."""

    def __init__(self, code: Exit, message: str) -> None:
        super().__init__(message)
        self.code = code
