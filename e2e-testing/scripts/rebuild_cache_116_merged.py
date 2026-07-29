#!/usr/bin/env python3
"""Compatibility wrapper for the release-general safe cache rebuild command."""

from __future__ import annotations

import sys

from rebuild_release_cache import main


if __name__ == "__main__":
    raise SystemExit(main(["--release", "116", *sys.argv[1:]]))
