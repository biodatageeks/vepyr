#!/usr/bin/env python3
"""Verify a plugin cache's invariants, profile its content, update its Hub card.

See docs/superpowers/specs/2026-09-05-plugin-cache-qa-profile-design.md and
e2e-testing/README.md for usage.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cache_qa.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
