#!/usr/bin/env python3
"""Compare vepyr annotation against an Ensembl VEP reference.

Replaces run_annotation_fast.py and run_annotation_fast_all.py. See
e2e-testing/README.md for usage.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
