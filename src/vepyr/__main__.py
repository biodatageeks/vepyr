"""Allow `python -m vepyr` to run the command-line interface."""

import sys

from vepyr.cli import main

if __name__ == "__main__":
    sys.exit(main())
