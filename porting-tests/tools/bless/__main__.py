"""``python -m bless`` — what the repository-root ``./bless`` wrapper executes."""

from __future__ import annotations

from bless import cli

raise SystemExit(cli.main())
