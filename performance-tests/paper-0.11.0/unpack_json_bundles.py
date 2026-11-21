"""Restore the per-run JSON files that were merged into one bundle per result set.

Each bundle is a JSON object with "bundle_of" (the directory name), "note" and
"files" ({path inside the directory: content}). Usage:

    python unpack_json_bundles.py BUNDLE.json [BUNDLE.json ...] [--out DIR]

Without --out, files are written next to the bundle, into the directory named by
"bundle_of". Content that was not valid JSON is stored as {"_unparsed_text": ...}
and written back as that text.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def unpack(bundle_path: Path, out: Path | None) -> int:
    bundle = json.loads(bundle_path.read_text())
    base = (out or bundle_path.parent) / bundle["bundle_of"]
    for rel, content in bundle["files"].items():
        target = base / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, dict) and set(content) == {"_unparsed_text"}:
            target.write_text(content["_unparsed_text"])
        else:
            target.write_text(json.dumps(content, indent=2) + "\n")
    return len(bundle["files"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("bundles", nargs="+", type=Path)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    for path in args.bundles:
        print(f"{path}: {unpack(path, args.out)} files")


if __name__ == "__main__":
    main()
