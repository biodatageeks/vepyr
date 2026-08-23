#!/usr/bin/env python3
"""MD5 concordance between Ensembl VEP and vepyr VCF output.

Two modes, same digest pipeline:

  canonical (default)
      Normalizes the serialization differences that are known to be cosmetic
      before hashing: QUAL rendered numerically, INFO and FORMAT keys sorted,
      FORMAT keys that are missing in every sample dropped. Two files with the
      same canonical digest carry identical annotation content and differ only
      in how they were written out.

  strict
      Hashes the record bytes as-is. This is the eventual parity target: it
      passes only once vepyr reproduces VEP's serialization exactly.

Headers are hashed separately from the body in both modes, because VEP appends
run-specific provenance (``##VEP=`` carries a timestamp) that never matches and
is not meant to.

Exit status is 0 when every pair concords in the selected mode, 1 otherwise.

Examples
--------
Compare one directory of per-chromosome runs::

    ./md5_concordance.py --results-dir ../results/116

Compare a single pair, and explain what differs::

    ./md5_concordance.py --pair vep_chr21.vcf vepyr_chr21.vcf --explain

Gate a release on exact bytes::

    ./md5_concordance.py --results-dir ../results/116 --mode strict
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
import hashlib
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison import vcfio  # noqa: E402

# Each tool appends its own provenance to the input header: wall-clock time,
# absolute cache paths, tool and per-module versions. None of it can match byte
# for byte, and none of it is meant to, so both sides' lines are excluded from
# the header digest.
VOLATILE_HEADER_PREFIXES = (
    "##VEP=",
    "##VEP-command-line=",
    "##datafusion-bio-function-vep=",
    "##datafusion-bio-function-vep-command-line=",
)

MISSING = (".", "")

# Default per-chromosome layout under e2e-testing/results/<release>/:
#   fast_chr21/vep_chr21_merged.vcf
#   fast_chr21/vepyr_parquet_chr21_merged.vcf
DEFAULT_VEP_GLOB = "vep_*.vcf*"
DEFAULT_VEPYR_GLOB = "vepyr_*.vcf*"

_CHR_RE = re.compile(r"(chr(?:\d+|[XYM]|MT))", re.IGNORECASE)


class ConcordanceError(RuntimeError):
    """Raised when inputs cannot be paired or read."""


# --------------------------------------------------------------------------
# canonicalization
# --------------------------------------------------------------------------


def normalize_qual(value: str) -> str:
    """Render QUAL numerically so ``50`` and ``50.00`` collapse to one form."""
    if value in MISSING:
        return "."
    try:
        q = float(value)
    except ValueError:
        return value
    return str(int(q)) if q == int(q) else repr(q)


def canonical_record(line: str) -> str:
    """Canonicalize one VCF data line.

    Sorts INFO and FORMAT keys, normalizes QUAL, and drops FORMAT keys whose
    value is missing in every sample. Leaves every value untouched — this
    reorders and reformats, it never rewrites content.
    """
    cols = line.split("\t")
    if len(cols) < 8:
        return line

    cols[5] = normalize_qual(cols[5])
    cols[7] = ";".join(sorted(cols[7].split(";")))

    if len(cols) > 9:
        keys = cols[8].split(":")
        samples = [c.split(":") for c in cols[9:]]
        # A sample may truncate trailing keys; pad so key/value zips line up.
        samples = [s + ["."] * (len(keys) - len(s)) for s in samples]
        keep = sorted(
            (i for i in range(len(keys)) if any(s[i] not in MISSING for s in samples)),
            key=lambda i: keys[i],
        )
        cols[8] = ":".join(keys[i] for i in keep)
        for n, s in enumerate(samples):
            cols[9 + n] = ":".join(s[i] for i in keep)

    return "\t".join(cols)


# --------------------------------------------------------------------------
# digests
# --------------------------------------------------------------------------


@dataclass
class Digest:
    """Header and body digests for one VCF, plus a record count."""

    header: str
    body: str
    records: int
    header_lines: int


def digest_vcf(path: str, mode: str) -> Digest:
    """Hash a VCF's header and body separately under `mode`."""
    header = hashlib.md5()
    body = hashlib.md5()
    records = 0
    header_lines = 0

    with vcfio.open_text(path) as handle:
        for line in handle:
            if line.startswith("#"):
                if line.startswith(VOLATILE_HEADER_PREFIXES):
                    continue
                header_lines += 1
                header.update(line.encode())
                continue
            records += 1
            if mode == "strict":
                body.update(line.encode())
            else:
                body.update((canonical_record(line.rstrip("\n")) + "\n").encode())

    return Digest(header.hexdigest(), body.hexdigest(), records, header_lines)


# --------------------------------------------------------------------------
# explain
# --------------------------------------------------------------------------


def classify_difference(vep_line: str, vepyr_line: str) -> list[str]:
    """Name the ways two records differ, ignoring nothing."""
    names = ["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT"]
    a, b = vep_line.split("\t"), vepyr_line.split("\t")
    if len(a) != len(b):
        return [f"column count {len(a)} vs {len(b)}"]

    out: list[str] = []
    for i, (x, y) in enumerate(zip(a, b)):
        if x == y:
            continue
        name = names[i] if i < len(names) else f"SAMPLE{i - 8}"

        if name == "QUAL":
            same = normalize_qual(x) == normalize_qual(y)
            out.append("QUAL format" if same else "QUAL VALUE")
        elif name == "INFO":
            ax = {kv.split("=")[0]: kv for kv in x.split(";") if kv}
            by = {kv.split("=")[0]: kv for kv in y.split(";") if kv}
            if set(ax) != set(by):
                out.append(
                    f"INFO KEYS (-{sorted(set(ax) - set(by))} +{sorted(set(by) - set(ax))})"
                )
            elif any(ax[k] != by[k] for k in ax):
                out.append("INFO VALUES")
            else:
                out.append("INFO order")
        elif name == "FORMAT":
            ax, by = x.split(":"), y.split(":")
            if set(ax) != set(by):
                out.append(
                    f"FORMAT KEYS (-{sorted(set(ax) - set(by))} +{sorted(set(by) - set(ax))})"
                )
            else:
                out.append("FORMAT order")
        elif name.startswith("SAMPLE"):
            ma = dict(zip(a[8].split(":"), x.split(":")))
            mb = dict(zip(b[8].split(":"), y.split(":")))
            shared = set(ma) & set(mb)
            if any(ma[k] != mb[k] for k in shared):
                out.append(f"{name} VALUES")
            elif set(ma) != set(mb):
                out.append(f"{name} keys")
            else:
                out.append(f"{name} order")
        else:
            out.append(f"{name} VALUE")
    return out


def explain(vep: str, vepyr: str, limit: int) -> Counter:
    """Classify every differing record. Returns a histogram of difference kinds."""
    kinds: Counter = Counter()
    with vcfio.open_text(vep) as fa, vcfio.open_text(vepyr) as fb:
        rows_a = (ln.rstrip("\n") for ln in fa if not ln.startswith("#"))
        rows_b = (ln.rstrip("\n") for ln in fb if not ln.startswith("#"))
        for n, (x, y) in enumerate(zip(rows_a, rows_b), start=1):
            if x == y:
                kinds["identical"] += 1
                continue
            for kind in classify_difference(x, y):
                kinds[kind] += 1
            if limit and n >= limit:
                break
    return kinds


def diff_headers(vep: str, vepyr: str) -> tuple[list[str], list[str]]:
    """Return (only-in-VEP, only-in-vepyr) header lines, volatile ones excluded."""

    def load(path: str) -> list[str]:
        with vcfio.open_text(path) as handle:
            return [
                ln.rstrip("\n")
                for ln in handle
                if ln.startswith("#") and not ln.startswith(VOLATILE_HEADER_PREFIXES)
            ]

    a, b = load(vep), load(vepyr)
    sa, sb = set(a), set(b)
    return [ln for ln in a if ln not in sb], [ln for ln in b if ln not in sa]


# --------------------------------------------------------------------------
# pairing
# --------------------------------------------------------------------------


@dataclass
class Pair:
    """One VEP/vepyr file pair to compare."""

    label: str
    vep: str
    vepyr: str


@dataclass
class Result:
    """Outcome of comparing one pair."""

    pair: Pair
    vep: Digest
    vepyr: Digest
    notes: list[str] = field(default_factory=list)

    @property
    def body_match(self) -> bool:
        return self.vep.body == self.vepyr.body

    @property
    def header_match(self) -> bool:
        return self.vep.header == self.vepyr.header

    @property
    def count_match(self) -> bool:
        return self.vep.records == self.vepyr.records


def label_for(path: Path) -> str:
    """Derive a short label from a path, preferring an embedded contig name."""
    match = _CHR_RE.search(path.name) or _CHR_RE.search(path.parent.name)
    return match.group(1).lower() if match else path.parent.name or path.stem


def sort_key(label: str) -> tuple[int, int, str]:
    """Order labels so chr2 sorts before chr10 and chrX comes last."""
    match = _CHR_RE.fullmatch(label)
    if not match:
        return (2, 0, label)
    name = match.group(1)[3:].upper()
    return (0, int(name), "") if name.isdigit() else (1, 0, name)


def discover_pairs(results_dir: Path, vep_glob: str, vepyr_glob: str) -> list[Pair]:
    """Find VEP/vepyr pairs in `results_dir`, one per subdirectory."""
    pairs: list[Pair] = []
    candidates = sorted(p for p in results_dir.iterdir() if p.is_dir())
    if not candidates:
        candidates = [results_dir]

    for directory in candidates:
        vep = sorted(directory.glob(vep_glob))
        vepyr = sorted(directory.glob(vepyr_glob))
        if not vep or not vepyr:
            continue
        if len(vep) > 1 or len(vepyr) > 1:
            raise ConcordanceError(
                f"{directory}: ambiguous match — "
                f"vep={[p.name for p in vep]} vepyr={[p.name for p in vepyr]}. "
                "Narrow it with --vep-glob/--vepyr-glob."
            )
        pairs.append(Pair(label_for(vep[0]), str(vep[0]), str(vepyr[0])))

    pairs.sort(key=lambda p: sort_key(p.label))
    return pairs


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------


def compare(pair: Pair, mode: str) -> Result:
    """Digest both sides of `pair` and note any structural mismatch."""
    result = Result(pair, digest_vcf(pair.vep, mode), digest_vcf(pair.vepyr, mode))
    if not result.count_match:
        result.notes.append(
            f"record count {result.vep.records} vs {result.vepyr.records}"
        )
    if not result.header_match:
        result.notes.append(
            f"header differs ({result.vep.header_lines} vs "
            f"{result.vepyr.header_lines} lines, "
            "each side's own provenance lines excluded)"
        )
    return result


def print_table(results: list[Result], mode: str) -> None:
    width = max((len(r.pair.label) for r in results), default=5)
    print(f"\n  mode: {mode}    body digest over {len(results)} pair(s)\n")
    print(f"  {'':<{width}}  {'RECORDS':>10}  {'BODY MD5':<34}  BODY  HEADER")
    print(f"  {'-' * width}  {'-' * 10}  {'-' * 34}  ----  ------")
    for r in results:
        digest = (
            r.vep.body if r.body_match else f"{r.vep.body[:12]}…/{r.vepyr.body[:12]}…"
        )
        print(
            f"  {r.pair.label:<{width}}  {r.vep.records:>10,}  {digest:<34}  "
            f"{'ok' if r.body_match else 'DIFF':<4}  {'ok' if r.header_match else 'DIFF'}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--results-dir",
        help="Directory of per-contig run directories (e.g. ../results/116).",
    )
    source.add_argument(
        "--pair",
        nargs=2,
        metavar=("VEP_VCF", "VEPYR_VCF"),
        help="Compare exactly one VEP/vepyr file pair.",
    )
    parser.add_argument(
        "--mode",
        choices=("canonical", "strict"),
        default="canonical",
        help="canonical normalizes cosmetic serialization first (default); "
        "strict hashes record bytes as-is.",
    )
    parser.add_argument("--vep-glob", default=DEFAULT_VEP_GLOB)
    parser.add_argument("--vepyr-glob", default=DEFAULT_VEPYR_GLOB)
    parser.add_argument(
        "--explain",
        action="store_true",
        help="For each mismatching pair, classify how the records differ.",
    )
    parser.add_argument(
        "--explain-limit",
        type=int,
        default=0,
        metavar="N",
        help="Stop classifying after N records (0 = all).",
    )
    args = parser.parse_args(argv)

    try:
        if args.pair:
            vep, vepyr = args.pair
            pairs = [Pair(label_for(Path(vep)), vep, vepyr)]
        else:
            results_dir = Path(args.results_dir).expanduser().resolve()
            if not results_dir.is_dir():
                raise ConcordanceError(f"not a directory: {results_dir}")
            pairs = discover_pairs(results_dir, args.vep_glob, args.vepyr_glob)
            if not pairs:
                raise ConcordanceError(
                    f"no VEP/vepyr pairs under {results_dir} matching "
                    f"{args.vep_glob!r} and {args.vepyr_glob!r}"
                )

        results = [compare(pair, args.mode) for pair in pairs]
    except ConcordanceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print_table(results, args.mode)

    mismatched = [r for r in results if not r.body_match]

    for r in results:
        if r.notes:
            print(f"\n  {r.pair.label}:")
            for note in r.notes:
                print(f"    - {note}")
            if not r.header_match:
                only_vep, only_vepyr = diff_headers(r.pair.vep, r.pair.vepyr)
                for line in only_vep[:8]:
                    print(f"      VEP only  : {line[:110]}")
                for line in only_vepyr[:8]:
                    print(f"      vepyr only: {line[:110]}")

    if args.explain and mismatched:
        for r in mismatched:
            print(f"\n  {r.pair.label} — record differences:")
            for kind, count in explain(
                r.pair.vep, r.pair.vepyr, args.explain_limit
            ).most_common():
                print(f"    {count:>12,}  {kind}")

    total = sum(r.vep.records for r in results)
    if mismatched:
        print(
            f"\n  FAIL: {len(mismatched)}/{len(results)} pair(s) differ "
            f"in {args.mode} mode ({total:,} records compared)\n"
        )
        return 1

    print(
        f"\n  PASS: {len(results)}/{len(results)} pair(s) concord "
        f"in {args.mode} mode ({total:,} records compared)\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
