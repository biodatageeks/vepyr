#!/usr/bin/env python3
"""Canonical body-only MD5 comparison for two VCF files.

Only three normalizations are applied:
1. ignore VCF headers,
2. sort INFO fields by key,
3. sort CSQ entries within INFO/CSQ by the full CSQ string.

Then all canonical records are sorted as plain text and hashed. This compares
the VCF record multiset while ignoring presentation order.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import subprocess
import tempfile
from pathlib import Path


def open_text(path: Path):
    return gzip.open(path, "rt") if str(path).endswith(".gz") else path.open()


def canonical_info(info: str) -> str:
    if info in ("", "."):
        return "."

    fields = []
    for item in info.split(";"):
        key, sep, value = item.partition("=")
        if sep and key == "CSQ":
            value = ",".join(sorted(value.split(",")))
        rendered = key if not sep else f"{key}={value}"
        fields.append((key, rendered))

    return ";".join(rendered for _, rendered in sorted(fields))


def canonical_record(line: str) -> str:
    cols = line.rstrip("\n").split("\t")
    if len(cols) < 8:
        raise ValueError(f"VCF record has fewer than 8 columns: {line[:120]}")
    cols[7] = canonical_info(cols[7])
    return "\t".join(cols) + "\n"


def write_canonical_records(vcf: Path, out: Path) -> int:
    count = 0
    with open_text(vcf) as src, out.open("w") as dst:
        for line in src:
            if line.startswith("#"):
                continue
            dst.write(canonical_record(line))
            count += 1
    return count


def md5_of_sorted_file(path: Path, tmpdir: Path) -> str:
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    env["TMPDIR"] = str(tmpdir)

    md5 = hashlib.md5()
    proc = subprocess.Popen(["sort", str(path)], stdout=subprocess.PIPE, env=env)
    assert proc.stdout is not None
    for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b""):
        md5.update(chunk)
    if proc.wait() != 0:
        raise RuntimeError(f"sort failed for {path}")
    return md5.hexdigest()


def canonical_md5(vcf: Path, label: str, tmpdir: Path) -> tuple[int, str]:
    canonical_path = tmpdir / f"{label}.canonical.unsorted.vcf"
    count = write_canonical_records(vcf, canonical_path)
    return count, md5_of_sorted_file(canonical_path, tmpdir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("vep_vcf", type=Path)
    parser.add_argument("vepyr_vcf", type=Path)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="canonical-md5-vcf-") as tmp:
        tmpdir = Path(tmp)
        vep_count, vep_md5 = canonical_md5(args.vep_vcf, "vep", tmpdir)
        vepyr_count, vepyr_md5 = canonical_md5(args.vepyr_vcf, "vepyr", tmpdir)

    print("file\trecords\tcanonical_md5")
    print(f"vep\t{vep_count}\t{vep_md5}")
    print(f"vepyr\t{vepyr_count}\t{vepyr_md5}")
    print("MATCH" if vep_md5 == vepyr_md5 else "DIFF")
    return 0 if vep_md5 == vepyr_md5 else 1


if __name__ == "__main__":
    raise SystemExit(main())
