"""Compression- and index-aware VCF helpers.

This module knows about gzip framing, bgzf, and tabix. It knows nothing about
CSQ fields or annotation profiles, and must import without the vepyr native
extension present.
"""

import gzip
import os
import subprocess

GZIP_SUFFIXES = (".gz", ".bgz", ".bgzf")


def open_text(path):
    """Open a VCF for text reading, transparently handling .gz (bgzf or plain gzip)."""
    if path.endswith(GZIP_SUFFIXES):
        return gzip.open(path, "rt")
    return open(path)


def is_bgzf(path):
    """Return True if `path` is BGZF (block-gzip): gzip magic plus a 'BC' subfield."""
    with open(path, "rb") as f:
        head = f.read(18)
    # gzip magic 1f 8b, deflate (08), FLG.FEXTRA set (bit 2), then an extra
    # field carrying the "BC" subfield id that marks bgzf blocks.
    return (
        len(head) >= 18
        and head[0:3] == b"\x1f\x8b\x08"
        and bool(head[3] & 0x04)
        and head[12:14] == b"BC"
    )


def count_data_lines(path):
    """Count non-header lines in a VCF (plain or .gz)."""
    n = 0
    with open_text(path) as f:
        for line in f:
            if not line.startswith("#"):
                n += 1
    return n


def ensure_tabix_index(vcf_gz):
    """Create a tabix index for `vcf_gz` if one is missing."""
    tbi = vcf_gz + ".tbi"
    if os.path.exists(tbi):
        return
    print(f"  Indexing (tabix) {os.path.basename(vcf_gz)} ...")
    subprocess.run(["tabix", "-p", "vcf", vcf_gz], check=True)


def ensure_bgzf(path, out_dir):
    """Return a bgzf-compressed, tabix-indexed copy of `path`.

    Already-compressed inputs are returned untouched. A plain VCF is copied into
    `out_dir` and block-gzipped there, so a read-only source directory is never
    written to. This is what lets --no-normalize accept an uncompressed VCF.
    """
    if path.endswith(GZIP_SUFFIXES):
        ensure_tabix_index(path)
        return path

    os.makedirs(out_dir, exist_ok=True)
    target = os.path.join(out_dir, os.path.basename(path))
    target_gz = target + ".gz"
    if os.path.exists(target_gz):
        ensure_tabix_index(target_gz)
        return target_gz

    print(f"  Input is plain text, block-gzipping {os.path.basename(path)} ...")
    with open(target_gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", path], stdout=fh, check=True)
    ensure_tabix_index(target_gz)
    return target_gz
