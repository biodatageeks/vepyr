"""Compression- and index-aware VCF helpers.

This module knows about gzip framing, bgzf, and tabix. It knows nothing about
CSQ fields or annotation profiles, and must import without the vepyr native
extension present.
"""

import gzip
import json
import os
import subprocess
import sys

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


def detect_contigs(vcf):
    """Return contigs that actually carry records, in index (coordinate) order.

    Reads the tabix index, never the ##contig headers: on the real HG002 VEP
    outputs the index lists 22 contigs while the header lists 195 (the whole
    GRCh38 primary assembly plus scaffolds and alts). Returns [] when the file
    has no usable index, so callers can fall back.
    """
    if not vcf.endswith(GZIP_SUFFIXES) or not os.path.exists(vcf + ".tbi"):
        return []
    result = subprocess.run(["tabix", "-l", vcf], capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def contig_aliases(chrom):
    """Return {chrom, with-prefix, without-prefix} so 'chr22' also matches '22'."""
    bare = chrom[3:] if chrom.startswith("chr") else chrom
    return {chrom, bare, f"chr{bare}"}


def normalize_vcf(vcf, out_dir):
    """Normalize with `bcftools norm -m -both`, then bgzip and index.

    Output is shared by every contig of one release. A `normalized.source.json`
    sidecar records the input path, size, and mtime; a mismatch forces a re-run
    so changing --vcf cannot silently reuse a stale decomposition.
    """
    os.makedirs(out_dir, exist_ok=True)
    norm_vcf = os.path.join(out_dir, "normalized.vcf")
    norm_vcf_gz = norm_vcf + ".gz"
    sidecar_path = os.path.join(out_dir, "normalized.source.json")

    stat = os.stat(vcf)
    source = {
        "path": os.path.abspath(vcf),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
    }

    if os.path.exists(norm_vcf_gz) and os.path.exists(sidecar_path):
        with open(sidecar_path) as f:
            previous = json.load(f)
        if previous == source:
            print(f"  Using existing {norm_vcf_gz}")
            ensure_tabix_index(norm_vcf_gz)
            return norm_vcf_gz
        print(
            f"  Source changed ({previous.get('path')} -> {source['path']}), "
            "re-normalizing"
        )

    print(f"  Normalizing {os.path.basename(vcf)} (bcftools norm -m -both) ...")
    result = subprocess.run(
        ["bcftools", "norm", "-m", "-both", "-o", norm_vcf, vcf],
        capture_output=True,
        text=True,
    )
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(f"bcftools norm failed: {result.stderr}")

    subprocess.run(["bgzip", "-f", norm_vcf], check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", norm_vcf_gz], check=True)
    with open(sidecar_path, "w") as f:
        json.dump(source, f, indent=2)
    print(f"  Created {norm_vcf_gz}")
    return norm_vcf_gz


def slice_contig(vcf_gz, chrom, out_dir):
    """Extract one contig from an indexed VCF into a bgzf + tabix-indexed slice."""
    os.makedirs(out_dir, exist_ok=True)
    out_vcf = os.path.join(out_dir, f"input_{chrom}.vcf")
    out_gz = out_vcf + ".gz"
    if os.path.exists(out_gz) and os.path.exists(out_gz + ".tbi"):
        print(f"  Using existing {out_gz}")
        return out_gz

    ensure_tabix_index(vcf_gz)
    header = subprocess.run(
        ["tabix", "-H", vcf_gz], capture_output=True, check=True
    ).stdout

    body = b""
    for candidate in contig_aliases(chrom):
        result = subprocess.run(["tabix", vcf_gz, candidate], capture_output=True)
        if result.returncode == 0 and result.stdout:
            body = result.stdout
            break
    if not body:
        raise SystemExit(
            f"Error: tabix found no records for {chrom} in {vcf_gz}. "
            f"Available contigs: {', '.join(detect_contigs(vcf_gz)) or '(none)'}"
        )

    with open(out_vcf, "wb") as f:
        f.write(header)
        f.write(body)
    subprocess.run(["bgzip", "-f", out_vcf], check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", out_gz], check=True)
    print(f"  Created {out_gz}")
    return out_gz


def slice_vep(vep_vcf, chrom, out_dir, suffix, force=False):
    """Extract one contig from a VEP reference VCF, plain or block-gzipped.

    Uses `tabix` when the reference is indexed, which turns a full scan of a
    multi-gigabyte reference into a seek. Falls back to a streaming linear scan
    through open_text() otherwise -- the previous implementation used a bare
    open() here and raised UnicodeDecodeError on any block-gzipped reference.
    """
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"vep_{chrom}_{suffix}.vcf")
    if os.path.exists(out_path) and not force:
        print(f"  Using existing {out_path}")
        return out_path

    indexed = vep_vcf.endswith(GZIP_SUFFIXES) and os.path.exists(vep_vcf + ".tbi")
    if indexed:
        print(f"  Extracting {chrom} from VEP reference via tabix ...")
        header = subprocess.run(
            ["tabix", "-H", vep_vcf], capture_output=True, check=True
        ).stdout
        body = b""
        for candidate in contig_aliases(chrom):
            result = subprocess.run(["tabix", vep_vcf, candidate], capture_output=True)
            if result.returncode == 0 and result.stdout:
                body = result.stdout
                break
        with open(out_path, "wb") as f:
            f.write(header)
            f.write(body)
        n = body.count(b"\n")
    else:
        if vep_vcf.endswith(GZIP_SUFFIXES):
            print(
                f"  Note: {os.path.basename(vep_vcf)} is compressed but unindexed; "
                "streaming instead of seeking",
                file=sys.stderr,
            )
        print(f"  Extracting {chrom} from VEP reference by scan ...")
        targets = contig_aliases(chrom)
        n = 0
        with open_text(vep_vcf) as fin, open(out_path, "w") as fout:
            for line in fin:
                if line.startswith("#"):
                    fout.write(line)
                elif line.split("\t", 1)[0] in targets:
                    fout.write(line)
                    n += 1

    print(f"  Extracted {n:,} VEP records for {chrom}")
    return out_path
