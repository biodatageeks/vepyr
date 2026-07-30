"""Compression- and index-aware VCF helpers.

This module knows about gzip framing, bgzf, and tabix. It knows nothing about
CSQ fields or annotation profiles, and must import without the vepyr native
extension present.
"""

import gzip
import json
import os
import re
import subprocess
import sys

GZIP_SUFFIXES = (".gz", ".bgz", ".bgzf")
_VEP_HEADER_VALUE_RE = re.compile(
    r'(?P<key>[A-Za-z0-9_-]+)=(?:"(?P<quoted>[^"]*)"|(?P<bare>\S+))'
)
_CACHE_RELEASE_RE = re.compile(r"(?:^|/)(?P<release>\d+)_GRCh\d+(?:$|/)")


def source_identity(path):
    """Return the filesystem identity used to bind inputs and derived files."""
    stat = os.stat(path)
    return {
        "path": os.path.abspath(path),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "mtime_ns": stat.st_mtime_ns,
    }


def _read_json(path):
    """Read a JSON marker, treating a missing or corrupt marker as a cache miss."""
    try:
        with open(path) as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError):
        return None


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


def _split_release_revision(value, field):
    """Split a VEP ``release.git-revision`` token and reject weak identities."""
    release, separator, revision = value.partition(".")
    if not separator or not release.isdigit() or not revision:
        raise ValueError(
            f"Malformed {field} value {value!r} in ##VEP header; "
            "expected <release>.<git-revision>"
        )
    return release, revision


def parse_vep_header(path):
    """Return the exact Ensembl/VEP/cache identity declared by a VCF.

    The comparison harness treats the reference VCF header as evidence, rather
    than inferring a release from its directory name. A comparison is not
    reproducible when this header is absent or does not include the VEP, API,
    cache, core, and variation identities.
    """
    header = None
    with open_text(path) as f:
        for line in f:
            if line.startswith("##VEP="):
                header = line.rstrip("\r\n")
                break
            if line.startswith("#CHROM"):
                break

    if header is None:
        raise ValueError(f"No ##VEP identity header found in {path}")

    values = {}
    for match in _VEP_HEADER_VALUE_RE.finditer(header[2:]):
        values[match.group("key")] = (
            match.group("quoted")
            if match.group("quoted") is not None
            else match.group("bare")
        )

    required = ("VEP", "API", "cache", "ensembl", "ensembl-variation")
    missing = [field for field in required if not values.get(field)]
    if missing:
        raise ValueError(
            f"Incomplete ##VEP identity header in {path}; missing " + ", ".join(missing)
        )

    cache_match = _CACHE_RELEASE_RE.search(values["cache"])
    if cache_match is None:
        raise ValueError(
            f"Cannot derive cache release from ##VEP cache={values['cache']!r} "
            f"in {path}; expected a <release>_GRCh<assembly> path component"
        )

    ensembl_release, ensembl_revision = _split_release_revision(
        values["ensembl"], "ensembl"
    )
    variation_release, variation_revision = _split_release_revision(
        values["ensembl-variation"], "ensembl-variation"
    )

    return {
        "vep_version": values["VEP"].removeprefix("v"),
        "api_version": values["API"].removeprefix("v"),
        "cache_version": cache_match.group("release"),
        "cache_path": values["cache"],
        "assembly": values.get("assembly"),
        "ensembl_release": ensembl_release,
        "ensembl_revision": ensembl_revision,
        "ensembl_variation_release": variation_release,
        "ensembl_variation_revision": variation_revision,
        "header": header,
    }


def validate_vep_reference_identity(identity, target):
    """Require a parsed reference identity to match one native support record."""
    expected = {
        "vep_version": target["vep_codebase_version"],
        "api_version": target["api_version"],
        "cache_version": target["cache_version"],
        "ensembl_release": target["api_version"],
        "ensembl_revision": target["ensembl_core_revision"],
        "ensembl_variation_release": target["api_version"],
        "ensembl_variation_revision": target["ensembl_variation_revision"],
    }
    mismatches = [
        f"{field}: reference={identity.get(field)!r}, expected={value!r}"
        for field, value in expected.items()
        if identity.get(field) != value
    ]
    assembly = identity.get("assembly")
    if assembly != "GRCh38" and not (
        isinstance(assembly, str) and assembly.startswith("GRCh38.")
    ):
        mismatches.append(
            f"assembly: reference={assembly!r}, expected GRCh38 or GRCh38.*"
        )
    if mismatches:
        raise ValueError(
            "VEP reference does not match the selected native support target: "
            + "; ".join(mismatches)
        )


def ensure_tabix_index(vcf_gz, source_marker=None):
    """Create or refresh the tabix index for `vcf_gz`.

    Without a marker, an existing index is reusable only when it is at least as
    new as the compressed VCF. ``ensure_bgzf`` additionally supplies a working
    directory marker so an edited or replaced ``--no-normalize`` input cannot
    retain a stale, newer-looking index.
    """
    tbi = vcf_gz + ".tbi"
    source = source_identity(vcf_gz)
    if os.path.exists(tbi):
        if source_marker is not None:
            if _read_json(source_marker) == source:
                return
        elif os.stat(tbi).st_mtime_ns >= source["mtime_ns"]:
            return

    print(f"  Indexing (tabix) {os.path.basename(vcf_gz)} ...")
    subprocess.run(["tabix", "-f", "-p", "vcf", vcf_gz], check=True)
    if source_marker is not None:
        os.makedirs(os.path.dirname(os.path.abspath(source_marker)), exist_ok=True)
        with open(source_marker, "w") as stream:
            json.dump(source, stream, indent=2)


def ensure_bgzf(path, out_dir):
    """Return a bgzf-compressed, tabix-indexed copy of `path`.

    Already-compressed inputs are returned untouched. A plain VCF is copied into
    `out_dir` and block-gzipped there, so a read-only source directory is never
    written to. This is what lets --no-normalize accept an uncompressed VCF.
    """
    if path.endswith(GZIP_SUFFIXES):
        os.makedirs(out_dir, exist_ok=True)
        source_marker = os.path.join(
            out_dir,
            os.path.basename(path) + ".tbi.source.json",
        )
        ensure_tabix_index(path, source_marker)
        return path

    os.makedirs(out_dir, exist_ok=True)
    target = os.path.join(out_dir, os.path.basename(path))
    target_gz = target + ".gz"
    sidecar_path = target_gz + ".source.json"
    source = source_identity(path)
    if (
        os.path.exists(target_gz)
        and os.path.exists(target_gz + ".tbi")
        and _read_json(sidecar_path) == source
    ):
        return target_gz

    print(f"  Input is plain text, block-gzipping {os.path.basename(path)} ...")
    with open(target_gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", path], stdout=fh, check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", target_gz], check=True)
    with open(sidecar_path, "w") as stream:
        json.dump(source, stream, indent=2)
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


def canonical_contig(chrom):
    """Return one canonical comparison spelling for a chr-prefixed alias."""
    bare = chrom[3:] if chrom.lower().startswith("chr") else chrom
    return f"chr{bare}"


def contig_aliases(chrom):
    """Return {chrom, with-prefix, without-prefix} so 'chr22' also matches '22'."""
    canonical = canonical_contig(chrom)
    return {chrom, canonical[3:], canonical}


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

    source = source_identity(vcf)

    if os.path.exists(norm_vcf_gz) and os.path.exists(sidecar_path):
        previous = _read_json(sidecar_path)
        if previous == source:
            print(f"  Using existing {norm_vcf_gz}")
            ensure_tabix_index(norm_vcf_gz)
            return norm_vcf_gz
        if previous is not None:
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


def slice_contig(vcf_gz, chrom, out_dir, force=False):
    """Extract one contig into a bgzf slice tied to its indexed source."""
    os.makedirs(out_dir, exist_ok=True)
    out_vcf = os.path.join(out_dir, f"input_{chrom}.vcf")
    out_gz = out_vcf + ".gz"
    sidecar_path = os.path.join(out_dir, f"input_{chrom}.source.json")
    source = source_identity(vcf_gz)
    previous = _read_json(sidecar_path)
    if (
        not force
        and previous == source
        and os.path.exists(out_gz)
        and os.path.exists(out_gz + ".tbi")
    ):
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
    with open(sidecar_path, "w") as stream:
        json.dump(source, stream, indent=2)
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
    sidecar_path = os.path.join(out_dir, f"vep_{chrom}_{suffix}.source.json")
    source = source_identity(vep_vcf)
    previous = _read_json(sidecar_path)
    if os.path.exists(out_path) and not force and previous == source:
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
        n = body.count(b"\n") + int(bool(body) and not body.endswith(b"\n"))
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

    with open(sidecar_path, "w") as stream:
        json.dump(source, stream, indent=2)
    print(f"  Extracted {n:,} VEP records for {chrom}")
    return out_path
