#!/usr/bin/env python3
"""Rebuild the offline HG002 chr22 parity fixture.

The fixture reproduces Ensembl VEP 116 `--everything` on all 50,861 normalized
HG002 chr22 records (profile `ensembl` in e2e-testing/run_comparison.py). No
VEP output is stored: tests assert the record-body md5 VEP produced, which
`md5_concordance.py` computes in strict mode.

The cache is trimmed to the rows this one annotation reads, ~480 MB -> ~19 MB:

- variation: rows overlapping [POS - 1, POS + len(REF)] of an input record
- transcript / regulatory / motif: features named in the CSQ Feature column
  of the same input annotated against the full cache
- exon / translation_core: rows of the kept transcripts
- translation_sift: (transcript_uid << 32) | protein_position keys behind every
  CSQ entry that carries SIFT or PolyPhen

The trim is fitted to this input and the current engine. If an engine change
needs a feature outside it, rerun this script rather than widening the test.

Prerequisites: the project environment (vepyr, pyarrow, datafusion) and
samtools, bgzip, tabix on PATH.

Usage:
    uv run python tests/data/hg002_chr22/prepare.py

Env vars (all optional):
    DATA_VEPYR_DIR  default ~/workspace/data_vepyr
    VCF_SRC         normalized HG002 VCF (docs/testing-vep.md steps 1-3)
    CACHE_SRC       full release-116 ensembl Parquet cache
    FASTA_SRC       GRCh38 primary-assembly FASTA
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datafusion import SessionContext

# Ensembl VEP 116 --everything, strict body md5 of HG002 chr22
# (e2e-testing/reports/fast_chr22_ensembl_116_report.json).
EXPECTED_BODY_MD5 = "f0a0a7021c498d2b4e38c9caf5959f77"
EXPECTED_RECORDS = 50861

SCRIPT_DIR = Path(__file__).resolve().parent
DATA = Path(
    os.path.expanduser(os.environ.get("DATA_VEPYR_DIR", "~/workspace/data_vepyr"))
)
VCF_SRC = Path(os.environ.get("VCF_SRC", DATA / "input" / "HG002_normalized.vcf.gz"))
CACHE_SRC = Path(os.environ.get("CACHE_SRC", DATA / "cache" / "116_GRCh38_ensembl"))
FASTA_SRC = Path(
    os.environ.get(
        "FASTA_SRC", DATA / "input" / "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
    )
)

CHROM = "chr22"
FASTA_CHROM = "22"
SHARD = f"{CHROM}.parquet"
VARIATION_BUFFER = 1

# Same writer settings as tests/data/golden/_cache_prep.py: page statistics give
# the ColumnIndex/OffsetIndex the engine's point-lookup reader needs, and
# skip_arrow_metadata=false keeps the bio.vep.* cache identity.
COPY_OPTIONS = (
    "('compression' 'zstd(3)', 'dictionary_enabled' 'false', "
    "'statistics_enabled' 'page', 'data_pagesize_limit' '4096', "
    "'data_page_row_count_limit' '512', 'skip_arrow_metadata' 'false')"
)


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


def body_digest(vcf: Path) -> tuple[str, int]:
    """md5 over record lines as written, the md5_concordance.py strict digest."""
    md5, records = hashlib.md5(), 0
    with gzip.open(vcf, "rb") as fh:
        for line in fh:
            if not line.startswith(b"#"):
                md5.update(line)
                records += 1
    return md5.hexdigest(), records


def annotate(vcf: Path, cache: Path, fasta: Path, out: Path) -> None:
    import vepyr

    vepyr.annotate(
        str(vcf),
        str(cache),
        everything=True,
        reference_fasta=str(fasta),
        cache_format="parquet",
        output_vcf=str(out),
        workers=4,
    )


# -- inputs ------------------------------------------------------------------


def slice_input(dst: Path) -> None:
    """Header plus the chr22 records, as run_comparison.py's slice_contig cuts them."""
    plain = dst.with_suffix("")
    header = run(["tabix", "-H", str(VCF_SRC)], capture_output=True).stdout
    body = run(["tabix", str(VCF_SRC), CHROM], capture_output=True).stdout
    plain.write_bytes(header + body)
    run(["bgzip", "-f", str(plain)])
    run(["tabix", "-f", "-p", "vcf", str(dst)])


def slice_fasta(dst: Path) -> None:
    with open(dst, "wb") as out:
        faidx = subprocess.Popen(
            ["samtools", "faidx", str(FASTA_SRC), FASTA_CHROM], stdout=subprocess.PIPE
        )
        run(["bgzip", "-l", "9"], stdin=faidx.stdout, stdout=out)
        if faidx.wait() != 0:
            raise SystemExit("samtools faidx failed")
    run(["samtools", "faidx", str(dst)])  # writes .fai and .gzi


# -- trim ----------------------------------------------------------------------


def touched_features(full_vcf: Path):
    """Feature ids and SIFT/PolyPhen protein positions the full annotation emitted."""
    features = {"Transcript": set(), "RegulatoryFeature": set(), "MotifFeature": set()}
    sift_positions: dict[str, set[int]] = {}
    ix = None
    with gzip.open(full_vcf, "rt") as fh:
        for line in fh:
            if line.startswith("##INFO=<ID=CSQ"):
                names = line.split("Format: ", 1)[1].rstrip('">\n').split("|")
                ix = {name: i for i, name in enumerate(names)}
                continue
            if line.startswith("#"):
                continue
            info = line.split("\t", 8)[7]
            csq = next(kv[4:] for kv in info.split(";") if kv.startswith("CSQ="))
            for entry in csq.split(","):
                v = entry.split("|")
                ftype, feature = v[ix["Feature_type"]], v[ix["Feature"]]
                if ftype in features and feature:
                    features[ftype].add(feature)
                if ftype == "Transcript" and (v[ix["SIFT"]] or v[ix["PolyPhen"]]):
                    bounds = [
                        int(p)
                        for p in v[ix["Protein_position"]].split("-")
                        if p.isdigit()
                    ]
                    if bounds:
                        sift_positions.setdefault(feature, set()).update(
                            range(min(bounds), max(bounds) + 1)
                        )
    return features, sift_positions


def input_windows(vcf: Path):
    """Merged, sorted [lo, hi] windows around every input record."""
    windows = []
    with gzip.open(vcf, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            _, pos, _, ref, _ = line.split("\t", 4)
            pos = int(pos)
            windows.append(
                (pos - VARIATION_BUFFER, pos + len(ref) - 1 + VARIATION_BUFFER)
            )
    windows.sort()
    merged: list[list[int]] = []
    for lo, hi in windows:
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return (
        np.array([m[0] for m in merged], dtype=np.int64),
        np.array([m[1] for m in merged], dtype=np.int64),
    )


def trim_cache(full_vcf: Path, input_vcf: Path, dest: Path) -> None:
    ctx = SessionContext()
    features, sift_positions = touched_features(full_vcf)
    win_lo, win_hi = input_windows(input_vcf)

    def read(entity, predicate):
        pf = pq.ParquetFile(CACHE_SRC / entity / SHARD)
        parts = [predicate(pf.read_row_group(i)) for i in range(pf.num_row_groups)]
        return pa.concat_tables(parts).replace_schema_metadata(pf.schema_arrow.metadata)

    def write(entity, table):
        out_dir = dest / entity
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / SHARD
        out.unlink(missing_ok=True)
        if table.num_rows == 0:
            # DataFusion's writer panics on a zero-batch input.
            pq.write_table(table, str(out))
        else:
            ctx.register_record_batches("shard", [table.combine_chunks().to_batches()])
            ctx.sql(
                f"COPY shard TO '{out}' STORED AS PARQUET OPTIONS {COPY_OPTIONS}"
            ).collect()
            ctx.deregister_table("shard")
        source = json.loads((CACHE_SRC / entity / "chrom_manifest.json").read_text())
        label = next(e["chrom"] for e in source if e["dataset"] == SHARD)
        (out_dir / "chrom_manifest.json").write_text(
            json.dumps(
                [{"chrom": label, "dataset": SHARD, "rows": table.num_rows}], indent=2
            )
            + "\n"
        )
        print(
            f"  {entity}: {table.num_rows:,} rows ({out.stat().st_size / 1e6:.2f} MB)"
        )

    def overlapping(rg):
        start = rg["start"].to_numpy().astype(np.int64)
        end = rg["end"].to_numpy().astype(np.int64)
        lo, hi = (
            np.minimum(start, end),
            np.maximum(start, end),
        )  # insertions: start > end
        idx = np.searchsorted(win_lo, hi, side="right") - 1
        return rg.filter(pa.array((idx >= 0) & (win_hi[np.clip(idx, 0, None)] >= lo)))

    def in_set(column, ids, type_=pa.string()):
        value_set = pa.array(sorted(ids), type=type_)
        return lambda rg: rg.filter(pc.is_in(rg[column], value_set=value_set))

    tx_ids = features["Transcript"]
    transcripts = read("transcript", in_set("stable_id", tx_ids))

    write("variation", read("variation", overlapping))
    write("transcript", transcripts)
    write("exon", read("exon", in_set("transcript_id", tx_ids)))
    write("translation_core", read("translation_core", in_set("transcript_id", tx_ids)))
    write(
        "regulatory",
        read("regulatory", in_set("stable_id", features["RegulatoryFeature"])),
    )
    write("motif", read("motif", in_set("motif_id", features["MotifFeature"])))

    uid_of = dict(
        zip(
            transcripts["stable_id"].to_pylist(),
            transcripts["transcript_uid"].to_pylist(),
        )
    )
    keys = {(uid_of[tx] << 32) | p for tx, ps in sift_positions.items() for p in ps}
    write(
        "translation_sift", read("translation_sift", in_set("key", keys, pa.uint64()))
    )


def main() -> None:
    for path in (VCF_SRC, CACHE_SRC, FASTA_SRC):
        if not path.exists():
            raise SystemExit(f"missing source: {path}")

    input_vcf = SCRIPT_DIR / "input_chr22.vcf.gz"
    fasta = SCRIPT_DIR / "chr22.fa.gz"
    cache = SCRIPT_DIR / "cache"

    print(f"1. Slicing {CHROM} from {VCF_SRC}")
    slice_input(input_vcf)
    print(f"2. Slicing FASTA contig {FASTA_CHROM} from {FASTA_SRC}")
    slice_fasta(fasta)

    with tempfile.TemporaryDirectory() as tmp:
        full_vcf = Path(tmp) / "full.vcf.gz"
        print(f"3. Annotating against the full cache {CACHE_SRC}")
        annotate(input_vcf, CACHE_SRC, fasta, full_vcf)
        digest = body_digest(full_vcf)
        if digest != (EXPECTED_BODY_MD5, EXPECTED_RECORDS):
            raise SystemExit(f"full-cache run no longer matches VEP: {digest}")

        print("4. Trimming the cache")
        trim_cache(full_vcf, input_vcf, cache)

        trimmed_vcf = Path(tmp) / "trimmed.vcf.gz"
        print("5. Annotating against the trimmed cache")
        annotate(input_vcf, cache, fasta, trimmed_vcf)
        digest = body_digest(trimmed_vcf)
        if digest != (EXPECTED_BODY_MD5, EXPECTED_RECORDS):
            raise SystemExit(f"trimmed cache does not reproduce VEP: {digest}")

    size = sum(f.stat().st_size for f in SCRIPT_DIR.rglob("*") if f.is_file())
    print(
        f"\nDone. body md5 {EXPECTED_BODY_MD5} reproduced; fixture {size / 1e6:.1f} MB"
    )


if __name__ == "__main__":
    sys.exit(main())
