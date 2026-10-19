"""chrMT transcripts translate with the vertebrate mitochondrial code (vepyr#200).

Twelve ClinVar 20261011 records in MT-ND1, one per distinct amino-acid change
that the standard and mitochondrial codes disagree on (ATA Met, AGA/AGG stop,
TGA Trp). The golden is Ensembl VEP 116.0 --everything --merged output; VEP
116.2 writes byte-identical bodies for these records. See prepare.py.
"""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

import vepyr


FIXTURE = Path(__file__).parent / "data" / "mt_codon_table_merged"


def body(path):
    return b"".join(
        line
        for line in path.read_bytes().splitlines(keepends=True)
        if not line.startswith(b"#")
    )


def codon_tables(cache_dir):
    table = pq.read_table(
        Path(cache_dir) / "transcript" / "chrMT.parquet", columns=["codon_table"]
    )
    return table.column("codon_table").to_pylist()


def annotate(cache_dir, output):
    vepyr.annotate(
        str(FIXTURE / "input.vcf"),
        str(cache_dir),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        expected_cache_version="116",
        output_vcf=str(output),
        show_progress=False,
    )


def test_published_cache_without_codon_table_uses_table_2_on_mt(tmp_path):
    # Caches converted before the reader fix carry codon_table null; the engine
    # resolves a null on a mitochondrial contig to table 2.
    assert set(codon_tables(FIXTURE / "published")) == {None}
    output = tmp_path / "output.vcf"
    annotate(FIXTURE / "published", output)
    assert body(output) == body(FIXTURE / "vep116.golden.vcf")


@pytest.mark.parametrize("workers", [1, 2])
def test_rebuilt_cache_carries_codon_table_2_on_mt(tmp_path, workers):
    cache_root = tmp_path / "converted"
    vepyr.build_cache(
        116,
        str(cache_root),
        cache_type="merged",
        chroms=["MT"],
        local_cache=str(FIXTURE / "raw" / "116_GRCh38_merged"),
        partitions=1,
        show_progress=False,
    )
    cache_dir = cache_root / "116_GRCh38_merged"
    tables = codon_tables(cache_dir)
    assert tables and set(tables) == {2}
    output = tmp_path / "output.vcf"
    vepyr.annotate(
        str(FIXTURE / ("input.vcf.gz" if workers > 1 else "input.vcf")),
        str(cache_dir),
        reference_fasta=str(FIXTURE / "reference.fa.gz"),
        expected_cache_version="116",
        output_vcf=str(output),
        workers=workers,
        show_progress=False,
    )
    assert body(output) == body(FIXTURE / "vep116.golden.vcf")
