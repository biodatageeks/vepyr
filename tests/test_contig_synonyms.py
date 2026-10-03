"""Cache synonyms resolve annotation sources while input chromosome names survive."""

from pathlib import Path
import hashlib
import shutil
import subprocess
import sys

import polars as pl
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata
import vepyr


GOLDEN = Path(__file__).parent / "data" / "golden"
ACCESSION = "NC_000001.11"


@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    target = tmp_path_factory.mktemp("synonym_cache")
    copy_cache_with_source_metadata(GOLDEN / "cache", target, "ensembl", "115")
    (target / "chr_synonyms.txt").write_text(f"1 {ACCESSION}\n")
    return str(target)


def write_input(path, chromosomes):
    path.write_text(
        "##fileformat=VCFv4.2\n"
        + "".join(f"##contig=<ID={chrom}>\n" for chrom in chromosomes)
        + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        + "".join(
            f"{chrom}\t779047\tcase{i}\tG\tA\t.\t.\t.\n"
            for i, chrom in enumerate(chromosomes)
        )
    )
    return path


def indexed_input(path):
    if not (shutil.which("bgzip") and shutil.which("tabix")):
        pytest.skip("indexed input requires bgzip and tabix")
    compressed = path.with_suffix(".vcf.gz")
    with compressed.open("wb") as out:
        subprocess.run(["bgzip", "-c", str(path)], stdout=out, check=True)
    subprocess.run(["tabix", "-p", "vcf", str(compressed)], check=True)
    return compressed


def assert_output(path, chromosomes):
    lines = path.read_text().splitlines()
    header = next(line for line in lines if line.startswith("##INFO=<ID=CSQ,"))
    fields = header.split("Format: ", 1)[1].split('"', 1)[0].split("|")
    rows = {
        row[2]: row
        for line in lines
        if not line.startswith("#")
        for row in [line.split("\t")]
    }
    assert len(rows) == len(chromosomes)
    control = rows["case0"][7]
    assert control.startswith("CSQ=")
    for entry in control.removeprefix("CSQ=").split(","):
        values = entry.split("|")
        assert len(values) == len(fields)
        annotation = dict(zip(fields, values))
        assert annotation["Allele"] == "A"
        assert annotation["Existing_variation"] == "rs12028261"
        assert annotation["AF"] == "0.7728"
        assert annotation["MAX_AF"] == "1"
        assert annotation["Feature"]
    for i, chrom in enumerate(chromosomes):
        row = rows[f"case{i}"]
        assert row[:7] == [chrom, "779047", f"case{i}", "G", "A", ".", "."]
        assert row[7] == control


@pytest.mark.parametrize("mode", ["vcf", "cli", "indexed"])
@pytest.mark.parametrize(
    "chromosomes", [[ACCESSION], [ACCESSION, "chr1", "1"], ["chr1", ACCESSION, "1"]]
)
def test_contig_synonym_vcf_output(cache_dir, tmp_path, mode, chromosomes):
    source = write_input(tmp_path / "input.vcf", chromosomes)
    output = tmp_path / "output.vcf"
    if mode == "cli":
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "vepyr",
                "annotate",
                "-i",
                str(source),
                "-o",
                str(output),
                "--dir_cache",
                cache_dir,
                "--fasta",
                str(GOLDEN / "reference.fa"),
                "--everything",
                "--no_progress",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
    else:
        if mode == "indexed":
            source = indexed_input(source)
        vepyr.annotate(
            str(source),
            cache_dir,
            reference_fasta=str(GOLDEN / "reference.fa"),
            output_vcf=str(output),
            workers=2 if mode == "indexed" else 1,
            show_progress=False,
        )
    assert_output(output, chromosomes)


@pytest.mark.parametrize("include_csq", [False, True])
def test_contig_synonym_lazyframe_keeps_filter_and_projection(
    cache_dir, tmp_path, include_csq
):
    chromosomes = [ACCESSION, "chr1"]
    source = indexed_input(write_input(tmp_path / "input.vcf", chromosomes))
    fields = ["Allele", "Existing_variation", "AF", "MAX_AF", "Consequence", "HGVSc"]
    if include_csq:
        fields.append("CSQ")
    frame = vepyr.annotate(
        str(source),
        cache_dir,
        reference_fasta=str(GOLDEN / "reference.fa"),
        skip_csq=not include_csq,
        show_progress=False,
    )
    rows = {
        row["id"]: row
        for row in frame.select("id", "chrom", *fields).collect().to_dicts()
    }
    assert set(rows) == {"case0", "case1"}
    assert rows["case0"]["chrom"] == ACCESSION
    assert rows["case1"]["chrom"] == "chr1"
    assert rows["case1"]["Existing_variation"]
    assert rows["case1"]["AF"]
    for field in fields:
        assert rows["case0"][field] == rows["case1"][field], field
    selected = (
        frame.filter(pl.col("chrom") == ACCESSION)
        .select("id", "chrom", *fields)
        .collect()
        .to_dicts()
    )
    assert selected == [rows["case0"]]


def test_cache_identity_accepts_synonym(cache_dir):
    identity = vepyr.cache_contig_identity(cache_dir, ACCESSION)
    assert identity["cache_version"] == "115"
    assert identity["contig"] == ACCESSION


@pytest.mark.parametrize("metadata", [None, f"1 {ACCESSION}\n"])
def test_unresolved_accession_reports_no_cache_contig(tmp_path, metadata):
    cache = tmp_path / "cache"
    copy_cache_with_source_metadata(GOLDEN / "cache", cache, "ensembl", "115")
    if metadata is not None:
        (cache / "chr_synonyms.txt").write_text(metadata)
    accession = ACCESSION if metadata is None else "NC_999999.1"
    source = write_input(tmp_path / "input.vcf", [accession])
    with pytest.raises(RuntimeError, match="none of the VCF"):
        vepyr.annotate(
            str(source),
            str(cache),
            reference_fasta=str(GOLDEN / "reference.fa"),
            output_vcf=str(tmp_path / "output.vcf"),
            show_progress=False,
        )


@pytest.mark.parametrize("entrypoint", ["identity", "annotation"])
def test_invalid_synonym_metadata_reports_source_path(tmp_path, entrypoint):
    cache = tmp_path / "cache"
    copy_cache_with_source_metadata(GOLDEN / "cache", cache, "ensembl", "115")
    metadata = cache / "chr_synonyms.txt"
    metadata.write_bytes(b"1 \xff\n")
    with pytest.raises(RuntimeError, match="failed to read chromosome synonyms") as exc:
        if entrypoint == "identity":
            vepyr.cache_contig_identity(str(cache), "chr1")
        else:
            source = write_input(tmp_path / "input.vcf", ["chr1"])
            vepyr.annotate(
                str(source),
                str(cache),
                reference_fasta=str(GOLDEN / "reference.fa"),
                output_vcf=str(tmp_path / "output.vcf"),
                show_progress=False,
            )
    assert str(metadata) in str(exc.value)


@pytest.mark.parametrize("overwrite", [False, True])
def test_cache_conversion_removes_obsolete_synonyms(tmp_path, overwrite):
    native = tmp_path / "native"
    shutil.copytree(Path(__file__).parent / "data" / "ensembl_cache", native)
    cache_root = tmp_path / "converted"
    output = cache_root / "115_GRCh38_ensembl"
    metadata = native / "chr_synonyms.txt"
    metadata.write_text("22 NC_000022.11\n")
    kwargs = dict(
        release=115,
        cache_dir=str(cache_root),
        cache_type="ensembl",
        entity="variation",
        local_cache=str(native),
        partitions=1,
        show_progress=False,
    )
    vepyr.build_cache(**kwargs)
    assert (output / "chr_synonyms.txt").exists()
    metadata.unlink()
    vepyr.build_cache(**kwargs, overwrite=overwrite)
    assert not (output / "chr_synonyms.txt").exists()
    assert list(output.rglob("*.parquet"))


@pytest.mark.parametrize("existing", [False, True])
def test_cache_conversion_preserves_synonyms_without_rebuilding(tmp_path, existing):
    native = tmp_path / "native"
    shutil.copytree(Path(__file__).parent / "data" / "ensembl_cache", native)
    cache_root = tmp_path / "converted"
    output = cache_root / "115_GRCh38_ensembl"

    def build():
        return vepyr.build_cache(
            115,
            str(cache_root),
            cache_type="ensembl",
            entity="variation",
            local_cache=str(native),
            partitions=1,
            show_progress=False,
        )

    def shard_state():
        return {
            str(path): (
                path.stat().st_mtime_ns,
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            for path in output.rglob("*.parquet")
        }

    if existing:
        build()
    original = shard_state()
    synonyms = b"22 NC_000022.11 custom_accession\n"
    (native / "chr_synonyms.txt").write_bytes(synonyms)
    written = build()
    assert (output / "chr_synonyms.txt").read_bytes() == synonyms
    if existing:
        assert original
        assert written == []
        assert shard_state() == original
    else:
        assert written
