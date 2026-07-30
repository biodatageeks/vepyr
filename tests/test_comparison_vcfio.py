import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from comparison import vcfio

VCF_BODY = """##fileformat=VCFv4.2
##contig=<ID=chr1>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|missense_variant|ENST01
chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|ENST01
"""


@pytest.fixture
def plain_vcf(tmp_path):
    p = tmp_path / "sample.vcf"
    p.write_text(VCF_BODY)
    return p


@pytest.fixture
def bgzf_vcf(tmp_path, plain_vcf):
    out = tmp_path / "sample_bgzf.vcf.gz"
    with open(out, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain_vcf)], stdout=fh, check=True)
    return out


def test_is_bgzf_distinguishes_plain_from_block_gzip(plain_vcf, bgzf_vcf):
    assert vcfio.is_bgzf(str(bgzf_vcf)) is True
    assert vcfio.is_bgzf(str(plain_vcf)) is False


def test_open_text_reads_plain_and_bgzf_identically(plain_vcf, bgzf_vcf):
    with vcfio.open_text(str(plain_vcf)) as fh:
        plain = fh.read()
    with vcfio.open_text(str(bgzf_vcf)) as fh:
        compressed = fh.read()
    assert plain == compressed == VCF_BODY


def test_count_data_lines_ignores_headers(plain_vcf, bgzf_vcf):
    assert vcfio.count_data_lines(str(plain_vcf)) == 2
    assert vcfio.count_data_lines(str(bgzf_vcf)) == 2


def test_parse_vep_header_extracts_exact_code_and_cache_identity(tmp_path):
    path = tmp_path / "reference.vcf"
    path.write_text(
        "##fileformat=VCFv4.2\n"
        '##VEP="v116.0" API="v116" '
        'cache="/opt/vep/.vep/homo_sapiens_merged/116_GRCh38" '
        "ensembl=116.c0cf13d ensembl-variation=116.2fb834b "
        'assembly="GRCh38.p14"\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    )

    identity = vcfio.parse_vep_header(str(path))

    assert identity["vep_version"] == "116.0"
    assert identity["api_version"] == "116"
    assert identity["cache_version"] == "116"
    assert identity["ensembl_release"] == "116"
    assert identity["ensembl_revision"] == "c0cf13d"
    assert identity["ensembl_variation_release"] == "116"
    assert identity["ensembl_variation_revision"] == "2fb834b"
    assert identity["assembly"] == "GRCh38.p14"


def test_parse_vep_header_rejects_missing_identity(tmp_path):
    path = tmp_path / "reference.vcf"
    path.write_text(VCF_BODY)
    with pytest.raises(ValueError, match="No ##VEP"):
        vcfio.parse_vep_header(str(path))


def test_validate_vep_reference_identity_requires_exact_supported_target(tmp_path):
    path = tmp_path / "reference.vcf"
    path.write_text(
        "##fileformat=VCFv4.2\n"
        '##VEP="v116.0" API="v116" '
        'cache="/opt/vep/.vep/homo_sapiens_merged/116_GRCh38" '
        "ensembl=116.c0cf13d ensembl-variation=116.2fb834b "
        'assembly="GRCh38.p14"\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    )
    identity = vcfio.parse_vep_header(str(path))
    target = {
        "cache_version": "116",
        "vep_codebase_version": "116.0",
        "api_version": "116",
        "ensembl_core_revision": "c0cf13d",
        "ensembl_variation_revision": "2fb834b",
    }
    vcfio.validate_vep_reference_identity(identity, target)

    wrong = dict(target, ensembl_core_revision="266b84d")
    with pytest.raises(ValueError, match="ensembl_revision"):
        vcfio.validate_vep_reference_identity(identity, wrong)


def test_ensure_bgzf_compresses_a_plain_vcf(plain_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    result = vcfio.ensure_bgzf(str(plain_vcf), str(out_dir))
    assert result.endswith(".vcf.gz")
    assert vcfio.is_bgzf(result)
    assert vcfio.count_data_lines(result) == 2


def test_ensure_bgzf_regenerates_when_the_plain_source_changes(plain_vcf, tmp_path):
    out_dir = tmp_path / "work"
    first = vcfio.ensure_bgzf(str(plain_vcf), str(out_dir))
    first_source = json.loads((out_dir / "sample.vcf.gz.source.json").read_text())
    plain_vcf.write_text(
        VCF_BODY + "chr1\t300\t.\tC\tG\t50\tPASS\tCSQ=G|intron_variant|ENST01\n"
    )

    second = vcfio.ensure_bgzf(str(plain_vcf), str(out_dir))

    assert second == first
    assert vcfio.count_data_lines(second) == 3
    second_source = json.loads((out_dir / "sample.vcf.gz.source.json").read_text())
    assert second_source != first_source


def test_ensure_bgzf_regenerates_for_a_same_named_source_from_another_path(tmp_path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    first_source = first_dir / "sample.vcf"
    second_source = second_dir / "sample.vcf"
    first_source.write_text(VCF_BODY)
    second_source.write_text(
        VCF_BODY + "chr1\t300\t.\tC\tG\t50\tPASS\tCSQ=G|intron_variant|ENST01\n"
    )
    out_dir = tmp_path / "work"

    vcfio.ensure_bgzf(str(first_source), str(out_dir))
    result = vcfio.ensure_bgzf(str(second_source), str(out_dir))

    assert vcfio.count_data_lines(result) == 3
    marker = json.loads((out_dir / "sample.vcf.gz.source.json").read_text())
    assert marker["path"] == str(second_source)


def test_ensure_bgzf_returns_an_already_compressed_file_unchanged(bgzf_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    assert vcfio.ensure_bgzf(str(bgzf_vcf), str(out_dir)) == str(bgzf_vcf)


def test_ensure_bgzf_reindexes_an_edited_compressed_input(
    bgzf_vcf, plain_vcf, tmp_path
):
    out_dir = tmp_path / "work"
    result = vcfio.ensure_bgzf(str(bgzf_vcf), str(out_dir))
    marker = out_dir / "sample_bgzf.vcf.gz.tbi.source.json"
    first_source = json.loads(marker.read_text())

    plain_vcf.write_text(
        VCF_BODY + "chr1\t300\t.\tC\tG\t50\tPASS\tCSQ=G|intron_variant|ENST01\n"
    )
    with open(bgzf_vcf, "wb") as stream:
        subprocess.run(["bgzip", "-c", str(plain_vcf)], stdout=stream, check=True)

    second = vcfio.ensure_bgzf(str(bgzf_vcf), str(out_dir))

    assert second == result
    assert json.loads(marker.read_text()) != first_source
    queried = subprocess.run(
        ["tabix", second, "chr1:300-300"],
        capture_output=True,
        check=True,
        text=True,
    )
    assert queried.stdout.startswith("chr1\t300\t")


MULTI_CONTIG_BODY = """##fileformat=VCFv4.2
##contig=<ID=chr1>
##contig=<ID=chr2>
##contig=<ID=chr3>
##contig=<ID=chrUn_scaffold99>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tT\t50\tPASS\t.
chr2\t100\t.\tG\tC\t50\tPASS\t.
chr1\t300\t.\tC\tG\t50\tPASS\t.
"""


@pytest.fixture
def indexed_multi_contig(tmp_path):
    """Header lists 4 contigs; only chr1 and chr2 carry records."""
    plain = tmp_path / "multi.vcf"
    # tabix requires coordinate-sorted input
    rows = sorted(
        [ln for ln in MULTI_CONTIG_BODY.splitlines() if not ln.startswith("#")],
        key=lambda ln: (ln.split("\t")[0], int(ln.split("\t")[1])),
    )
    header = [ln for ln in MULTI_CONTIG_BODY.splitlines() if ln.startswith("#")]
    plain.write_text("\n".join(header + rows) + "\n")
    gz = tmp_path / "multi.vcf.gz"
    with open(gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain)], stdout=fh, check=True)
    subprocess.run(["tabix", "-p", "vcf", str(gz)], check=True)
    return gz


def test_detect_contigs_uses_the_index_not_the_header(indexed_multi_contig):
    """The header lists 4 contigs but only 2 have records; detection must find 2."""
    assert vcfio.detect_contigs(str(indexed_multi_contig)) == ["chr1", "chr2"]


def test_detect_contigs_returns_empty_for_an_unindexed_file(plain_vcf):
    assert vcfio.detect_contigs(str(plain_vcf)) == []


def test_slice_contig_extracts_only_the_requested_contig(
    indexed_multi_contig, tmp_path
):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    sliced = vcfio.slice_contig(str(indexed_multi_contig), "chr1", str(out_dir))
    assert vcfio.is_bgzf(sliced)
    assert vcfio.count_data_lines(sliced) == 2
    with vcfio.open_text(sliced) as fh:
        chroms = {ln.split("\t")[0] for ln in fh if not ln.startswith("#")}
    assert chroms == {"chr1"}


def test_slice_contig_regenerates_when_the_indexed_source_changes(
    indexed_multi_contig, tmp_path
):
    out_dir = tmp_path / "work"
    first = vcfio.slice_contig(str(indexed_multi_contig), "chr1", str(out_dir))
    marker = out_dir / "input_chr1.source.json"
    first_source = json.loads(marker.read_text())
    stat = indexed_multi_contig.stat()
    os.utime(indexed_multi_contig, (stat.st_atime, stat.st_mtime + 1))

    second = vcfio.slice_contig(str(indexed_multi_contig), "chr1", str(out_dir))

    assert second == first
    second_source = json.loads(marker.read_text())
    assert second_source["mtime"] != first_source["mtime"]
    assert vcfio.count_data_lines(second) == 2


def test_slice_vep_reads_a_bgzf_reference(indexed_multi_contig, tmp_path):
    """Regression: extract_chrom_from_vep used bare open() and raised UnicodeDecodeError."""
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    out = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(out_dir), "merged")
    assert vcfio.count_data_lines(out) == 2


def test_slice_vep_regenerates_when_the_reference_source_changes(
    indexed_multi_contig, tmp_path
):
    out_dir = tmp_path / "work"
    first = vcfio.slice_vep(
        str(indexed_multi_contig),
        "chr1",
        str(out_dir),
        "merged",
    )
    marker = out_dir / "vep_chr1_merged.source.json"
    first_source = json.loads(marker.read_text())
    stat = indexed_multi_contig.stat()
    os.utime(indexed_multi_contig, (stat.st_atime, stat.st_mtime + 1))

    second = vcfio.slice_vep(
        str(indexed_multi_contig),
        "chr1",
        str(out_dir),
        "merged",
    )

    assert second == first
    second_source = json.loads(marker.read_text())
    assert second_source["mtime"] != first_source["mtime"]
    assert vcfio.count_data_lines(second) == 2


def test_slice_vep_tabix_and_linear_paths_agree(indexed_multi_contig, tmp_path):
    """The indexed fast path and the plain linear scan must produce identical records."""
    gz_dir = tmp_path / "gz"
    gz_dir.mkdir()
    via_tabix = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(gz_dir), "a")

    plain = tmp_path / "plain.vcf"
    with vcfio.open_text(str(indexed_multi_contig)) as fh:
        plain.write_text(fh.read())
    plain_dir = tmp_path / "plain_out"
    plain_dir.mkdir()
    via_scan = vcfio.slice_vep(str(plain), "chr1", str(plain_dir), "a")

    def records(path):
        with vcfio.open_text(path) as fh:
            return [ln for ln in fh if not ln.startswith("#")]

    assert records(via_tabix) == records(via_scan)


def test_slice_vep_matches_contig_without_chr_prefix(tmp_path):
    """VEP output may use bare contig names; chr22 must still match a '22' record."""
    src = tmp_path / "bare.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "22\t100\t.\tA\tT\t50\tPASS\t.\n"
    )
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    out = vcfio.slice_vep(str(src), "chr22", str(out_dir), "merged")
    assert vcfio.count_data_lines(out) == 1


def test_slice_vep_counts_an_indexed_final_record_without_a_newline(
    tmp_path, monkeypatch, capsys
):
    reference = tmp_path / "reference.vcf.gz"
    reference.write_bytes(b"")
    (tmp_path / "reference.vcf.gz.tbi").write_bytes(b"")

    def fake_run(command, **_kwargs):
        if command[1] == "-H":
            return SimpleNamespace(
                returncode=0,
                stdout=b"##fileformat=VCFv4.2\n"
                b"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n",
            )
        return SimpleNamespace(
            returncode=0,
            stdout=b"22\t100\t.\tA\tT\t50\tPASS\t.",
        )

    monkeypatch.setattr(vcfio.subprocess, "run", fake_run)
    out_dir = tmp_path / "work"

    vcfio.slice_vep(str(reference), "chr22", str(out_dir), "merged")

    assert "Extracted 1 VEP records" in capsys.readouterr().out


def test_normalize_vcf_records_its_source(indexed_multi_contig, tmp_path):
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    norm = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    sidecar = json.loads((tmp_path / "shared" / "normalized.source.json").read_text())
    assert sidecar["path"] == str(indexed_multi_contig)
    assert sidecar["size"] == indexed_multi_contig.stat().st_size
    assert vcfio.is_bgzf(norm)


def test_normalize_vcf_reuses_output_for_the_same_source(
    indexed_multi_contig, tmp_path
):
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    first = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    marker = os.path.join(str(out_dir), "marker")
    open(marker, "w").close()
    second = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    assert first == second
    assert os.path.exists(marker), "reuse must not wipe the shared directory"


def test_normalize_vcf_reruns_when_the_source_changes(indexed_multi_contig, tmp_path):
    """A different --vcf at the same release must not silently reuse a stale decomposition."""
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))

    other = tmp_path / "other.vcf.gz"
    other.write_bytes(indexed_multi_contig.read_bytes())
    subprocess.run(["tabix", "-f", "-p", "vcf", str(other)], check=True)
    vcfio.normalize_vcf(str(other), str(out_dir))

    sidecar = json.loads((out_dir / "normalized.source.json").read_text())
    assert sidecar["path"] == str(other)
