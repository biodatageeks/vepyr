import subprocess

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


def test_ensure_bgzf_compresses_a_plain_vcf(plain_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    result = vcfio.ensure_bgzf(str(plain_vcf), str(out_dir))
    assert result.endswith(".vcf.gz")
    assert vcfio.is_bgzf(result)
    assert vcfio.count_data_lines(result) == 2


def test_ensure_bgzf_returns_an_already_compressed_file_unchanged(bgzf_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    assert vcfio.ensure_bgzf(str(bgzf_vcf), str(out_dir)) == str(bgzf_vcf)
