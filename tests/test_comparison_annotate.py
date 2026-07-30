import sys
import types

import pytest

from comparison import annotate


@pytest.fixture
def fake_vepyr(monkeypatch):
    """Install a stub vepyr module that writes a two-record VCF."""
    calls = []

    def fake_annotate(vcf, cache_dir, **kwargs):
        calls.append({"vcf": vcf, "cache_dir": cache_dir, **kwargs})
        out = kwargs["output_vcf"]
        with open(out, "w") as f:
            f.write("##fileformat=VCFv4.2\n")
            f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
            f.write("chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|x|y\n")
            f.write("chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|x|y\n")

    module = types.ModuleType("vepyr")
    module.annotate = fake_annotate
    module.supported_vep_targets = lambda: (
        {"cache_version": "115", "vep_codebase_version": "115.2"},
    )
    module.cache_contig_identity = lambda cache_dir, chrom, **kwargs: {
        "cache_version": kwargs["expected_cache_version"],
        "cache_source_type": "merged",
        "contig": chrom,
    }
    monkeypatch.setitem(sys.modules, "vepyr", module)
    return calls


def test_native_contract_adapters_defer_import_and_forward(fake_vepyr):
    assert annotate.supported_vep_targets()[0]["cache_version"] == "115"
    identity = annotate.cache_contig_identity("/cache", "chr1", "115")
    assert identity == {
        "cache_version": "115",
        "cache_source_type": "merged",
        "contig": "chr1",
    }


def _existing_output(path, n=200):
    path.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|x|y\n"
        * n
    )


def test_annotate_contig_forwards_profile_kwargs(fake_vepyr, tmp_path):
    out = tmp_path / "out.vcf"
    elapsed, n = annotate.annotate_contig(
        "input.vcf.gz",
        "/cache",
        "/ref.fa",
        str(out),
        workers=4,
        annotate_kwargs={"per_gene": True, "pick_order": "rank"},
    )
    assert n == 2
    assert elapsed is not None
    call = fake_vepyr[0]
    assert call["workers"] == 4
    assert call["per_gene"] is True
    assert call["pick_order"] == "rank"
    assert call["everything"] is True
    assert call["cache_format"] == "parquet"


def test_annotate_contig_never_relabels_an_unverified_existing_output(
    fake_vepyr, tmp_path
):
    out = tmp_path / "out.vcf"
    _existing_output(out)
    elapsed, n = annotate.annotate_contig(
        "input.vcf.gz", "/cache", "/ref.fa", str(out), workers=1, annotate_kwargs={}
    )
    assert elapsed is not None
    assert n == 2
    assert len(fake_vepyr) == 1


def test_annotate_contig_force_reannotates(fake_vepyr, tmp_path):
    out = tmp_path / "out.vcf"
    _existing_output(out)
    annotate.annotate_contig(
        "input.vcf.gz",
        "/cache",
        "/ref.fa",
        str(out),
        workers=1,
        annotate_kwargs={},
        force=True,
    )
    assert len(fake_vepyr) == 1


def test_annotate_contig_rejects_non_bgzf_output_when_bgzf_requested(
    fake_vepyr, tmp_path
):
    """The stub writes plain text, so --bgzf validation must fail loudly."""
    out = tmp_path / "out.vcf.gz"
    with pytest.raises(SystemExit, match="not valid BGZF"):
        annotate.annotate_contig(
            "input.vcf.gz",
            "/cache",
            "/ref.fa",
            str(out),
            workers=1,
            annotate_kwargs={},
            bgzf=True,
        )
