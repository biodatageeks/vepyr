from __future__ import annotations

import gzip
import importlib.util
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


def _load_plugin_sources_module():
    module_path = Path(__file__).resolve().parents[1] / "src" / "vepyr" / "plugin_sources.py"
    module_name = "vepyr_plugin_sources_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _write_gzip_text(path: Path, text: str) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write(text)


def _write_plain_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def plugin_sources():
    return _load_plugin_sources_module()


def test_fetch_alphamissense_writes_source_metadata(tmp_path: Path, monkeypatch, plugin_sources):
    download_calls: list[str] = []

    def fake_download(url: str, dest: Path) -> None:
        download_calls.append(url)
        _write_gzip_text(
            dest,
            "#CHROM\tPOS\tREF\tALT\tgenome\tuniprot_id\ttranscript_id\tprotein_variant\tam_pathogenicity\tam_class\n"
            "1\t100\tA\tG\tGRCh38\tP12345\tENST1\tp.A1G\t0.42\tlikely_pathogenic\n",
        )

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    source_path = Path(plugin_sources.fetch_plugin_source("alphamissense", str(tmp_path)))

    assert source_path.name == "AlphaMissense_hg38.tsv.gz"
    assert len(download_calls) == 1

    metadata = json.loads(source_path.with_name("source.json").read_text())
    assert metadata["plugin_name"] == "alphamissense"
    assert metadata["assembly"] == "GRCh38"
    assert metadata["filename"] == source_path.name
    assert metadata["source_url"].endswith("/AlphaMissense_hg38.tsv.gz")
    assert metadata["chromosomes"] is None
    assert len(metadata["sha256"]) == 64


def test_fetch_plugin_source_is_idempotent_without_force(
    tmp_path: Path, monkeypatch, plugin_sources
):
    download_calls = 0

    def fake_download(url: str, dest: Path) -> None:
        nonlocal download_calls
        download_calls += 1
        _write_gzip_text(
            dest,
            "#Chrom\tPos\tRef\tAlt\tRawScore\tPHRED\n1\t101\tC\tT\t1.5\t12.0\n",
        )

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    first = plugin_sources.fetch_plugin_source("cadd", str(tmp_path))
    second = plugin_sources.fetch_plugin_source("cadd", str(tmp_path))

    assert first == second
    assert download_calls == 1


def test_fetch_plugin_source_force_redownloads(tmp_path: Path, monkeypatch, plugin_sources):
    download_calls = 0

    def fake_download(url: str, dest: Path) -> None:
        nonlocal download_calls
        download_calls += 1
        _write_gzip_text(
            dest,
            "##fileformat=VCFv4.2\n"
            '##INFO=<ID=SpliceAI,Number=.,Type=String,Description="scores">\n'
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            "1\t123\t.\tA\tG\t.\t.\tSpliceAI=G|GENE|0.1|0.2|0.3|0.4|1|2|3|4\n",
        )

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    plugin_sources.fetch_plugin_source("spliceai", str(tmp_path))
    plugin_sources.fetch_plugin_source("spliceai", str(tmp_path), force=True)

    assert download_calls == 2


def test_fetch_dbnsfp_prepares_merged_grch38_gzip(tmp_path: Path, monkeypatch, plugin_sources):
    def fake_download(url: str, dest: Path) -> None:
        with zipfile.ZipFile(dest, "w") as archive:
            for chrom, pos in (("1", "100"), ("2", "200")):
                inner_name = f"dbNSFP4.9c_variant.chr{chrom}.gz"
                inner_path = tmp_path / inner_name
                _write_gzip_text(
                    inner_path,
                    "#chr\tpos(1-based)\tref\talt\tSIFT4G_score\tSIFT4G_pred\t"
                    "Polyphen2_HDIV_score\tPolyphen2_HVAR_score\tLRT_score\tLRT_pred\t"
                    "MutationTaster_score\tREVEL_score\tMetaSVM_score\tMetaSVM_pred\t"
                    "MetaLR_score\tMetaLR_pred\tGERP++_RS\tphyloP100way_vertebrate\t"
                    "phyloP30way_mammalian\tphastCons100way_vertebrate\t"
                    "phastCons30way_mammalian\tSiPhy_29way_logOdds\tCADD_raw\tCADD_phred\t"
                    "FATHMM_score\tFATHMM_pred\tPROVEAN_score\tPROVEAN_pred\tVEST4_score\t"
                    "BayesDel_addAF_score\tBayesDel_noAF_score\tMutationTaster_pred\n"
                    f"{chrom}\t{pos}\tA\tG\t0.1\tT\t0.2\t0.3\t0.4\tD\t0.5\t0.6\t0.7\tD\t0.8\tD\t1.0\t1.1\t1.2\t1.3\t1.4\t1.5\t1.6\t10.0\tD\tD\tN\tD\t0.9\t0.11\t0.12\tD\n",
                )
                archive.write(inner_path, arcname=inner_name)

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    source_path = Path(plugin_sources.fetch_plugin_source("dbnsfp", str(tmp_path)))

    assert source_path.name == "dbNSFP4.9c_grch38.gz"
    assert source_path.exists()
    assert not source_path.with_name("dbNSFP4.9c.zip").exists()
    with gzip.open(source_path, "rt", encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle]
    assert lines[0].startswith("#chr\tpos(1-based)\tref\talt")
    assert len(lines) == 3


def test_fetch_alphamissense_filters_requested_chromosomes(
    tmp_path: Path, monkeypatch, plugin_sources
):
    def fake_download(url: str, dest: Path) -> None:
        _write_gzip_text(
            dest,
            "#CHROM\tPOS\tREF\tALT\tgenome\tuniprot_id\ttranscript_id\tprotein_variant\tam_pathogenicity\tam_class\n"
            "1\t100\tA\tG\tGRCh38\tP12345\tENST1\tp.A1G\t0.42\tlikely_pathogenic\n"
            "2\t200\tC\tT\tGRCh38\tP67890\tENST2\tp.C2T\t0.10\tbenign\n",
        )

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    source_path = Path(
        plugin_sources.fetch_plugin_source("alphamissense", str(tmp_path), chromosomes=["1"])
    )

    assert "chromosomes-1" in str(source_path)
    with gzip.open(source_path, "rt", encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle]
    assert lines == [
        "#CHROM\tPOS\tREF\tALT\tgenome\tuniprot_id\ttranscript_id\tprotein_variant\tam_pathogenicity\tam_class",
        "1\t100\tA\tG\tGRCh38\tP12345\tENST1\tp.A1G\t0.42\tlikely_pathogenic",
    ]


def test_fetch_dbnsfp_filters_requested_members(tmp_path: Path, monkeypatch, plugin_sources):
    def fake_download(url: str, dest: Path) -> None:
        with zipfile.ZipFile(dest, "w") as archive:
            for chrom, pos in (("1", "100"), ("2", "200")):
                inner_name = f"dbNSFP4.9c_variant.chr{chrom}.gz"
                inner_path = tmp_path / inner_name
                _write_gzip_text(
                    inner_path,
                    "#chr\tpos(1-based)\tref\talt\tSIFT4G_score\tSIFT4G_pred\t"
                    "Polyphen2_HDIV_score\tPolyphen2_HVAR_score\tLRT_score\tLRT_pred\t"
                    "MutationTaster_score\tREVEL_score\tMetaSVM_score\tMetaSVM_pred\t"
                    "MetaLR_score\tMetaLR_pred\tGERP++_RS\tphyloP100way_vertebrate\t"
                    "phyloP30way_mammalian\tphastCons100way_vertebrate\t"
                    "phastCons30way_mammalian\tSiPhy_29way_logOdds\tCADD_raw\tCADD_phred\t"
                    "FATHMM_score\tFATHMM_pred\tPROVEAN_score\tPROVEAN_pred\tVEST4_score\t"
                    "BayesDel_addAF_score\tBayesDel_noAF_score\tMutationTaster_pred\n"
                    f"{chrom}\t{pos}\tA\tG\t0.1\tT\t0.2\t0.3\t0.4\tD\t0.5\t0.6\t0.7\tD\t0.8\tD\t1.0\t1.1\t1.2\t1.3\t1.4\t1.5\t1.6\t10.0\tD\tD\tN\tD\t0.9\t0.11\t0.12\tD\n",
                )
                archive.write(inner_path, arcname=inner_name)

    monkeypatch.setattr(plugin_sources, "_download_with_progress", fake_download)

    source_path = Path(
        plugin_sources.fetch_plugin_source("dbnsfp", str(tmp_path), chromosomes=["2"])
    )
    with gzip.open(source_path, "rt", encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle]
    assert len(lines) == 2
    assert lines[1].startswith("2\t200\t")


def test_fetch_clinvar_uses_tabix_region_slice(tmp_path: Path, monkeypatch, plugin_sources):
    if shutil.which("tabix") is None or shutil.which("bgzip") is None:
        pytest.skip("tabix/bgzip not available")

    source_vcf = tmp_path / "clinvar.vcf"
    _write_plain_text(
        source_vcf,
        "##fileformat=VCFv4.2\n"
        '##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Clinical significance">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "1\t100\t.\tA\tG\t.\t.\tCLNSIG=Pathogenic\n"
        "2\t200\t.\tC\tT\t.\t.\tCLNSIG=Benign\n",
    )
    source_bgz = tmp_path / "clinvar.vcf.gz"
    with open(source_bgz, "wb") as handle:
        subprocess.run(["bgzip", "-c", str(source_vcf)], check=True, stdout=handle)
    subprocess.run(["tabix", "-f", "-p", "vcf", str(source_bgz)], check=True)

    spec = plugin_sources.PLUGIN_SOURCE_SPECS["clinvar"]
    patched_spec = plugin_sources.PluginSourceSpec(
        name=spec.name,
        source_kind=spec.source_kind,
        fetch_mode=spec.fetch_mode,
        supported_assemblies=spec.supported_assemblies,
        default_version=spec.default_version,
        download_filename=spec.download_filename,
        output_filename=spec.output_filename,
        download_url=lambda assembly, version: source_bgz.as_uri(),
        validate=spec.validate,
        filter_downloaded=spec.filter_downloaded,
        prepare_download=spec.prepare_download,
        release_to_version=spec.release_to_version,
    )
    monkeypatch.setitem(plugin_sources.PLUGIN_SOURCE_SPECS, "clinvar", patched_spec)

    source_path = Path(
        plugin_sources.fetch_plugin_source("clinvar", str(tmp_path), chromosomes=["1"])
    )
    with gzip.open(source_path, "rt", encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle if not line.startswith("##")]
    assert lines == [
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
        "1\t100\t.\tA\tG\t.\t.\tCLNSIG=Pathogenic",
    ]


def test_fetch_plugin_source_rejects_unsupported_assembly(tmp_path: Path, plugin_sources):
    with pytest.raises(ValueError, match="does not support assembly"):
        plugin_sources.fetch_plugin_source("dbnsfp", str(tmp_path), assembly="GRCh37")
