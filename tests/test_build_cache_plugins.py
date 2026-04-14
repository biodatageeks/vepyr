from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _load_vepyr_module(core_module: types.ModuleType, plugin_sources_module: types.ModuleType):
    module_path = Path(__file__).resolve().parents[1] / "src" / "vepyr" / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "vepyr", module_path, submodule_search_locations=[str(module_path.parent)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)

    previous = {
        name: sys.modules.get(name)
        for name in ("vepyr", "vepyr._core", "vepyr.plugin_sources")
    }
    sys.modules["vepyr"] = module
    sys.modules["vepyr._core"] = core_module
    sys.modules["vepyr.plugin_sources"] = plugin_sources_module
    try:
        spec.loader.exec_module(module)
    finally:
        for name, old in previous.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old
    return module


def _make_fake_core(call_log: dict[str, list[tuple]]):
    module = types.ModuleType("vepyr._core")
    module.convert_entity = lambda *args: [("entity.parquet", 1)]
    module.build_entity_fjall = lambda *args: []

    def build_plugin_fjall(plugin_name, parquet_dir, fjall_path, partitions=8, chromosomes=None):
        call_log.setdefault("build_plugin_fjall", []).append(
            (plugin_name, parquet_dir, fjall_path, partitions, chromosomes)
        )
        return (fjall_path, 1)

    module.build_plugin_fjall = build_plugin_fjall
    module.annotate_vcf = lambda *args, **kwargs: 0
    module.create_annotator = lambda *args, **kwargs: None

    def convert_plugin(
        plugin_name,
        source_path,
        output_dir,
        partitions=8,
        memory_limit_gb=32,
        chromosomes=None,
        assume_sorted_input=False,
        preview_rows=None,
    ):
        call_log.setdefault("convert_plugin", []).append(
            (
                plugin_name,
                source_path,
                output_dir,
                partitions,
                memory_limit_gb,
                chromosomes,
                assume_sorted_input,
                preview_rows,
            )
        )
        return [(str(Path(output_dir) / "chr1.parquet"), 1)]

    module.convert_plugin = convert_plugin

    def convert_cadd_plugin(
        snv_source_path,
        indel_source_path,
        output_dir,
        partitions=8,
        memory_limit_gb=32,
        chromosomes=None,
        assume_sorted_input=False,
        preview_rows=None,
    ):
        call_log.setdefault("convert_cadd_plugin", []).append(
            (
                snv_source_path,
                indel_source_path,
                output_dir,
                partitions,
                memory_limit_gb,
                chromosomes,
                assume_sorted_input,
                preview_rows,
            )
        )
        return [(str(Path(output_dir) / "chr1.parquet"), 2)]

    module.convert_cadd_plugin = convert_cadd_plugin
    return module


def _make_fake_plugin_sources(call_log: dict[str, list[tuple]]):
    module = types.ModuleType("vepyr.plugin_sources")

    def fetch_plugin_source(plugin_name, cache_dir, **kwargs):
        call_log.setdefault("fetch_plugin_source", []).append((plugin_name, cache_dir, kwargs))
        path = Path(cache_dir) / f"{plugin_name}.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture", encoding="utf-8")
        return str(path)

    module.fetch_plugin_source = fetch_plugin_source
    return module


def test_build_cache_accepts_local_plugin_mapping(tmp_path: Path):
    call_log: dict[str, list[tuple]] = {}
    core = _make_fake_core(call_log)
    plugin_sources = _make_fake_plugin_sources(call_log)
    module = _load_vepyr_module(core, plugin_sources)

    local_cache = tmp_path / "local-cache"
    local_cache.mkdir()
    snv = tmp_path / "whole_genome_SNVs.tsv.gz"
    indel = tmp_path / "gnomad.genomes.r4.0.indel.tsv.gz"
    spliceai = tmp_path / "spliceai.vcf.gz"
    for path in (snv, indel, spliceai):
        path.write_text("fixture", encoding="utf-8")

    module.build_cache(
        115,
        str(tmp_path / "cache"),
        local_cache=str(local_cache),
        plugins={"cadd": {"snv": str(snv), "indel": str(indel)}, "spliceai": str(spliceai)},
    )

    assert call_log["convert_plugin"] == [
        (
            "cadd",
            str(snv),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            8,
            32,
            None,
            False,
            None,
        ),
        (
            "spliceai",
            str(spliceai),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "spliceai"),
            8,
            32,
            None,
            False,
            None,
        ),
    ]
    assert "convert_cadd_plugin" not in call_log
    assert "fetch_plugin_source" not in call_log
    assert call_log["build_plugin_fjall"] == [
        (
            "cadd",
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd.fjall"),
            8,
            None,
        ),
        (
            "spliceai",
            str(tmp_path / "cache" / "115_GRCh38_vep" / "spliceai"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "spliceai.fjall"),
            8,
            None,
        ),
    ]


def test_build_cache_accepts_single_cadd_snv_path(tmp_path: Path):
    call_log: dict[str, list[tuple]] = {}
    core = _make_fake_core(call_log)
    plugin_sources = _make_fake_plugin_sources(call_log)
    module = _load_vepyr_module(core, plugin_sources)

    local_cache = tmp_path / "local-cache"
    local_cache.mkdir()
    snv = tmp_path / "whole_genome_SNVs.tsv.gz"
    indel = tmp_path / "gnomad.genomes.r4.0.indel.tsv.gz"
    snv.write_text("fixture", encoding="utf-8")
    indel.write_text("fixture", encoding="utf-8")

    module.build_cache(
        115,
        str(tmp_path / "cache"),
        local_cache=str(local_cache),
        plugins={"cadd": str(snv)},
    )

    assert call_log["convert_plugin"] == [
        (
            "cadd",
            str(snv),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            8,
            32,
            None,
            False,
            None,
        ),
    ]
    assert "convert_cadd_plugin" not in call_log
    assert call_log["build_plugin_fjall"] == [
        (
            "cadd",
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd.fjall"),
            8,
            None,
        )
    ]


def test_build_cache_auto_download_expands_logical_cadd(tmp_path: Path):
    call_log: dict[str, list[tuple]] = {}
    core = _make_fake_core(call_log)
    plugin_sources = _make_fake_plugin_sources(call_log)
    module = _load_vepyr_module(core, plugin_sources)

    local_cache = tmp_path / "local-cache"
    local_cache.mkdir()

    module.build_cache(
        115,
        str(tmp_path / "cache"),
        local_cache=str(local_cache),
        chromosomes=["Y"],
        plugins=["cadd", "clinvar"],
    )

    fetched = call_log["fetch_plugin_source"]
    assert [item[0] for item in fetched] == ["cadd_snv", "cadd_indel", "clinvar"]
    assert fetched[0][2]["chromosomes"] == ["Y"]
    assert call_log["convert_cadd_plugin"] == [
        (
            str(tmp_path / "cache" / "cadd_snv.gz"),
            str(tmp_path / "cache" / "cadd_indel.gz"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            8,
            32,
            ["Y"],
            False,
            None,
        )
    ]
    assert [item[0] for item in call_log["convert_plugin"]] == ["clinvar"]
    assert call_log["build_plugin_fjall"] == [
        (
            "cadd",
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "cadd.fjall"),
            8,
            ["Y"],
        ),
        (
            "clinvar",
            str(tmp_path / "cache" / "115_GRCh38_vep" / "clinvar"),
            str(tmp_path / "cache" / "115_GRCh38_vep" / "clinvar.fjall"),
            8,
            ["Y"],
        ),
    ]
