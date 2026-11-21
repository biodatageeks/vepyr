"""Cache parity on small synthetic caches: what passes and what each failure says."""

import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "e2e-testing" / "scripts"))
import cache_parity  # noqa: E402

PLUGIN_ROWS = {
    "chrom": ["1", "1", "1", "1"],
    "start": pa.array([10, 20, 30, 40], pa.uint32()),
    "end": pa.array([10, 20, 30, 40], pa.uint32()),
    "allele_string": ["A/G", "C/T", "G/-", "T/A"],
    "clnsig": ["Benign", None, "Pathogenic", "Uncertain_significance"],
    "tier": pa.array([0, 0, 1, 1], pa.int8()),
}


def plugin_table(rows: dict | None = None) -> pa.Table:
    return pa.table(rows or PLUGIN_ROWS)


def write_plugin(
    root: Path, shards: dict[str, pa.Table] | None = None, **manifest
) -> Path:
    """A plugin cache (`<root>/plugin/demo`) whose manifest matches its shards."""
    shards = shards or {"chr1": plugin_table(), "chr2": plugin_table()}
    cache = root / "plugin" / "demo"
    cache.mkdir(parents=True)
    chroms = []
    for name, table in shards.items():
        pq.write_table(table, cache / f"{name}.parquet")
        tiers = table.column("tier").to_pylist() if "tier" in table.column_names else []
        chroms.append(
            {
                "chrom": name,
                "file": f"{name}.parquet",
                "rows": table.num_rows,
                "warm": tiers.count(0),
                "cold": tiers.count(1),
            }
        )
    body = {
        "plugin_name": "demo",
        "key_columns": ["chrom", "start", "end", "allele_string"],
        "match_columns": [],
        "value_columns": [{"column": "clnsig", "csq_field": "CLNSIG", "type": "Utf8"}],
        "allele_match": "minimised",
        "field_order": "declared",
        "assume_unique": False,
        "chroms": chroms,
        "sources": [{"file": "demo.vcf.gz", "md5": "aa", "verified_md5": "aa"}],
        "cache_source_version": "v1@" + "c" * 40,
    }
    body.update(manifest)
    (cache / "manifest.json").write_text(json.dumps(body))
    return cache


def changed(rows: dict | None = None, **columns) -> pa.Table:
    return pa.table({**(rows or PLUGIN_ROWS), **columns})


def without_row(index: int) -> pa.Table:
    table = plugin_table()
    keep = [i for i in range(table.num_rows) if i != index]
    return table.take(keep)


def problems(report) -> str:
    return "\n".join(
        [*report.problems, *(p for s in report.shards for p in s.problems)]
    )


@pytest.fixture
def published(tmp_path):
    return write_plugin(tmp_path / "published")


def test_identical_plugin_caches_pass(tmp_path, published):
    built = write_plugin(tmp_path / "built")
    report = cache_parity.compare_plugin_cache(built, published)
    assert report.ok, problems(report)
    assert report.verdict() == (
        "PASS: 2 of 2 shards identical in rows, schema, tiers and every column (8 rows)"
    )
    assert [s.tiers_built for s in report.shards] == [{"warm": 2, "cold": 2}] * 2
    assert "chr1" in report.text() and "identical" in report.text()


def test_dropped_row_fails_and_names_the_row(tmp_path, published):
    built = write_plugin(
        tmp_path / "built", {"chr1": without_row(1), "chr2": plugin_table()}
    )
    report = cache_parity.compare_plugin_cache(built, published)
    assert not report.ok
    text = problems(report)
    assert "rows: 3 in the rebuilt cache, 4 in the published cache (-1)" in text
    assert "1 row(s) only in the published cache, e.g. chrom='1', start=20" in text
    assert "manifest.json: chr1 is file chr1.parquet, rows 3, warm 1, cold 2" in text
    assert report.verdict().startswith("FAIL: 1 of 2 shards differ (chr1); chr1: rows:")
    assert [s.ok for s in report.shards] == [False, True]


def test_changed_value_fails_and_names_column_and_row(tmp_path, published):
    clnsig = ["Benign", None, "Likely_pathogenic", "Uncertain_significance"]
    built = write_plugin(
        tmp_path / "built", {"chr1": changed(clnsig=clnsig), "chr2": plugin_table()}
    )
    report = cache_parity.compare_plugin_cache(built, published)
    text = problems(report)
    assert not report.ok
    assert "content: values differ in clnsig (1 rows)" in text
    assert (
        "row 2 (chrom='1', start=30, end=30, allele_string='G/-'): clnsig is "
        "'Likely_pathogenic' in the rebuilt cache, 'Pathogenic' in the published cache"
    ) in text


def test_null_instead_of_value_fails(tmp_path, published):
    clnsig = [None, None, "Pathogenic", "Uncertain_significance"]
    built = write_plugin(tmp_path / "built", {"chr1": changed(clnsig=clnsig)})
    shard = cache_parity.compare_plugin_cache(built, published).shards[0]
    assert "clnsig is None in the rebuilt cache, 'Benign'" in "\n".join(shard.problems)


def test_tier_change_fails_with_tier_counts(tmp_path, published):
    tier = pa.array([0, 1, 1, 1], pa.int8())
    built = write_plugin(
        tmp_path / "built", {"chr1": changed(tier=tier), "chr2": plugin_table()}
    )
    report = cache_parity.compare_plugin_cache(built, published)
    text = problems(report)
    assert not report.ok
    assert (
        "tier counts: warm 1, cold 3 in the rebuilt cache, warm 2, cold 2 in the "
        "published cache"
    ) in text
    assert "manifest.json: chr1 is file chr1.parquet, rows 4, warm 1, cold 3" in text
    assert "content: values differ in tier (1 rows)" in text


def test_tier_counts_in_manifest_alone_fail(tmp_path, published):
    built = write_plugin(tmp_path / "built")
    manifest = json.loads((built / "manifest.json").read_text())
    manifest["chroms"][0].update(warm=1, cold=3)
    (built / "manifest.json").write_text(json.dumps(manifest))
    report = cache_parity.compare_plugin_cache(built, published)
    text = problems(report)
    assert not report.ok
    assert "manifest.json: chr1 is file chr1.parquet, rows 4, warm 1, cold 3" in text
    assert "manifest.json of the rebuilt cache lists" in text
    assert "but the shard holds {'rows': 4, 'warm': 2, 'cold': 2}" in text


@pytest.mark.parametrize(
    "table, message",
    [
        (
            changed(start=pa.array([10, 20, 30, 40], pa.int64())),
            "schema: column start is int64 in the rebuilt cache, uint32 in the "
            "published cache",
        ),
        (
            plugin_table().drop_columns(["clnsig"]),
            "schema: column(s) missing from the rebuilt cache: clnsig",
        ),
        (
            plugin_table().append_column("extra", pa.array([1, 2, 3, 4])),
            "schema: column(s) not in the published cache: extra",
        ),
        (
            plugin_table().select(
                ["chrom", "end", "start", "allele_string", "clnsig", "tier"]
            ),
            "schema: column order is chrom, end, start",
        ),
        (
            plugin_table().cast(
                pa.schema(
                    [
                        pa.field(f.name, f.type, nullable=f.name != "chrom")
                        for f in plugin_table().schema
                    ]
                )
            ),
            "schema: column chrom is nullable=False in the rebuilt cache, "
            "nullable=True in the published cache",
        ),
        (
            plugin_table().replace_schema_metadata({"bio.vep.cache_version": "117"}),
            "schema: metadata bio.vep.cache_version is '117' in the rebuilt cache, "
            "None in the published cache",
        ),
    ],
    ids=["type", "missing", "extra", "order", "nullable", "metadata"],
)
def test_schema_change_fails(tmp_path, published, table, message):
    built = write_plugin(tmp_path / "built", {"chr1": table, "chr2": plugin_table()})
    report = cache_parity.compare_plugin_cache(built, published)
    assert not report.ok
    assert message in problems(report)
    assert report.shards[1].ok


def test_reordered_rows_pass_with_a_note_unless_strict(tmp_path, published):
    built = write_plugin(
        tmp_path / "built",
        {"chr1": plugin_table().take([1, 0, 2, 3]), "chr2": plugin_table()},
    )
    report = cache_parity.compare_plugin_cache(built, published)
    assert report.ok
    assert report.shards[0].notes == ["same rows in a different order"]
    assert "1 with notes" in report.verdict()
    strict = cache_parity.compare_plugin_cache(built, published, strict_order=True)
    assert "same rows in a different order" in problems(strict)


def test_reorder_in_a_shard_too_large_to_sort_is_a_mismatch(tmp_path, published):
    built = write_plugin(
        tmp_path / "built", {"chr1": plugin_table().take([1, 0, 2, 3])}
    )
    shard = cache_parity.compare_plugin_cache(
        built, published, sort_limit_rows=3
    ).shards[0]
    assert not shard.ok
    assert "content: values differ in" in shard.problems[0]
    assert "more than 3 rows" in shard.notes[0]


def test_floats_compare_by_bits(tmp_path):
    def shard(values, name):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"score": pa.array(values, pa.float64())}), path)
        return path

    nan = float("nan")
    same = cache_parity.compare_parquet(
        shard([nan, 0.5, None], "a"),
        shard([nan, 0.5, None], "b"),
        entity="e",
        shard="s",
    )
    assert same.ok
    zero = cache_parity.compare_parquet(
        shard([0.0], "c"), shard([-0.0], "d"), entity="e", shard="s"
    )
    assert "score is 0.0 in the rebuilt cache, -0.0 in the published cache" in (
        "\n".join(zero.problems)
    )


def test_nested_column_change_is_found(tmp_path):
    features = pa.list_(pa.struct([("analysis", pa.string()), ("start", pa.int64())]))

    def shard(start, name):
        table = pa.table(
            {
                "transcript_id": ["T1", "T2"],
                "protein_features": pa.array(
                    [[{"analysis": "Pfam", "start": start}], []], features
                ),
            }
        )
        pq.write_table(table, tmp_path / f"{name}.parquet")
        return tmp_path / f"{name}.parquet"

    result = cache_parity.compare_parquet(
        shard(5, "a"), shard(7, "b"), entity="translation_core", shard="chr1"
    )
    assert "content: values differ in protein_features (1 rows)" in result.problems
    assert cache_parity.compare_parquet(
        shard(5, "c"), shard(5, "d"), entity="translation_core", shard="chr1"
    ).ok


def test_batches_align_across_different_row_groups(tmp_path):
    table = pa.table({"start": list(range(1000)), "v": [str(i) for i in range(1000)]})
    pq.write_table(table, tmp_path / "a.parquet", row_group_size=70)
    pq.write_table(table, tmp_path / "b.parquet", row_group_size=333)
    result = cache_parity.compare_parquet(
        tmp_path / "a.parquet",
        tmp_path / "b.parquet",
        entity="e",
        shard="s",
        batch_rows=64,
    )
    assert result.ok and result.rows_built == 1000
    other = table.set_column(
        1, "v", pa.array(["x" if i == 777 else str(i) for i in range(1000)])
    )
    pq.write_table(other, tmp_path / "c.parquet", row_group_size=70)
    result = cache_parity.compare_parquet(
        tmp_path / "c.parquet",
        tmp_path / "b.parquet",
        entity="e",
        shard="s",
        batch_rows=64,
        sort_limit_rows=0,
    )
    assert "row 777 (start=777, v='777'): v is 'x' in the rebuilt cache" in (
        "\n".join(result.problems)
    )


def test_unreadable_shard_is_a_problem(tmp_path, published):
    built = write_plugin(tmp_path / "built")
    (built / "chr1.parquet").write_bytes(b"not parquet")
    shard = cache_parity.compare_plugin_cache(built, published).shards[0]
    assert "shard of the rebuilt cache is unreadable" in shard.problems[0]


def test_missing_and_extra_shards_fail(tmp_path, published):
    built = write_plugin(
        tmp_path / "built", {"chr1": plugin_table(), "chr3": plugin_table()}
    )
    report = cache_parity.compare_plugin_cache(built, published)
    by_name = {s.shard: s.problems for s in report.shards}
    assert by_name["chr2"] == ["shard missing from the rebuilt cache"]
    assert by_name["chr3"][0] == "shard is not in the published cache"
    assert "manifest.json: chr2 is not listed in the rebuilt cache" in report.problems


def test_plugin_manifest_field_change_fails_and_lookup_defaults(tmp_path, published):
    built = write_plugin(tmp_path / "built", allele_match="exact", lookup="point")
    report = cache_parity.compare_plugin_cache(built, published)
    assert report.problems == [
        "manifest.json: allele_match is 'exact' in the rebuilt cache, 'minimised' "
        "in the published cache"
    ]
    assert report.verdict().startswith("FAIL: manifest.json: allele_match")


# ---- core caches ---------------------------------------------------------

CORE_METADATA = {"bio.vep.cache_version": "116", "bio.vep.cache_source_type": "merged"}


def core_table(entity: str) -> pa.Table:
    table = pa.table(
        {
            "chrom": ["1", "1", "1"],
            "start": pa.array([5, 6, 7], pa.int64()),
            "name": [f"{entity}-a", f"{entity}-b", f"{entity}-c"],
        }
    )
    return table.replace_schema_metadata(CORE_METADATA)


def write_core(
    root: Path, tables: dict[tuple[str, str], pa.Table] | None = None
) -> Path:
    cache = root / "116_GRCh38_merged"
    entities = ("variation", "transcript")
    tables = tables or {}
    for entity in entities:
        (cache / entity).mkdir(parents=True)
        manifest = []
        for chrom in ("chr1", "chr2"):
            table = tables.get((entity, chrom), core_table(entity))
            pq.write_table(table, cache / entity / f"{chrom}.parquet")
            manifest.append(
                {"chrom": chrom, "dataset": f"{chrom}.parquet", "rows": table.num_rows}
            )
        (cache / entity / "chrom_manifest.json").write_text(json.dumps(manifest))
    (cache / "reference_policy.json").write_text(
        json.dumps({"schema_version": 1, "cache_version": "116"})
    )
    (cache / "chr_synonyms.txt").write_text("1\tchr1\n")
    return cache


def test_identical_core_caches_pass(tmp_path):
    built, published = write_core(tmp_path / "b"), write_core(tmp_path / "p")
    (published / ".revision").write_text("{}")  # staging marker, not cache content
    report = cache_parity.compare_core_cache(built, published)
    assert report.ok, problems(report)
    assert report.entities() == ["transcript", "variation"]
    assert report.verdict() == (
        "PASS: 4 of 4 shards identical in rows, schema and every column (12 rows)"
    )
    assert "variation" in report.text()


def test_core_dropped_row_and_changed_value_fail(tmp_path):
    published = write_core(tmp_path / "p")
    shorter = core_table("variation").slice(0, 2)
    renamed = core_table("transcript").set_column(2, "name", pa.array(["x", "y", "z"]))
    built = write_core(
        tmp_path / "b",
        {("variation", "chr2"): shorter, ("transcript", "chr1"): renamed},
    )
    report = cache_parity.compare_core_cache(built, published)
    by_name = {f"{s.entity}/{s.shard}": s.problems for s in report.shards}
    assert by_name["variation/chr2"][0] == (
        "rows: 2 in the rebuilt cache, 3 in the published cache (-1)"
    )
    assert by_name["transcript/chr1"][0] == "content: values differ in name (3 rows)"
    assert by_name["variation/chr1"] == []
    assert any(
        p.startswith("variation/chrom_manifest.json differs for 1 contig(s), e.g. chr2")
        for p in report.problems
    )
    assert "transcript/chr1, variation/chr2" in report.verdict()
    assert "2 shard(s) DIFFER" not in report.text()  # one per entity
    assert "1 shard(s) DIFFER" in report.text()


def test_core_metadata_sidecar_and_missing_entity_fail(tmp_path):
    published = write_core(tmp_path / "p")
    other = core_table("variation").replace_schema_metadata(
        {**CORE_METADATA, "bio.vep.cache_version": "115"}
    )
    built = write_core(tmp_path / "b", {("variation", "chr1"): other})
    (built / "reference_policy.json").write_text(json.dumps({"schema_version": 2}))
    (built / "chr_synonyms.txt").write_text("1\tCM000663.2\n")
    shutil.rmtree(built / "transcript")
    report = cache_parity.compare_core_cache(built, published)
    text = problems(report)
    assert (
        "schema: metadata bio.vep.cache_version is '115' in the rebuilt cache" in text
    )
    assert "reference_policy.json differs" in text
    assert "chr_synonyms.txt differs between the two caches" in text
    assert (
        "transcript/chrom_manifest.json is missing or unreadable in the rebuilt" in text
    )
    missing = [s for s in report.shards if s.entity == "transcript"]
    assert [s.problems for s in missing] == [
        ["shard missing from the rebuilt cache"]
    ] * 2


def test_empty_directories_are_not_a_pass(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    assert not cache_parity.compare_core_cache(tmp_path / "a", tmp_path / "b").ok
    assert not cache_parity.compare_plugin_cache(tmp_path / "a", tmp_path / "b").ok


# ---- CLI -----------------------------------------------------------------


def test_cli_compare_plugin_exit_codes_and_files(tmp_path, published, capsys):
    built = write_plugin(tmp_path / "built")
    summary, report = tmp_path / "out/summary.txt", tmp_path / "out/report.json"
    args = ["--summary", str(summary), "--report", str(report)]
    assert cache_parity.main(["compare-plugin", str(built), str(published), *args]) == 0
    assert summary.read_text().splitlines()[-1].startswith("PASS: 2 of 2 shards")
    assert json.loads(report.read_text())["ok"] is True
    pq.write_table(without_row(0), built / "chr1.parquet")
    assert cache_parity.main(["compare-plugin", str(built), str(published), *args]) == 1
    assert summary.read_text().splitlines()[-1].startswith("FAIL: 1 of 2 shards differ")
    data = json.loads(report.read_text())
    assert data["ok"] is False and data["shards"][0]["rows_built"] == 3
    assert "FAIL:" in capsys.readouterr().out


def test_cli_compare_core(tmp_path):
    built, published = write_core(tmp_path / "b"), write_core(tmp_path / "p")
    assert cache_parity.main(["compare-core", str(built), str(published)]) == 0


# ---- the /parity-caches job ---------------------------------------------

CONFIG = {
    "release": 116,
    "assembly": "GRCh38",
    "core": {"merged": {"raw": "vep-cache/homo_sapiens_merged/116_GRCh38"}},
    "plugin_tiering_cache": "merged",
    "plugins": {"demo": {}},
}
PINS = {(116, "GRCh38", "merged"): "a" * 40, (116, "GRCh38", "plugin_demo"): "b" * 40}


class FakeVepyr:
    """The public API surface the job uses, building from prepared directories."""

    __version__ = "9.9.9"
    _published = SimpleNamespace(
        PINS=PINS,
        cache_repo_id=lambda r, a, t: f"org/vepyr_{r}_{a}_{t}",
        plugin_repo_id=lambda r, a, p: f"org/vepyr_{r}_{a}_plugin_{p}",
    )

    def __init__(self, core=None, plugin=None, hub=None):
        self.core, self.plugin, self.hub = core, plugin, hub
        self.calls = []

    def build_cache(self, release, cache_dir, **kwargs):
        self.calls.append(("build_cache", release, kwargs))
        shutil.copytree(self.core, Path(cache_dir) / self.core.name)
        return []

    def build_plugin_cache(self, plugin, version, **kwargs):
        self.calls.append(("build_plugin_cache", plugin, version, kwargs))
        target = Path(kwargs["plugin_cache_root"]) / "plugin" / plugin
        shutil.copytree(self.plugin, target)
        return []

    def download_plugin_cache(self, plugin, release, root, **kwargs):
        self.calls.append(("download_plugin_cache", plugin, release, kwargs))
        target = Path(root) / "plugin" / plugin
        shutil.copytree(self.hub, target)
        return str(target)


def stage(data: Path, *, core_revision: str = "a" * 40) -> Path:
    """Staged data as the runner holds it: published core cache, raw cache, source."""
    published = write_core(data / "cache")
    (published / ".revision").write_text(
        json.dumps(
            {"repo_id": "org/vepyr_116_GRCh38_merged", "revision": core_revision}
        )
    )
    raw = data / "vep-cache/homo_sapiens_merged/116_GRCh38"
    raw.mkdir(parents=True)
    (raw / "info.txt").write_text("species\thomo_sapiens\n")
    source = data / "plugin-source/demo"
    source.mkdir(parents=True)
    (source / "demo.vcf.gz").write_bytes(b"vcf")
    return published


def test_run_core_builds_from_the_staged_raw_cache_and_compares(tmp_path):
    data, work = tmp_path / "data", tmp_path / "work"
    published = stage(data)
    api = FakeVepyr(core=published)
    report = cache_parity.run_target(
        "merged", data=data, work=work, config=CONFIG, api=api
    )
    assert report.ok, problems(report)
    assert (
        "merged rebuilt with vepyr 9.9.9 vs org/vepyr_116_GRCh38_merged@aaaaaaaaaaaa"
        in (report.label)
    )
    (name, release, kwargs) = api.calls[0]
    assert (name, release) == ("build_cache", 116)
    assert kwargs == {
        "cache_type": "merged",
        "assembly": "GRCh38",
        "local_cache": str(data / "vep-cache/homo_sapiens_merged/116_GRCh38"),
        "show_progress": False,
    }


def test_run_core_reports_a_mismatch(tmp_path):
    data = tmp_path / "data"
    stage(data)
    broken = write_core(
        tmp_path / "other", {("variation", "chr1"): core_table("variation").slice(1)}
    )
    report = cache_parity.run_target(
        "merged",
        data=data,
        work=tmp_path / "work",
        config=CONFIG,
        api=FakeVepyr(core=broken),
    )
    assert report.verdict().startswith("FAIL: 1 of 4 shards differ (variation/chr1)")


def test_run_refuses_stale_or_missing_staged_data(tmp_path):
    data, work = tmp_path / "data", tmp_path / "work"
    published = stage(data, core_revision="f" * 40)
    api = FakeVepyr(core=published)
    with pytest.raises(
        cache_parity.RunnerNotReady, match="is not org/vepyr_116_GRCh38_merged@a"
    ):
        cache_parity.run_target("merged", data=data, work=work, config=CONFIG, api=api)
    (published / ".revision").write_text(
        json.dumps({"repo_id": "org/vepyr_116_GRCh38_merged", "revision": "a" * 40})
    )
    (data / "vep-cache/homo_sapiens_merged/116_GRCh38/info.txt").unlink()
    with pytest.raises(
        cache_parity.RunnerNotReady, match="raw Ensembl VEP cache missing"
    ):
        cache_parity.run_target("merged", data=data, work=work, config=CONFIG, api=api)
    assert api.calls == []


def test_run_refuses_too_little_scratch(tmp_path):
    data = tmp_path / "data"
    published = stage(data)
    config = {
        **CONFIG,
        "core": {"merged": {**CONFIG["core"]["merged"], "scratch_gb": 10**9}},
    }
    with pytest.raises(cache_parity.RunnerNotReady, match="needs 1000000000 GiB"):
        cache_parity.run_target(
            "merged",
            data=data,
            work=tmp_path / "work",
            config=config,
            api=FakeVepyr(core=published),
        )


def test_run_unpinned_target_is_an_error_not_a_verdict(tmp_path):
    with pytest.raises(ValueError, match="no pinned revision"):
        cache_parity.run_target(
            "plugin_other",
            data=tmp_path,
            work=tmp_path / "work",
            config=CONFIG,
            api=FakeVepyr(),
        )


def test_run_plugin_downloads_the_pin_and_rebuilds_from_its_manifest(tmp_path):
    data, work = tmp_path / "data", tmp_path / "work"
    stage(data)
    hub = write_plugin(tmp_path / "hub")
    api = FakeVepyr(plugin=hub, hub=hub)
    report = cache_parity.run_target(
        "plugin_demo", data=data, work=work, config=CONFIG, api=api
    )
    assert report.ok, problems(report)
    download, build = api.calls
    assert download[:3] == ("download_plugin_cache", "demo", 116)
    assert build[:3] == ("build_plugin_cache", "demo", "c" * 40)
    assert build[3] == {
        "source_path": str(data / "plugin-source/demo/demo.vcf.gz"),
        "cache_dir": str(data / "cache/116_GRCh38_merged"),
        "plugin_cache_root": str(work / "built"),
        "chroms": ["chr1", "chr2"],
        "verify_source": "strict",
    }


def test_run_plugin_uses_a_staged_published_cache_only_at_the_pin(tmp_path):
    data = tmp_path / "data"
    stage(data)
    staged = write_plugin(data / "plugin-cache/116_GRCh38")
    hub = write_plugin(tmp_path / "hub")
    marker = {"repo_id": "org/vepyr_116_GRCh38_plugin_demo", "revision": "e" * 40}
    (staged / ".revision").write_text(json.dumps(marker))
    api = FakeVepyr(plugin=staged, hub=hub)
    report = cache_parity.run_target(
        "plugin_demo", data=data, work=tmp_path / "w1", config=CONFIG, api=api
    )
    assert report.ok
    # Staged at another revision: the pinned one is downloaded instead.
    assert [c[0] for c in api.calls] == ["download_plugin_cache", "build_plugin_cache"]
    marker["revision"] = "b" * 40
    (staged / ".revision").write_text(json.dumps(marker))
    api = FakeVepyr(plugin=staged)
    report = cache_parity.run_target(
        "plugin_demo", data=data, work=tmp_path / "w2", config=CONFIG, api=api
    )
    assert report.ok
    assert [c[0] for c in api.calls] == ["build_plugin_cache"]


def test_run_plugin_missing_source_is_not_ready(tmp_path):
    data = tmp_path / "data"
    stage(data)
    (data / "plugin-source/demo/demo.vcf.gz").unlink()
    hub = write_plugin(tmp_path / "hub")
    with pytest.raises(
        cache_parity.RunnerNotReady, match="staged plugin source missing"
    ):
        cache_parity.run_target(
            "plugin_demo",
            data=data,
            work=tmp_path / "work",
            config=CONFIG,
            api=FakeVepyr(plugin=hub, hub=hub),
        )


def test_plugin_build_plan_parts_derived_sources_and_manifest_ref(tmp_path):
    source = tmp_path / "plugin-source/demo"
    source.mkdir(parents=True)
    for name in ("snv.tsv.gz", "snv.tsv.gz.tbi", "indel.tsv.gz"):
        (source / name).write_bytes(b"x")
    manifest = {
        "cache_source_version": "v0.1.1@" + "d" * 40,
        "chroms": [{"chrom": "chr1", "file": "chr1.parquet"}, {"chrom": "chrMT"}],
        "sources": [
            {
                "part": "snv",
                "file": "snv.tsv.gz",
                "md5": "11",
                "verified_md5": "11",
                "index": {"file": "snv.tsv.gz.tbi"},
            },
            {
                "part": "indel",
                "file": "indel.tsv.gz",
                "md5": "22",
                "verified_md5": "33",
            },
        ],
    }
    ref, paths, verify, chroms = cache_parity.plugin_build_plan(
        "demo", manifest, {}, tmp_path
    )
    assert ref == "d" * 40
    assert chroms == ["chr1", "chrMT"]  # only the contigs the published cache holds
    assert paths == {
        "snv": str(source / "snv.tsv.gz"),
        "indel": str(source / "indel.tsv.gz"),
    }
    assert verify == "warn"  # the indel input is not the upstream bytes
    ref, *_ = cache_parity.plugin_build_plan(
        "demo", manifest, {"manifest_ref": "v0.2.1"}, tmp_path
    )
    assert ref == "v0.2.1"
    (source / "snv.tsv.gz.tbi").unlink()
    with pytest.raises(cache_parity.RunnerNotReady, match="snv.tsv.gz.tbi"):
        cache_parity.plugin_build_plan("demo", manifest, {}, tmp_path)


def test_run_plugin_notes_a_source_digest_other_than_the_published_one(tmp_path):
    data = tmp_path / "data"
    stage(data)
    hub = write_plugin(tmp_path / "hub")
    built = write_plugin(
        tmp_path / "rebuilt",
        sources=[{"file": "demo.vcf.gz", "md5": "aa", "verified_md5": "zz"}],
    )
    report = cache_parity.run_target(
        "plugin_demo",
        data=data,
        work=tmp_path / "work",
        config=CONFIG,
        api=FakeVepyr(plugin=built, hub=hub),
    )
    assert report.ok
    assert report.notes == [
        "source demo.vcf.gz has md5 zz; the published cache was built from md5 aa"
    ]


def test_cli_run_exit_codes(tmp_path, monkeypatch, capsys):
    data = tmp_path / "data"
    published = stage(data)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(CONFIG))
    summary = tmp_path / "summary.txt"
    args = [
        "run",
        "--target",
        "merged",
        "--data",
        str(data),
        "--manifest",
        str(manifest),
        "--summary",
        str(summary),
    ]
    monkeypatch.setitem(sys.modules, "vepyr", FakeVepyr(core=published))
    assert cache_parity.main([*args, "--work", str(tmp_path / "w1")]) == 0
    assert summary.read_text().splitlines()[-1].startswith("PASS:")
    (published / ".revision").write_text("stale")
    assert cache_parity.main([*args, "--work", str(tmp_path / "w2")]) == 3
    assert summary.read_text().startswith("ERROR: staged cache")
    monkeypatch.setitem(sys.modules, "vepyr", FakeVepyr(core=tmp_path / "nowhere"))
    (published / ".revision").write_text(
        json.dumps({"repo_id": "org/vepyr_116_GRCh38_merged", "revision": "a" * 40})
    )
    assert cache_parity.main([*args, "--work", str(tmp_path / "w3")]) == 4
    assert summary.read_text().startswith("ERROR: FileNotFoundError")
    capsys.readouterr()
