import polars as pl
import pytest
from cache_qa import manifest, profile
from cache_qa_synthetic import SyntheticCache


@pytest.fixture
def prof(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.with_columns(
        pl.when(pl.arange(0, df.height) == 5)
        .then(None)
        .otherwise(pl.col("raw"))
        .cast(pl.Float32)
        .alias("raw")
    )
    m = manifest.load_manifest(cache.write())
    return profile.profile_cache(m), m


def test_contig_table(prof):
    p, m = prof
    by = {c.chrom: c for c in p.contigs}
    assert by["chr1"].rows == 6 and by["chr1"].warm == 2 and by["chr1"].cold == 4
    assert by["chr1"].warm_share == pytest.approx(2 / 6)
    assert by["chr1"].start_min == 100 and by["chr1"].start_max == 150
    assert by["chr1"].bytes == (m.plugin_dir / "chr1.parquet").stat().st_size
    assert p.rows == 13 and p.warm == 6 and p.cold == 7
    assert p.bytes == sum(c.bytes for c in p.contigs)


def test_column_roles_and_null_share(prof):
    p, _ = prof
    cols = {c.name: c for c in p.columns}
    assert cols["chrom"].role == "key" and cols["tier"].role == "tier"
    assert cols["symbol"].role == "match" and cols["score"].role == "value"
    assert cols["raw"].null_share == pytest.approx(1 / 13)
    assert cols["raw"].per_contig["chr1"]["null_share"] == pytest.approx(1 / 6)
    assert cols["chrom"].distinct is None


def test_empty_share_counts_empty_string_and_dot(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chr2"] = cache.rows["chr2"].with_columns(pl.lit(".").alias("score"))
    p = profile.profile_cache(manifest.load_manifest(cache.write()))
    score = next(c for c in p.columns if c.name == "score")
    assert score.empty_share == pytest.approx(7 / 13)
    assert score.per_contig["chr2"]["empty_share"] == pytest.approx(1.0)


def test_exact_distinct_and_top_values(prof):
    p, _ = prof
    symbol = next(c for c in p.columns if c.name == "symbol")
    assert symbol.distinct == 3 and symbol.approx is False
    assert symbol.top_values[0] == ("GENE1", 6)
    assert [v for v, _ in symbol.top_values] == ["GENE1", "GENE2", "GENEX"]


def test_numeric_on_parsable_text(prof):
    p, _ = prof
    score = next(c for c in p.columns if c.name == "score")
    assert score.numeric is not None
    assert score.numeric.parsable_share == pytest.approx(8 / 13)
    assert score.numeric.min == 0.5 and score.numeric.max == 0.5


def test_numeric_on_float_column(prof):
    p, _ = prof
    raw = next(c for c in p.columns if c.name == "raw")
    assert raw.numeric.parsable_share == pytest.approx(12 / 13)
    assert raw.numeric.min == 0.0 and raw.numeric.max == 4.0
    assert raw.empty_share is None


def test_top_values_capped_and_absent_above_threshold(tmp_path, monkeypatch):
    cache = SyntheticCache(tmp_path)
    for chrom, df in cache.rows.items():
        cache.rows[chrom] = df.with_columns(
            (pl.lit(chrom) + pl.arange(0, df.height).cast(pl.String)).alias("symbol")
        )
    m = manifest.load_manifest(cache.write())
    symbol = next(c for c in profile.profile_cache(m).columns if c.name == "symbol")
    assert symbol.distinct == 13 and len(symbol.top_values) == profile.TOP_VALUES_N
    monkeypatch.setattr(profile, "TOP_VALUES_MAX_DISTINCT", 12)
    symbol = next(c for c in profile.profile_cache(m).columns if c.name == "symbol")
    assert symbol.top_values is None


def test_profile_runs_with_missing_zero_row_shard(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"].append(
        {"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0}
    )
    cache.set_manifest(m)
    p = profile.profile_cache(manifest.load_manifest(cache.write()))
    assert [c.chrom for c in p.contigs] == ["chr1", "chr2", "chrX", "chrMT"]
    assert p.contigs[-1].rows == 0 and p.contigs[-1].bytes == 0
