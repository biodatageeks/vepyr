import polars as pl
from cache_qa import invariants, manifest
from cache_qa_synthetic import SyntheticCache

ORDER = ["tier", "start", "allele_string", "symbol"]


def _load(cache: SyntheticCache) -> manifest.CacheManifest:
    return manifest.load_manifest(cache.write())


def _set_row(
    df: pl.DataFrame, row: int, column: str, value, dtype=None
) -> pl.DataFrame:
    expr = pl.when(pl.arange(0, df.height) == row).then(value).otherwise(pl.col(column))
    if dtype is not None:
        expr = expr.cast(dtype)
    return df.with_columns(expr.alias(column))


def test_schema_passes_on_clean_cache(tmp_path):
    r = invariants.check_schema(_load(SyntheticCache(tmp_path)))
    assert r.status == "pass"
    assert "3 shards" in r.detail


def test_schema_fails_on_wrong_type(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chr2"] = cache.rows["chr2"].with_columns(pl.col("start").cast(pl.Int64))
    r = invariants.check_schema(_load(cache))
    assert r.status == "fail"
    assert "chr2" in r.detail and "start" in r.detail


def test_schema_fails_on_wrong_order(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.select([c for c in df.columns if c != "score"] + ["score"])
    r = invariants.check_schema(_load(cache))
    assert r.status == "fail" and "chr1" in r.detail


def test_contig_counts_foreign_rows(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chr1"] = _set_row(cache.rows["chr1"], 0, "chrom", pl.lit("2"))
    r = invariants.check_contig(_load(cache))
    assert r.status == "fail"
    assert r.per_contig == {"chr1": 1}


def test_contig_passes(tmp_path):
    assert invariants.check_contig(_load(SyntheticCache(tmp_path))).status == "pass"


def test_tier_domain_fails_on_two(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    cache.rows["chrX"] = _set_row(df, df.height - 1, "tier", 2, pl.Int8)
    r = invariants.check_tier_domain(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chrX": 1}


def test_positions_flags_end_before_start_minus_one(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    cache.rows["chr2"] = _set_row(df, 1, "end", pl.col("start") - 2, pl.UInt32)
    r = invariants.check_positions(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr2": 1}


def test_positions_allows_insertion(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    cache.rows["chr2"] = df.with_columns(
        (pl.col("start") - 1).cast(pl.UInt32).alias("end")
    )
    assert invariants.check_positions(_load(cache)).status == "pass"


def test_allele_form_exact_accepts_shared_base(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="exact")
    cache.rows["chr1"] = cache.rows["chr1"].with_columns(
        pl.lit("AC/AT").alias("allele_string")
    )
    assert invariants.check_allele_form(_load(cache)).status == "pass"


def test_allele_form_minimised_rejects_shared_base(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="minimised")
    cache.rows["chr1"] = _set_row(
        cache.rows["chr1"], 0, "allele_string", pl.lit("AC/AT")
    )
    r = invariants.check_allele_form(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_allele_form_minimised_allows_dash(tmp_path):
    cache = SyntheticCache(tmp_path, allele_match="minimised")
    cache.rows["chr1"] = cache.rows["chr1"].with_columns(
        pl.lit("-/A").alias("allele_string")
    )
    assert invariants.check_allele_form(_load(cache)).status == "pass"


def test_allele_form_rejects_missing_slash_or_empty_ref(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    df = _set_row(df, 0, "allele_string", pl.lit("A"))
    cache.rows["chrX"] = _set_row(df, 1, "allele_string", pl.lit("/A"))
    r = invariants.check_allele_form(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chrX": 2}


def test_allele_form_warns_on_empty_alt(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.rows["chrX"] = _set_row(cache.rows["chrX"], 0, "allele_string", pl.lit("T/"))
    r = invariants.check_allele_form(_load(cache))
    assert r.status == "warn" and r.per_contig == {"chrX": 1}
    assert "empty ALT" in r.detail


def test_order_passes_on_sorted_cache(tmp_path):
    r = invariants.check_order(_load(SyntheticCache(tmp_path)))
    assert r.status == "pass" and "0 descending steps" in r.detail


def test_order_counts_one_swapped_pair(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr1"]
    idx = list(range(df.height))
    idx[3], idx[4] = idx[4], idx[3]
    cache.rows["chr1"] = df[idx]
    r = invariants.check_order(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_order_uses_match_column_as_final_key(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chr2"]
    two = (
        df.head(1)
        .with_columns(pl.lit("B").alias("symbol"))
        .vstack(df.head(1).with_columns(pl.lit("A").alias("symbol")))
    )
    cache.rows["chr2"] = two
    r = invariants.check_order(_load(cache))
    assert r.per_contig == {"chr2": 1}


def test_duplicates_fail_when_deduplicated(tmp_path):
    cache = SyntheticCache(tmp_path, assume_unique=False)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.vstack(df.head(1)).sort(ORDER)
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "fail" and r.per_contig == {"chr1": 1}


def test_duplicates_warn_when_assume_unique(tmp_path):
    cache = SyntheticCache(tmp_path, assume_unique=True)
    df = cache.rows["chr1"]
    cache.rows["chr1"] = df.vstack(df.head(2)).sort(ORDER)
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "warn" and r.per_contig == {"chr1": 2}
    assert "assume_unique" in r.detail


def test_duplicates_detail_mentions_fallback(tmp_path):
    cache = SyntheticCache(tmp_path, plugin="spliceai", assume_unique=None)
    r = invariants.check_duplicates(_load(cache))
    assert r.status == "pass" and "fallback" in r.detail


def test_manifest_counts_off_by_one(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"][1]["warm"] += 1
    cache.set_manifest(m)
    r = invariants.check_manifest_counts(_load(cache))
    assert r.status == "fail" and "chr2" in r.detail and "warm" in r.detail


def test_manifest_counts_pass(tmp_path):
    assert (
        invariants.check_manifest_counts(_load(SyntheticCache(tmp_path))).status
        == "pass"
    )


def test_manifest_files_missing_with_rows_fails(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    (plugin_dir / "chr2.parquet").unlink()
    r = invariants.check_manifest_files(manifest.load_manifest(plugin_dir))
    assert r.status == "fail" and "chr2.parquet" in r.detail


def test_manifest_files_missing_with_zero_rows_allowed(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["chroms"].append(
        {"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0}
    )
    cache.set_manifest(m)
    r = invariants.check_manifest_files(_load(cache))
    assert r.status == "pass"


def test_manifest_files_unlisted_shard_fails(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    cache.rows["chr1"].write_parquet(plugin_dir / "chr9.parquet")
    r = invariants.check_manifest_files(manifest.load_manifest(plugin_dir))
    assert r.status == "fail" and "chr9.parquet" in r.detail


def test_run_all_order_and_ids(tmp_path):
    results = invariants.run_all(_load(SyntheticCache(tmp_path)))
    assert [r.id for r in results] == [
        "schema",
        "contig",
        "order",
        "tier_domain",
        "manifest_counts",
        "manifest_files",
        "positions",
        "allele_form",
        "duplicates",
    ]
    assert all(r.status == "pass" for r in results)
