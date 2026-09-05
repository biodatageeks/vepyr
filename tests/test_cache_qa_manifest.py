import polars as pl
import pytest
from cache_qa import manifest
from cache_qa_synthetic import SyntheticCache


def test_load_manifest_reads_columns_and_contigs(tmp_path):
    plugin_dir = SyntheticCache(tmp_path).write()
    m = manifest.load_manifest(plugin_dir)
    assert m.plugin == "demo"
    assert m.key_columns == ["chrom", "start", "end", "allele_string"]
    assert m.match_columns == ["symbol"]
    assert m.value_columns == [("score", "Utf8"), ("raw", "Float32")]
    assert [c.chrom for c in m.contigs] == ["chr1", "chr2", "chrX"]
    assert m.contigs[2].bare == "X"
    assert m.assume_unique is False
    assert m.allele_match == "exact"


def test_expected_schema_order_and_types(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    specs = m.expected_schema()
    assert [s.name for s in specs] == [
        "chrom",
        "start",
        "end",
        "allele_string",
        "symbol",
        "score",
        "raw",
        "tier",
    ]
    assert [s.role for s in specs] == [
        "key",
        "key",
        "key",
        "key",
        "match",
        "value",
        "value",
        "tier",
    ]
    assert specs[1].dtype == pl.UInt32
    assert specs[5].dtype == pl.String
    assert specs[6].dtype == pl.Float32
    assert specs[7].dtype == pl.Int8


def test_keys(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    assert m.order_key() == ["tier", "start", "allele_string", "symbol"]
    assert m.probe_key() == ["chrom", "start", "end", "allele_string", "symbol"]


def test_present_shards_skips_missing_files(tmp_path):
    cache = SyntheticCache(tmp_path)
    plugin_dir = cache.write()
    (plugin_dir / "chrX.parquet").unlink()
    m = manifest.load_manifest(plugin_dir)
    assert [e.chrom for e, _ in m.present_shards()] == ["chr1", "chr2"]


def test_bare_contig_names():
    assert manifest.ContigEntry("chrMT", "chrMT.parquet", 0, 0, 0).bare == "MT"
    assert manifest.ContigEntry("chr22", "chr22.parquet", 1, 1, 0).bare == "22"


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(tmp_path / "plugin" / "nope")
    assert "manifest.json" in str(e.value)


def test_missing_key_names_the_key(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    del m["chroms"]
    cache.set_manifest(m)
    plugin_dir = cache.write()
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(plugin_dir)
    assert "chroms" in str(e.value)


def test_unknown_value_type_raises(tmp_path):
    cache = SyntheticCache(tmp_path)
    m = cache.manifest_dict()
    m["value_columns"][0]["type"] = "Decimal"
    cache.set_manifest(m)
    with pytest.raises(manifest.ManifestError) as e:
        manifest.load_manifest(cache.write())
    assert "Decimal" in str(e.value)


@pytest.mark.parametrize(
    "plugin,key,expected,fallback",
    [
        ("demo", True, True, False),
        ("demo", False, False, False),
        ("spliceai", None, True, True),
        ("cadd", None, True, True),
        ("clinvar", None, False, True),
        ("unknown", None, False, True),
    ],
)
def test_dedup_policy(tmp_path, plugin, key, expected, fallback):
    cache = SyntheticCache(tmp_path, plugin=plugin, assume_unique=key)
    m = manifest.load_manifest(cache.write())
    assume_unique, detail = manifest.dedup_policy(m)
    assert assume_unique is expected
    assert ("fallback" in detail) is fallback
