"""annotate() refuses merged/RefSeq caches that predate reference_policy.json."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vepyr import _require_reference_policy


def _cache(tmp_path, source, policy, identity=True):
    root = tmp_path / source
    (root / "variation").mkdir(parents=True)
    meta = {"bio.vep.cache_source_type": source}
    if identity:
        meta["bio.vep.cache_version"] = "116"
    schema = pa.schema([("chrom", pa.string())]).with_metadata(meta)
    pq.write_table(
        pa.table({"chrom": ["22"]}, schema=schema), root / "variation" / "chr22.parquet"
    )
    if policy:
        (root / "reference_policy.json").write_text("{}")
    return str(root)


@pytest.mark.parametrize("source", ["merged", "refseq"])
def test_bam_capable_cache_without_policy_is_rejected(tmp_path, source):
    with pytest.raises(ValueError, match="reference_policy.json"):
        _require_reference_policy(_cache(tmp_path, source, policy=False))


@pytest.mark.parametrize("source", ["merged", "refseq", "ensembl"])
def test_cache_with_policy_or_ensembl_is_accepted(tmp_path, source):
    _require_reference_policy(_cache(tmp_path, source, policy=True))
    if source == "ensembl":
        _require_reference_policy(_cache(tmp_path / "x", source, policy=False))


def test_pre_identity_cache_is_left_to_the_engine(tmp_path):
    # Shards without bio.vep.cache_version predate cache identity (test
    # fixtures); the engine cannot attach a policy to them either.
    _require_reference_policy(_cache(tmp_path, "merged", policy=False, identity=False))


def test_directory_without_parquet_variation_is_left_to_the_engine(tmp_path):
    _require_reference_policy(str(tmp_path))


def test_cache_path_with_glob_metacharacters_is_still_checked(tmp_path):
    # A path containing [ ] must not be read as a glob pattern.
    with pytest.raises(ValueError, match="reference_policy.json"):
        _require_reference_policy(
            _cache(tmp_path / "cache[v1]", "merged", policy=False)
        )


def test_unreadable_unrelated_shard_does_not_block(tmp_path):
    # A damaged shard sorted first must not stop the check or the run.
    root = _cache(tmp_path, "ensembl", policy=False)
    (tmp_path / "ensembl" / "variation" / "chr1.parquet").write_bytes(b"not parquet")
    _require_reference_policy(root)
    merged = _cache(tmp_path / "m", "merged", policy=False)
    (tmp_path / "m" / "merged" / "variation" / "chr1.parquet").write_bytes(
        b"not parquet"
    )
    with pytest.raises(ValueError, match="reference_policy.json"):
        _require_reference_policy(merged)
