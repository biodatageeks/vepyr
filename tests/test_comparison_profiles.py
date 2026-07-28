import pytest

from comparison import profiles


def test_suffixes_have_no_leading_underscore():
    """Filename templates add separators, so stored suffixes must not."""
    for name, profile in profiles.PROFILES.items():
        assert not profile.suffix.startswith("_"), name


def test_default_profile_is_merged():
    assert profiles.DEFAULT_PROFILE == "merged"


def test_releases_are_strings():
    assert all(isinstance(r, str) for r in profiles.RELEASES)


def test_release_dirs_map_115_to_the_dotted_directory():
    assert profiles.RELEASE_DIRS["115"] == "115.2"
    assert profiles.RELEASE_DIRS["116"] == "116"


def test_resolve_derives_cache_and_reference_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "115_GRCh38_merged").mkdir(parents=True)
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    ref = ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    ref.write_text("")

    resolved = profiles.resolve("merged", "115")
    assert resolved.cache_dir == str(tmp_path / "cache" / "115_GRCh38_merged")
    assert resolved.vep_vcf == str(ref)
    assert resolved.suffix == "merged"


def test_cache_dir_falls_back_to_the_data_root(tmp_path, monkeypatch, capsys):
    """Legacy layout: caches sit at $DATA/ rather than $DATA/cache/."""
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    legacy = tmp_path / "115_GRCh38_merged"
    legacy.mkdir()
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz").write_text("")

    resolved = profiles.resolve("merged", "115")
    assert resolved.cache_dir == str(legacy)
    assert "cache/" in capsys.readouterr().err


def test_cache_dir_prefers_the_cache_subdirectory(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "115_GRCh38_merged").mkdir()
    preferred = tmp_path / "cache" / "115_GRCh38_merged"
    preferred.mkdir(parents=True)
    assert profiles.cache_dir_for("merged", "115") == str(preferred)


def test_resolve_prefers_bgzf_over_plain_reference(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "115_GRCh38_merged").mkdir(parents=True)
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf").write_text("")
    gz = ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    gz.write_text("")
    assert profiles.resolve("merged", "115").vep_vcf == str(gz)


def test_resolve_reports_what_is_available_when_the_cache_is_missing(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    ref_dir = tmp_path / "output" / "116"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz").write_text("")

    with pytest.raises(profiles.ProfileUnavailable) as excinfo:
        profiles.resolve("refseq", "116")
    message = str(excinfo.value)
    assert "116_GRCh38_refseq" in message
    assert "Available" in message


def test_resolve_accepts_explicit_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    cache = tmp_path / "custom_cache"
    cache.mkdir()
    ref = tmp_path / "custom.vcf.gz"
    ref.write_text("")
    resolved = profiles.resolve("merged", "116", cache_dir=str(cache), vep_vcf=str(ref))
    assert resolved.cache_dir == str(cache)
    assert resolved.vep_vcf == str(ref)


def test_hash_order_profiles_ignore_csq_order():
    assert profiles.PROFILES["merged_per_gene"].ignore_csq_order is True
    assert profiles.PROFILES["merged_pick_allele_gene"].ignore_csq_order is True
    assert profiles.PROFILES["merged"].ignore_csq_order is False


def test_default_input_prefers_the_input_subdirectory(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "input").mkdir()
    preferred = tmp_path / "input" / "ref.fa"
    preferred.write_text("")
    (tmp_path / "ref.fa").write_text("")
    assert profiles.default_input("ref.fa") == str(preferred)


def test_default_input_falls_back_to_the_data_root(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    legacy = tmp_path / "ref.fa"
    legacy.write_text("")
    assert profiles.default_input("ref.fa") == str(legacy)
    assert "input/" in capsys.readouterr().err


def test_default_input_returns_the_preferred_path_when_neither_exists(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    assert profiles.default_input("ref.fa") == str(tmp_path / "input" / "ref.fa")
