import gzip
import hashlib
import json
from pathlib import Path

import pytest
from comparison import profiles
from comparison.cli import parse_args


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


def test_raw_cache_dir_accepts_explicit_ensembl_workspace_layout(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    explicit = tmp_path / "homo_sapiens_ensembl" / "116_GRCh38"
    explicit.mkdir(parents=True)
    (explicit / "info.txt").write_text("cache_version 116\n")

    assert profiles.raw_cache_dir_for("ensembl", "116") == str(explicit)


def test_raw_cache_dir_prefers_official_ensembl_layout(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    official = tmp_path / "homo_sapiens" / "116_GRCh38"
    explicit = tmp_path / "homo_sapiens_ensembl" / "116_GRCh38"
    for path in (official, explicit):
        path.mkdir(parents=True)
        (path / "info.txt").write_text("cache_version 116\n")

    assert profiles.raw_cache_dir_for("ensembl", "116") == str(official)


@pytest.mark.parametrize("cache_type", ["merged", "refseq"])
def test_raw_cache_dir_resolves_source_specific_layout(
    tmp_path, monkeypatch, cache_type
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    raw = tmp_path / f"homo_sapiens_{cache_type}" / "115_GRCh38"
    raw.mkdir(parents=True)
    (raw / "info.txt").write_text("cache_version 115\n")

    assert profiles.raw_cache_dir_for(cache_type, "115") == str(raw)


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


def test_resolve_annotation_only_does_not_require_a_vep_reference(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    cache = tmp_path / "cache" / "115_GRCh38_merged"
    cache.mkdir(parents=True)

    resolved = profiles.resolve("merged", "115", require_reference=False)

    assert resolved.cache_dir == str(cache)
    assert resolved.vep_vcf is None


def test_resolve_summary_only_does_not_require_live_cache_or_reference(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))

    resolved = profiles.resolve(
        "merged",
        "116",
        require_cache=False,
        require_reference=False,
    )

    assert resolved.cache_dir == str(tmp_path / "cache" / "116_GRCh38_merged")
    assert resolved.vep_vcf is None


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


def _write_plugin_reference(tmp_path, chrom=22, profile="merged_plugins"):
    """Create the per-contig plugin reference the reference generators write."""
    ref_dir = tmp_path / "output" / "116" / "plugins"
    ref_dir.mkdir(parents=True, exist_ok=True)
    name = profiles.PROFILES[profile].vep_per_contig.format(chrom=chrom)
    (ref_dir / f"{name}.vcf.gz").write_text("")
    return ref_dir / f"{name}.vcf.gz"


def test_plugin_profile_resolves_and_injects_the_plugin_cache_root(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    plugin_cache = tmp_path / "cache" / "plugin_cache_116"
    plugin_cache.mkdir(parents=True)
    _write_plugin_reference(tmp_path)

    resolved = profiles.resolve("merged_plugins", "116", chrom=22)

    assert resolved.plugin_cache_root == str(plugin_cache)
    assert resolved.annotate_kwargs["plugin_cache_root"] == str(plugin_cache)


def test_phenotypeorthologous_profile_attaches_only_that_plugin(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    plugin_cache = tmp_path / "cache" / "plugin_cache_116"
    plugin_cache.mkdir(parents=True)
    _write_plugin_reference(tmp_path, chrom=22, profile="merged_phenotypeorthologous")

    resolved = profiles.resolve("merged_phenotypeorthologous", "116", chrom=22)
    assert resolved.annotate_kwargs["plugins"] == ["phenotypeorthologous"]
    ref = Path(resolved.vep_vcf)
    assert ref.name == "HG002_chr22_phenotypeorthologous_vep116.vcf.gz"
    assert ref.parent.name == "plugins"


def test_plugin_profile_fails_when_the_plugin_cache_is_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    _write_plugin_reference(tmp_path)

    with pytest.raises(profiles.ProfileUnavailable) as excinfo:
        profiles.resolve("merged_plugins", "116", chrom=22)
    assert "plugin_cache_116" in str(excinfo.value)


def test_plugin_base_profile_shares_the_reference_but_attaches_no_plugins(
    tmp_path, monkeypatch
):
    """The base variant isolates core-field differences against the same reference."""
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    _write_plugin_reference(tmp_path)
    basename = profiles.PROFILES["merged_plugins_base"].vep_basename

    assert basename == profiles.PROFILES["merged_plugins"].vep_basename

    resolved = profiles.resolve("merged_plugins_base", "116", chrom=22)
    assert resolved.plugin_cache_root is None
    assert "plugin_cache_root" not in resolved.annotate_kwargs


def test_explicit_plugin_cache_override_skips_derivation(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    custom = tmp_path / "elsewhere"
    custom.mkdir()
    ref = tmp_path / "custom.vcf.gz"
    ref.write_text("")

    resolved = profiles.resolve(
        "merged_plugins", "116", vep_vcf=str(ref), plugin_cache_root=str(custom)
    )
    assert resolved.annotate_kwargs["plugin_cache_root"] == str(custom)


def test_plugin_profile_without_a_contig_explains_the_per_contig_layout(
    monkeypatch, tmp_path
):
    """Plugin references are one file per contig, so there is nothing to slice.

    Previously the profile declared a single WGS basename that the generator
    never writes, so the documented command reported the profile "unavailable"
    even with every generated reference present.
    """
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    with pytest.raises(profiles.ProfileUnavailable) as excinfo:
        profiles.resolve("merged_plugins", "116")

    message = str(excinfo.value)
    assert "per-contig" in message
    assert "--chroms" in message and "--vep" in message


def test_plugin_reference_resolves_from_the_generated_location(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    written = _write_plugin_reference(tmp_path, chrom=7)

    assert profiles.vep_vcf_for("merged_plugins", "116", chrom=7) == str(written)
    assert profiles.vep_vcf_for("merged_plugins", "116", chrom="chr7") == str(written)
    # With no contig, availability still answers truthfully.
    assert profiles.vep_vcf_for("merged_plugins", "116") == str(written)
    assert profiles.vep_vcf_for("merged_plugins", "116", chrom=9) is None


def test_plugin_profile_without_a_contig_is_allowed_when_no_reference_is_needed(
    tmp_path, monkeypatch
):
    """Reference-free modes must not be blocked by a missing reference.

    The per-contig rejection was unconditional, so `--skip-annotate` aggregation
    of stored reports -- which reads no reference at all -- was refused for want
    of something it never opens.
    """
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))

    resolved = profiles.resolve(
        "merged_plugins",
        "116",
        require_cache=False,
        require_reference=False,
    )

    assert resolved.vep_vcf is None


def test_plugin_profile_without_a_contig_uses_a_whole_genome_reference(
    monkeypatch, tmp_path
):
    """A full VEP run with the plugins serves a multi-contig comparison directly."""
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    out = tmp_path / "output" / "116"
    out.mkdir(parents=True)
    wgs = out / (profiles.PROFILES["merged_plugins"].vep_basename + ".vcf.gz")
    wgs.write_bytes(b"")
    resolved = profiles.resolve("merged_plugins", "116", require_cache=False)
    assert resolved.vep_vcf == str(wgs)


def _write_clinvar_manifest(tmp_path, monkeypatch, golden_bytes=b"golden-vcf"):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    input_path = tmp_path / "input/clinvar/clinvar_20261011.norm.vcf.gz"
    input_path.parent.mkdir(parents=True)
    input_records = b"1\t1\t.\tA\tC\t.\tPASS\t.\n"
    input_path.write_bytes(
        gzip.compress(
            b"##fileDate=20261020\n#CHROM\tPOS\tID\tREF\tALT\n" + input_records
        )
    )
    input_index_bytes = b"normalized-input-index"
    (input_path.parent / (input_path.name + ".tbi")).write_bytes(input_index_bytes)
    golden_rel = (
        "output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz"
    )
    golden_path = tmp_path / golden_rel
    golden_path.parent.mkdir(parents=True)
    golden_path.write_bytes(golden_bytes)
    golden_index_bytes = b"golden-index"
    (golden_path.parent / (golden_path.name + ".tbi")).write_bytes(golden_index_bytes)
    manifest_path = tmp_path / "wgs-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "clinvar_profiles": {
                    "clinvar_merged": {
                        "input": {
                            "path": "input/clinvar/clinvar_20261011.norm.vcf.gz",
                            "sha256": hashlib.sha256(
                                input_path.read_bytes()
                            ).hexdigest(),
                            "size": input_path.stat().st_size,
                            "body_md5": hashlib.md5(input_records).hexdigest(),
                        },
                        "input_index": {
                            "path": "input/clinvar/clinvar_20261011.norm.vcf.gz.tbi",
                            "sha256": hashlib.sha256(input_index_bytes).hexdigest(),
                            "size": len(input_index_bytes),
                        },
                        "golden": {
                            "path": golden_rel,
                            "sha256": hashlib.sha256(golden_bytes).hexdigest(),
                            "size": len(golden_bytes),
                        },
                        "golden_index": {
                            "path": golden_rel + ".tbi",
                            "sha256": hashlib.sha256(golden_index_bytes).hexdigest(),
                            "size": len(golden_index_bytes),
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(profiles, "WGS_MANIFEST_PATH", manifest_path)
    return input_path, golden_path, manifest_path


def test_clinvar_profile_resolves_canonical_paths_and_manifest_checksum(
    tmp_path, monkeypatch
):
    input_path, golden_path, _ = _write_clinvar_manifest(tmp_path, monkeypatch)

    resolved = profiles.resolve("clinvar_merged", "116")

    assert profiles.input_vcf_for("clinvar_merged") == str(input_path)
    assert resolved.vep_vcf == str(golden_path)
    assert resolved.cache_dir == str(tmp_path / "cache/116_GRCh38_merged")
    assert profiles.PROFILES["clinvar_merged"].no_normalize is True


def test_clinvar_profile_rejects_a_golden_with_a_wrong_sha256(tmp_path, monkeypatch):
    _, golden_path, _ = _write_clinvar_manifest(
        tmp_path, monkeypatch, golden_bytes=b"golden-vcf"
    )
    # Keep the byte count unchanged so this specifically exercises SHA checking.
    golden_path.write_bytes(b"broken-vcf")

    with pytest.raises(profiles.ProfileUnavailable, match="SHA-256 mismatch"):
        profiles.resolve("clinvar_merged", "116")


def test_clinvar_profile_accepts_regenerated_header_and_validates_record_md5(
    tmp_path, monkeypatch
):
    input_path, _, _ = _write_clinvar_manifest(tmp_path, monkeypatch)
    records = b"1\t1\t.\tA\tC\t.\tPASS\t.\n"
    # A regenerated bcftools header changes the compressed size and SHA-256.
    input_path.write_bytes(
        gzip.compress(b"##fileDate=20261021\n#CHROM\tPOS\tID\tREF\tALT\n" + records)
    )

    profiles.validate_input_vcf("clinvar_merged", str(input_path))

    input_path.write_bytes(
        gzip.compress(
            b"##fileDate=20261021\n#CHROM\tPOS\tID\tREF\tALT\n"
            b"1\t2\t.\tA\tC\t.\tPASS\t.\n"
        )
    )

    with pytest.raises(
        profiles.ProfileUnavailable, match="input record-body MD5 mismatch"
    ):
        profiles.validate_input_vcf("clinvar_merged", str(input_path))


def test_clinvar_profile_rejects_a_golden_index_with_a_wrong_sha256(
    tmp_path, monkeypatch
):
    _, golden_path, _ = _write_clinvar_manifest(tmp_path, monkeypatch)
    golden_index = Path(str(golden_path) + ".tbi")
    golden_index.write_bytes(b"x" * len(b"golden-index"))

    with pytest.raises(
        profiles.ProfileUnavailable, match="golden_index SHA-256 mismatch"
    ):
        profiles.resolve("clinvar_merged", "116")


def test_clinvar_profile_sets_input_and_normalization_defaults(tmp_path, monkeypatch):
    input_path, _, _ = _write_clinvar_manifest(tmp_path, monkeypatch)

    args = parse_args(
        ["--release", "116", "--profile", "clinvar_merged", "--chroms", "all"]
    )
    explicit_normalize = parse_args(
        [
            "--release",
            "116",
            "--profile",
            "clinvar_merged",
            "--normalize",
        ]
    )
    explicit_no_normalize = parse_args(
        [
            "--release",
            "116",
            "--profile",
            "clinvar_merged",
            "--no-normalize",
        ]
    )

    assert args.vcf == str(input_path)
    assert args.no_normalize is True
    assert explicit_normalize.no_normalize is False
    assert explicit_no_normalize.no_normalize is True


def test_clinvar_manifest_does_not_expand_parity_wgs_profile_list():
    manifest_path = (
        Path(__file__).resolve().parents[1]
        / "e2e-testing"
        / "golden"
        / "wgs-manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert set(manifest["profiles"]) == {"ensembl", "merged", "refseq"}
    profile = manifest["clinvar_profiles"]["clinvar_merged"]
    assert profile["input"]["path"] == ("input/clinvar/clinvar_20261011.norm.vcf.gz")
    assert profile["golden"]["path"] == (
        "output/116/clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz"
    )
    assert profile["golden"]["sha256"] == (
        "ef4206e1a5168add794fb3defea9c061613fb608af986b3967a25349b9273564"
    )
    assert profile["golden"]["size"] == 5885304891
    assert profile["no_normalize"] is True
    assert profile["cache"]["revision"].startswith("5b83dd8")
