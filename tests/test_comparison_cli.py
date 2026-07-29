import pytest

from comparison import cli, profiles


def _fake_resolved(vep_vcf="ref.gz"):
    return profiles.Resolved(
        profile="merged",
        release="115",
        cache_dir="/cache",
        vep_vcf=vep_vcf,
        annotate_kwargs={},
        suffix="merged",
        ignore_csq_order=False,
    )


def test_release_is_required():
    with pytest.raises(SystemExit):
        cli.parse_args([])


def test_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    args = cli.parse_args(["--release", "115"])
    assert args.release == "115"
    assert args.profile == profiles.DEFAULT_PROFILE == "merged"
    assert args.force is False
    assert args.bgzf is False
    assert args.workers == 1
    assert args.isolate is False
    assert args.no_normalize is False
    assert args.chroms is None


def test_release_must_be_known():
    with pytest.raises(SystemExit):
        cli.parse_args(["--release", "999"])


def test_removed_flags_are_rejected():
    for flag in (["--no-force"], ["--cache", "merged"], ["--backend", "lance"]):
        with pytest.raises(SystemExit):
            cli.parse_args(["--release", "115", *flag])


def test_chroms_normalises_bare_numbers():
    args = cli.parse_args(["--release", "115", "--chroms", "1", "22"])
    assert args.chroms == ["chr1", "chr22"]


def test_chroms_all_means_detect():
    args = cli.parse_args(["--release", "115", "--chroms", "all"])
    assert args.chroms is None


def test_workers_must_be_positive():
    with pytest.raises(SystemExit):
        cli.parse_args(["--release", "115", "--workers", "0"])


def test_resolve_contigs_intersects_reference_and_input(monkeypatch):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: (
            ["chr1", "chr2", "chr3"] if path == "ref.gz" else ["chr2", "chr3", "chr4"]
        ),
    )
    args = cli.parse_args(["--release", "115"])
    resolved = _fake_resolved(vep_vcf="ref.gz")
    assert cli.resolve_contigs(args, resolved, "input.gz") == ["chr2", "chr3"]


def test_resolve_contigs_preserves_reference_order(monkeypatch):
    """tabix -l returns coordinate order; a naive sort would give chr1, chr10, chr2."""
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: ["chr1", "chr2", "chr10"],
    )
    args = cli.parse_args(["--release", "115"])
    assert cli.resolve_contigs(args, _fake_resolved(), "input.gz") == [
        "chr1",
        "chr2",
        "chr10",
    ]


def test_resolve_contigs_falls_back_to_input_when_reference_unindexed(
    monkeypatch, capsys
):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: [] if path == "ref.vcf" else ["chr1", "chr2"],
    )
    args = cli.parse_args(["--release", "115"])
    resolved = _fake_resolved(vep_vcf="ref.vcf")
    assert cli.resolve_contigs(args, resolved, "input.gz") == ["chr1", "chr2"]
    assert "degraded" in capsys.readouterr().err.lower()


def test_resolve_contigs_rejects_an_explicit_contig_that_is_absent(monkeypatch):
    monkeypatch.setattr(cli.vcfio, "detect_contigs", lambda path: ["chr1", "chr2"])
    args = cli.parse_args(["--release", "115", "--chroms", "chr9"])
    with pytest.raises(SystemExit, match="chr9"):
        cli.resolve_contigs(args, _fake_resolved(), "input.gz")


def test_results_root_is_release_scoped(tmp_path):
    root = cli.results_root(str(tmp_path), "116")
    assert root.endswith("results/116") or root.endswith("results\\116")


def test_select_supported_target_uses_native_records_without_python_version_map():
    targets = (
        {"cache_version": "115", "vep_codebase_version": "115.2"},
        {"cache_version": "116", "vep_codebase_version": "116.0"},
    )
    assert cli.select_supported_target("116", targets) == targets[1]


@pytest.mark.parametrize("targets", [(), ({"cache_version": "115"},) * 2])
def test_select_supported_target_rejects_missing_or_duplicate_records(targets):
    with pytest.raises(ValueError, match="not uniquely supported"):
        cli.select_supported_target("115", targets)
