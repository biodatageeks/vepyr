import json

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


@pytest.mark.parametrize("values", [("1", "1"), ("1", "chr1")])
def test_chroms_reject_duplicate_canonical_contigs(values):
    with pytest.raises(SystemExit):
        cli.parse_args(["--release", "115", "--chroms", *values])


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


@pytest.mark.parametrize(
    ("reference", "input_contigs"),
    [
        (["chr22"], ["22"]),
        (["22"], ["chr22"]),
    ],
)
def test_resolve_contigs_intersects_chr_aliases(monkeypatch, reference, input_contigs):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: reference if path == "ref.gz" else input_contigs,
    )
    args = cli.parse_args(["--release", "115"])

    assert cli.resolve_contigs(args, _fake_resolved(), "input.gz") == ["chr22"]


def test_resolve_contigs_validates_explicit_chr_aliases(monkeypatch):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: ["22"] if path == "ref.gz" else ["chr22"],
    )
    args = cli.parse_args(["--release", "115", "--chroms", "22"])

    assert cli.resolve_contigs(args, _fake_resolved(), "input.gz") == ["chr22"]


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


def test_summary_only_does_not_load_native_targets_or_live_data(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    monkeypatch.setattr(cli.report, "load_reports", lambda *_args: [])
    monkeypatch.setattr(
        cli,
        "resolve_contigs",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("explicit summary contigs must not inspect live VCFs")
        ),
    )

    def fail_if_called():
        raise AssertionError("summary-only mode must not load the native extension")

    monkeypatch.setattr(cli.annotate, "supported_vep_targets", fail_if_called)

    result = cli.main(
        [
            "--release",
            "116",
            "--skip-annotate",
            "--chroms",
            "1",
        ]
    )

    assert result == 1


def test_summary_only_auto_discovers_release_qualified_reports(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    observed = {}

    def discover(report_dir, suffix, release):
        observed.update(report_dir=report_dir, suffix=suffix, release=release)
        return ["chr1", "chr2"]

    monkeypatch.setattr(cli.report, "discover_report_contigs", discover)
    monkeypatch.setattr(cli.report, "load_reports", lambda *_args: [])
    monkeypatch.setattr(
        cli,
        "resolve_contigs",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("auto summary discovery must not inspect live VCFs")
        ),
    )
    monkeypatch.setattr(
        cli.annotate,
        "supported_vep_targets",
        lambda: (_ for _ in ()).throw(
            AssertionError("summary-only mode must not load the native extension")
        ),
    )

    result = cli.main(["--release", "116", "--skip-annotate"])

    assert result == 1
    assert observed["suffix"] == "merged"
    assert observed["release"] == "116"
    assert observed["report_dir"].endswith("e2e-testing/reports")


def test_failed_isolated_rerun_quarantines_and_excludes_old_evidence(
    monkeypatch, tmp_path
):
    module_path = tmp_path / "e2e-testing" / "scripts" / "comparison" / "cli.py"
    module_path.parent.mkdir(parents=True)
    report_dir = tmp_path / "e2e-testing" / "reports"
    report_dir.mkdir()
    stale_report = report_dir / "fast_chr1_merged_115_report.json"
    stale_ledger = report_dir / "fast_chr1_merged_115_mismatches.jsonl"
    stale_report.write_text(json.dumps({"chrom": "chr1"}))
    stale_ledger.write_text('{"kind":"old"}\n')
    observed = {}

    monkeypatch.setattr(cli, "__file__", str(module_path))
    monkeypatch.setattr(
        cli.profiles, "resolve", lambda *_args, **_kwargs: _fake_resolved()
    )
    monkeypatch.setattr(
        cli.annotate,
        "supported_vep_targets",
        lambda: ({"cache_version": "115"},),
    )
    monkeypatch.setattr(cli.vcfio, "parse_vep_header", lambda _path: {})
    monkeypatch.setattr(
        cli.vcfio, "validate_vep_reference_identity", lambda *_args: None
    )
    monkeypatch.setattr(cli.vcfio, "normalize_vcf", lambda *_args: "input.vcf.gz")
    monkeypatch.setattr(cli, "resolve_contigs", lambda *_args: ["chr1", "chr2"])
    monkeypatch.setattr(cli.report, "get_build_info", lambda: {"git": "head"})

    def run_isolated(chrom, _args):
        if chrom == "chr2":
            (report_dir / "fast_chr2_merged_115_report.json").write_text(
                json.dumps({"chrom": "chr2"})
            )
            return True
        stale_report.write_text(json.dumps({"attempt": "failed"}))
        stale_ledger.write_text('{"kind":"partial"}\n')
        return False

    monkeypatch.setattr(cli, "_run_contig_isolated", run_isolated)

    def load_reports(_report_dir, chroms, suffix, release):
        observed.update(chroms=chroms, suffix=suffix, release=release)
        return []

    monkeypatch.setattr(cli.report, "load_reports", load_reports)

    result = cli.main(["--release", "115", "--isolate", "--chroms", "1", "2"])

    assert result == 1
    assert observed == {
        "chroms": ["chr2"],
        "suffix": "merged",
        "release": "115",
    }
    assert not stale_report.exists()
    assert not stale_ledger.exists()
    quarantined_report = report_dir / (stale_report.name + ".stale")
    quarantined_ledger = report_dir / (stale_ledger.name + ".stale")
    assert json.loads(quarantined_report.read_text()) == {"attempt": "failed"}
    assert quarantined_ledger.read_text() == '{"kind":"partial"}\n'
