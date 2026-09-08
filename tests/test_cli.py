"""Tests for the `vepyr` command-line interface."""

from __future__ import annotations

import gzip
import subprocess
import sys
from pathlib import Path

import pytest

from tests.cache_metadata import copy_cache_with_source_metadata
from vepyr.cli import annotate_kwargs, build_parser

GOLDEN_DIR = Path(__file__).parent / "data" / "golden"
GOLDEN_CACHE = GOLDEN_DIR / "cache"
GOLDEN_INPUT = GOLDEN_DIR / "input.vcf.gz"
GOLDEN_FASTA = GOLDEN_DIR / "reference.fa"


def _parse(*argv: str):
    return build_parser().parse_args(["annotate", *argv])


MINIMAL = ("-i", "in.vcf", "-o", "out.vcf.gz", "--dir_cache", "/cache")


def test_required_flags_populate_the_namespace():
    args = _parse(*MINIMAL)
    assert args.command == "annotate"
    assert args.input_file == "in.vcf"
    assert args.output_file == "out.vcf.gz"
    assert args.dir_cache == "/cache"


def test_minimal_invocation_maps_to_kwargs():
    kwargs = annotate_kwargs(_parse(*MINIMAL))
    assert kwargs == {
        "output_vcf": "out.vcf.gz",
        "show_progress": True,
        "workers": 1,
    }


def test_everything_and_fasta_are_forwarded():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--everything", "--fasta", "ref.fa"))
    assert kwargs["everything"] is True
    assert kwargs["reference_fasta"] == "ref.fa"


@pytest.mark.parametrize("flag", ["--fork", "--workers"])
def test_fork_and_workers_are_the_same_knob(flag):
    kwargs = annotate_kwargs(_parse(*MINIMAL, flag, "8"))
    assert kwargs["workers"] == 8


def test_hgvsc_is_forwarded():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--hgvsc", "--fasta", "ref.fa"))
    assert kwargs["hgvsc"] is True


def test_hgvsc_without_fasta_is_rejected_by_the_api(monkeypatch, capsys):
    # The CLI does not pre-validate this; annotate() raises and main() turns it
    # into exit 2. Guards the pairing rather than duplicating the check.
    import vepyr

    from vepyr.cli import main

    def boom(vcf, cache_dir, **kwargs):
        raise ValueError(
            "reference_fasta is required when everything/hgvs/hgvsc/hgvsp=True"
        )

    monkeypatch.setattr(vepyr, "annotate", boom)
    code = main(
        ["annotate", "-i", "in.vcf", "-o", "o.vcf", "--dir_cache", "/c", "--hgvsc"]
    )
    assert code == 2
    assert "reference_fasta is required" in capsys.readouterr().err


def test_cache_version_is_passed_as_a_string():
    # annotate() validates expected_cache_version as a string ("116"), but the
    # CLI takes an int so `--cache_version 116x` is rejected by argparse.
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--cache_version", "116"))
    assert kwargs["expected_cache_version"] == "116"


def test_repeated_plugin_flags_preserve_order():
    kwargs = annotate_kwargs(
        _parse(
            *MINIMAL,
            "--plugin_cache_root",
            "/plugins",
            "--plugin",
            "cadd",
            "--plugin",
            "clinvar",
        )
    )
    assert kwargs["plugin_cache_root"] == "/plugins"
    assert kwargs["plugins"] == ["cadd", "clinvar"]


def test_plugin_cache_root_alone_is_forwarded_without_plugins():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--plugin_cache_root", "/plugins"))
    assert kwargs["plugin_cache_root"] == "/plugins"
    assert "plugins" not in kwargs


def test_no_progress_disables_the_bar():
    kwargs = annotate_kwargs(_parse(*MINIMAL, "--no_progress"))
    assert kwargs["show_progress"] is False


def test_missing_required_flag_exits_2():
    with pytest.raises(SystemExit) as excinfo:
        _parse("-i", "in.vcf", "-o", "out.vcf.gz")
    assert excinfo.value.code == 2


def test_unknown_flag_exits_2():
    # An ext.args string copied from ensemblvep must fail loudly, never be
    # silently ignored.
    with pytest.raises(SystemExit) as excinfo:
        _parse(*MINIMAL, "--pick")
    assert excinfo.value.code == 2


def test_main_forwards_positionals_and_kwargs(monkeypatch):
    import vepyr

    calls = []

    def spy(vcf, cache_dir, **kwargs):
        calls.append((vcf, cache_dir, kwargs))
        return kwargs["output_vcf"]

    monkeypatch.setattr(vepyr, "annotate", spy)

    from vepyr.cli import main

    code = main(
        [
            "annotate",
            "-i",
            "in.vcf",
            "-o",
            "out.vcf.gz",
            "--dir_cache",
            "/cache",
            "--everything",
            "--fasta",
            "ref.fa",
            "--no_progress",
        ]
    )

    assert code == 0
    assert len(calls) == 1
    vcf, cache_dir, kwargs = calls[0]
    assert vcf == "in.vcf"
    assert cache_dir == "/cache"
    assert kwargs["everything"] is True
    assert kwargs["reference_fasta"] == "ref.fa"
    assert kwargs["output_vcf"] == "out.vcf.gz"
    assert kwargs["show_progress"] is False


@pytest.mark.parametrize("error", [ValueError, FileNotFoundError])
def test_api_errors_become_exit_2_without_a_traceback(monkeypatch, capsys, error):
    import vepyr

    def boom(vcf, cache_dir, **kwargs):
        raise error("cache is unusable")

    monkeypatch.setattr(vepyr, "annotate", boom)

    from vepyr.cli import main

    code = main(["annotate", "-i", "in.vcf", "-o", "o.vcf", "--dir_cache", "/c"])

    assert code == 2
    captured = capsys.readouterr()
    assert "cache is unusable" in captured.err
    assert "Traceback" not in captured.err


@pytest.fixture(scope="module")
def golden_cache(tmp_path_factory):
    """The golden cache, stamped with the source metadata annotate() requires."""
    if not GOLDEN_CACHE.is_dir():
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("cli_golden_cache")
    return str(copy_cache_with_source_metadata(GOLDEN_CACHE, target, "ensembl", "115"))


def test_cli_annotates_the_golden_fixture(tmp_path, golden_cache):
    output = tmp_path / "annotated.vcf.gz"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "-i",
            str(GOLDEN_INPUT),
            "-o",
            str(output),
            "--dir_cache",
            golden_cache,
            "--fasta",
            str(GOLDEN_FASTA),
            "--everything",
            "--cache_version",
            "115",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert output.exists()

    with gzip.open(output, "rt") as handle:
        lines = handle.read().splitlines()

    header = [line for line in lines if line.startswith("##")]
    records = [line for line in lines if not line.startswith("#")]

    # The fixture holds 100 variants; annotation must not drop or duplicate any.
    assert len(records) == 100
    assert any(line.startswith("##INFO=<ID=CSQ,") for line in header)
    assert all("CSQ=" in line.split("\t")[7] for line in records)


def test_cli_reports_a_bad_cache_version_and_exits_2(tmp_path, golden_cache):
    output = tmp_path / "annotated.vcf.gz"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "annotate",
            "-i",
            str(GOLDEN_INPUT),
            "-o",
            str(output),
            "--dir_cache",
            golden_cache,
            "--cache_version",
            "116",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Traceback" not in result.stderr


def test_cli_version_matches_the_installed_package():
    import vepyr

    result = subprocess.run(
        [sys.executable, "-m", "vepyr", "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == f"vepyr {vepyr.__version__}"
