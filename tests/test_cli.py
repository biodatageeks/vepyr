"""Tests for the `vepyr` command-line interface."""

from __future__ import annotations

import pytest

from vepyr.cli import annotate_kwargs, build_parser


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
