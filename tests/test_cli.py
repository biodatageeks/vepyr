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


MINIMAL = (
    "-i",
    "in.vcf",
    "-o",
    "out.vcf.gz",
    "--dir_cache",
    "/cache",
    "--fasta",
    "ref.fa",
)


def test_required_flags_populate_the_namespace():
    args = _parse(*MINIMAL)
    assert args.command == "annotate"
    assert args.input_file == "in.vcf"
    assert args.output_file == "out.vcf.gz"
    assert args.dir_cache == "/cache"
    assert args.fasta == "ref.fa"


def test_minimal_invocation_maps_to_kwargs():
    kwargs = annotate_kwargs(_parse(*MINIMAL))
    assert kwargs == {
        "output_vcf": "out.vcf.gz",
        "show_progress": True,
        "workers": 1,
        "reference_fasta": "ref.fa",
    }


def test_missing_fasta_exits_2():
    # annotation always runs --everything, which needs the FASTA
    with pytest.raises(SystemExit) as excinfo:
        _parse("-i", "in.vcf", "-o", "out.vcf.gz", "--dir_cache", "/cache")
    assert excinfo.value.code == 2


@pytest.mark.parametrize("flag", ["--fork", "--workers"])
def test_fork_and_workers_are_the_same_knob(flag):
    kwargs = annotate_kwargs(_parse(*MINIMAL, flag, "8"))
    assert kwargs["workers"] == 8


@pytest.mark.parametrize("flags", [("--everything",), ("--hgvsc",)])
def test_everything_flags_are_accepted_and_ignored(flags):
    # --everything is always on, so ensemblvep-style ext.args carrying it (or
    # --hgvsc, which it implies) still parse, and change nothing.
    assert annotate_kwargs(_parse(*MINIMAL, *flags)) == annotate_kwargs(
        _parse(*MINIMAL)
    )


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


@pytest.mark.parametrize(
    "vep_flag",
    [
        ("--hgvs",),
        ("--dir", "/vep"),
        ("--plugin_cache", "/plugins"),
        ("--allow",),
    ],
)
def test_vep_flag_that_prefixes_a_vepyr_flag_exits_2(vep_flag):
    # argparse would otherwise expand an unambiguous prefix: VEP's --hgvs
    # (HGVSc and HGVSp) would silently become --hgvsc, --dir would become
    # --dir_cache.
    with pytest.raises(SystemExit) as excinfo:
        _parse(*MINIMAL, *vep_flag)
    assert excinfo.value.code == 2


def test_abbreviated_top_level_flag_exits_2(capsys):
    # `vepyr --ver annotate ...` must not expand to --version, which would
    # print the version and exit 0 without annotating anything.
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["--ver", "annotate", *MINIMAL])
    assert excinfo.value.code == 2
    assert "vepyr 0." not in capsys.readouterr().out


def test_full_version_flag_still_works(capsys):
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(["--version"])
    assert excinfo.value.code == 0
    assert capsys.readouterr().out.startswith("vepyr ")


@pytest.mark.parametrize("buffer_size", [1, 2, 3, 5, 5000])
def test_main_forwards_positionals_and_kwargs(monkeypatch, buffer_size):
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
            "--buffer-size",
            str(buffer_size),
        ]
    )

    assert code == 0
    assert len(calls) == 1
    vcf, cache_dir, kwargs = calls[0]
    assert vcf == "in.vcf"
    assert cache_dir == "/cache"
    assert "everything" not in kwargs
    assert kwargs["reference_fasta"] == "ref.fa"
    assert kwargs["output_vcf"] == "out.vcf.gz"
    assert kwargs["show_progress"] is False
    assert kwargs["buffer_size"] == buffer_size


@pytest.mark.parametrize("value", ["not-an-integer", "1.5"])
def test_buffer_size_rejects_non_integers(value):
    with pytest.raises(SystemExit) as excinfo:
        _parse(*MINIMAL, "--buffer-size", value)
    assert excinfo.value.code == 2


@pytest.mark.parametrize("error", [ValueError, FileNotFoundError])
def test_api_errors_become_exit_2_without_a_traceback(monkeypatch, capsys, error):
    import vepyr

    def boom(vcf, cache_dir, **kwargs):
        raise error("cache is unusable")

    monkeypatch.setattr(vepyr, "annotate", boom)

    from vepyr.cli import main

    code = main(["annotate", *MINIMAL])

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
            "--fasta",
            str(GOLDEN_FASTA),
            "--cache_version",
            "116",
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("value", ["0", "-1"])
def test_cli_rejects_non_positive_buffer_size(tmp_path, golden_cache, value):
    output = tmp_path / "invalid.vcf"
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
            "--buffer-size",
            value,
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "buffer_size must be a positive integer" in result.stderr
    assert "Traceback" not in result.stderr
    assert not output.exists()


def test_cli_buffer_size_preserves_output(tmp_path, golden_cache):
    bodies = []
    for size in [None, 1, 2, 3, 5]:
        output = tmp_path / f"buffer-{size}.vcf"
        argv = [
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
            "--no_progress",
        ]
        if size is not None:
            argv.extend(["--buffer-size", str(size)])
        result = subprocess.run(argv, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        bodies.append(
            b"".join(
                line
                for line in output.read_bytes().splitlines(keepends=True)
                if not line.startswith(b"#")
            )
        )
    assert len(bodies[0].splitlines()) == 100
    assert all(body == bodies[0] for body in bodies[1:])


def test_cli_version_matches_the_installed_package():
    import vepyr

    result = subprocess.run(
        [sys.executable, "-m", "vepyr", "--version"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == f"vepyr {vepyr.__version__}"
