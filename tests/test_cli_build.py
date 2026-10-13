"""Tests for the `vepyr build-cache` and `vepyr build-plugin-cache` commands."""

from __future__ import annotations

import subprocess
import sys

import pytest

import vepyr
from tests.test_build_plugin_cache import (
    _init_full_repo,
    _write_source,
    _write_variation_shard,
)
from vepyr.cli import build_cache_kwargs, build_parser, build_plugin_cache_kwargs, main


def _parse(*argv: str):
    return build_parser().parse_args(list(argv))


CACHE = (
    "build-cache",
    "--release",
    "116",
    "--cache_type",
    "merged",
    "--dir_cache",
    "/c",
)
PLUGIN = (
    "build-plugin-cache",
    "--plugin",
    "cadd",
    "--version",
    "v0.2.0",
    "--source",
    "/src/cadd.tsv.gz",
    "--dir_cache",
    "/c/116_GRCh38_merged",
    "--plugin_cache_root",
    "/pc",
)


# --- build-cache -----------------------------------------------------------


def test_build_cache_minimal_maps_to_positionals_and_kwargs():
    args = _parse(*CACHE)
    assert args.command == "build-cache"
    assert (args.release, args.dir_cache) == (116, "/c")
    assert build_cache_kwargs(args) == {"cache_type": "merged", "show_progress": True}


def test_build_cache_optional_flags_map_to_keywords():
    args = _parse(
        *CACHE,
        "--local_cache",
        "/vep/116",
        "--entity",
        "variation",
        "--chrom",
        "chr1",
        "--chrom",
        "chrX",
        "--species",
        "mus_musculus",
        "--assembly",
        "GRCm39",
        "--partitions",
        "4",
        "--download_retries",
        "3",
        "--overwrite",
        "--no_progress",
    )
    assert build_cache_kwargs(args) == {
        "cache_type": "merged",
        "show_progress": False,
        "local_cache": "/vep/116",
        "entity": "variation",
        "chroms": ["chr1", "chrX"],
        "species": "mus_musculus",
        "assembly": "GRCm39",
        "partitions": 4,
        "download_retries": 3,
        "overwrite": True,
    }


@pytest.mark.parametrize(
    "argv",
    [
        ("build-cache", "--cache_type", "merged", "--dir_cache", "/c"),
        ("build-cache", "--release", "116", "--dir_cache", "/c"),
        ("build-cache", "--release", "116", "--cache_type", "merged"),
        (*CACHE[:4], "vep", *CACHE[5:]),
        (*CACHE, "--entity", "genes"),
        ("build-cache", "--release", "x", *CACHE[3:]),
    ],
)
def test_build_cache_bad_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        _parse(*argv)
    assert excinfo.value.code == 2


def test_build_cache_main_calls_the_api_and_prints_the_next_step(monkeypatch, capsys):
    calls = []

    def spy(release, cache_dir, **kwargs):
        calls.append((release, cache_dir, kwargs))
        return [
            ("/c/116_GRCh38_merged/variation/chr1.parquet", 10),
            ("/c/116_GRCh38_merged/variation/chr2.parquet", 5),
            ("/c/116_GRCh38_merged/transcript/chr1.parquet", 2),
        ]

    monkeypatch.setattr(vepyr, "build_cache", spy)
    assert main(list(CACHE)) == 0
    assert calls == [(116, "/c", {"cache_type": "merged", "show_progress": True})]
    out = capsys.readouterr().out
    assert "variation" in out and "15" in out
    assert "transcript" in out
    assert "--dir_cache /c/116_GRCh38_merged" in out


@pytest.mark.parametrize("error", [ValueError, FileNotFoundError, RuntimeError])
def test_build_cache_api_errors_become_exit_2(monkeypatch, capsys, error):
    def boom(*args, **kwargs):
        raise error("no such cache")

    monkeypatch.setattr(vepyr, "build_cache", boom)
    assert main(list(CACHE)) == 2
    assert capsys.readouterr().err == "vepyr: error: no such cache\n"


def test_build_cache_missing_local_cache_exits_2(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "build-cache",
            "--release",
            "116",
            "--cache_type",
            "merged",
            "--dir_cache",
            str(tmp_path),
            "--local_cache",
            str(tmp_path / "missing"),
            "--no_progress",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert result.stderr.startswith("vepyr: error: ")
    assert "Traceback" not in result.stderr


# --- build-plugin-cache ----------------------------------------------------


def test_build_plugin_cache_single_source_is_a_plain_path():
    args = _parse(*PLUGIN)
    assert (args.plugin, args.version) == ("cadd", "v0.2.0")
    assert build_plugin_cache_kwargs(args) == {
        "source_path": "/src/cadd.tsv.gz",
        "cache_dir": "/c/116_GRCh38_merged",
        "plugin_cache_root": "/pc",
    }


def test_build_plugin_cache_parts_become_a_mapping():
    argv = list(PLUGIN)
    i = argv.index("--source")
    argv[i : i + 2] = [
        "--source",
        "snv=/s/snv.tsv.gz",
        "--source",
        "indel=/s/indel.tsv.gz",
    ]
    kwargs = build_plugin_cache_kwargs(_parse(*argv))
    assert kwargs["source_path"] == {"snv": "/s/snv.tsv.gz", "indel": "/s/indel.tsv.gz"}


def test_build_plugin_cache_optional_flags_map_to_keywords():
    args = _parse(
        *PLUGIN,
        "--chrom",
        "chr22",
        "--plugins_repo",
        "/repo",
        "--verify_source",
        "warn",
        "--overwrite",
    )
    kwargs = build_plugin_cache_kwargs(args)
    assert kwargs["chroms"] == ["chr22"]
    assert kwargs["plugins_repo"] == "/repo"
    assert kwargs["verify_source"] == "warn"
    assert kwargs["overwrite"] is True


@pytest.mark.parametrize(
    "sources",
    [
        ["a=/x", "/y"],  # mixing a bare path with parts
        ["/x", "/y"],  # two bare paths
        ["a=/x", "a=/y"],  # the same part twice
        ["=/x", "b=/y"],  # empty part name
        ["a=", "b=/y"],  # empty path
    ],
)
def test_build_plugin_cache_rejects_malformed_sources(sources, capsys):
    argv = [a for a in PLUGIN if a not in ("--source", "/src/cadd.tsv.gz")]
    for source in sources:
        argv += ["--source", source]
    assert main(argv) == 2
    assert capsys.readouterr().err.startswith("vepyr: error: ")


@pytest.mark.parametrize(
    "drop", ["--plugin", "--version", "--source", "--dir_cache", "--plugin_cache_root"]
)
def test_build_plugin_cache_required_flags(drop):
    argv = list(PLUGIN)
    i = argv.index(drop)
    del argv[i : i + 2]
    with pytest.raises(SystemExit) as excinfo:
        _parse(*argv)
    assert excinfo.value.code == 2


def test_build_plugin_cache_rejects_unknown_verify_mode():
    with pytest.raises(SystemExit) as excinfo:
        _parse(*PLUGIN, "--verify_source", "maybe")
    assert excinfo.value.code == 2


def test_build_plugin_cache_end_to_end(tmp_path):
    """A real one-chromosome build through the console entry point."""
    src, md5 = _write_source(tmp_path)
    repo = _init_full_repo(tmp_path, md5=md5)
    cache = _write_variation_shard(tmp_path)
    root = tmp_path / "pc"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "build-plugin-cache",
            "--plugin",
            "demo",
            "--version",
            "v0.1.0",
            "--source",
            str(src),
            "--dir_cache",
            str(cache),
            "--plugin_cache_root",
            str(root),
            "--plugins_repo",
            str(repo),
            "--chrom",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert (root / "plugin" / "demo" / "manifest.json").is_file()
    assert "chr1" in result.stdout
    assert f"--plugin_cache_root {root} --plugin demo" in result.stdout


# --- review follow-ups -------------------------------------------------------


@pytest.mark.parametrize(
    "flag, value",
    [
        ("--partitions", "0"),
        ("--partitions", "-1"),
        ("--partitions", "x"),
        ("--download_retries", "-1"),
        ("--download_retries", "x"),
    ],
)
def test_build_cache_rejects_out_of_range_numbers(flag, value):
    with pytest.raises(SystemExit) as excinfo:
        _parse(*CACHE, flag, value)
    assert excinfo.value.code == 2


def test_build_cache_accepts_zero_download_retries():
    kwargs = build_cache_kwargs(_parse(*CACHE, "--download_retries", "0"))
    assert kwargs["download_retries"] == 0


def test_build_cache_reports_when_nothing_was_written(monkeypatch, capsys):
    monkeypatch.setattr(vepyr, "build_cache", lambda *args, **kwargs: [])
    assert main([*CACHE, "--assembly", "GRCh37"]) == 0
    out = capsys.readouterr().out
    assert "No files written" in out and "--overwrite" in out
    assert "Cache ready: /c/116_GRCh37_merged" in out


def test_build_plugin_cache_bare_path_may_contain_equals():
    argv = list(PLUGIN)
    argv[argv.index("/src/cadd.tsv.gz")] = "/data/batch=1/cadd.tsv.gz"
    kwargs = build_plugin_cache_kwargs(_parse(*argv))
    assert kwargs["source_path"] == "/data/batch=1/cadd.tsv.gz"


@pytest.mark.parametrize("path", ["batch=1/cadd.tsv.gz", "snv=x.tsv.gz", "=x"])
def test_build_plugin_cache_single_source_is_always_a_path(path):
    argv = list(PLUGIN)
    argv[argv.index("/src/cadd.tsv.gz")] = path
    assert build_plugin_cache_kwargs(_parse(*argv))["source_path"] == path


def test_build_plugin_cache_part_path_may_contain_equals():
    argv = list(PLUGIN)
    i = argv.index("--source")
    argv[i : i + 2] = [
        "--source",
        "snv=/s/run=1/snv.tsv.gz",
        "--source",
        "indel=/s/indel.tsv.gz",
    ]
    kwargs = build_plugin_cache_kwargs(_parse(*argv))
    assert kwargs["source_path"] == {
        "snv": "/s/run=1/snv.tsv.gz",
        "indel": "/s/indel.tsv.gz",
    }


def test_build_plugin_cache_git_failure_becomes_exit_2(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise subprocess.CalledProcessError(
            128,
            ["git", "clone", "--quiet", "https://x", "/tmp/y"],
            stderr="remote: denied\nfatal: could not read\n",
        )

    monkeypatch.setattr(vepyr, "build_plugin_cache", boom)
    assert main(list(PLUGIN)) == 2
    err = capsys.readouterr().err
    assert err.startswith("vepyr: error: could not resolve the cadd manifest")
    assert "--version v0.2.0" in err and "denied" in err
    assert err.count("\n") == 1


def test_build_plugin_cache_unknown_version_exits_2(tmp_path):
    """An unknown tag fails in git; the command reports it in one line."""
    src, md5 = _write_source(tmp_path)
    repo = _init_full_repo(tmp_path, md5=md5)
    cache = _write_variation_shard(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "vepyr",
            "build-plugin-cache",
            "--plugin",
            "demo",
            "--version",
            "v9.9.9",
            "--source",
            str(src),
            "--dir_cache",
            str(cache),
            "--plugin_cache_root",
            str(tmp_path / "pc"),
            "--plugins_repo",
            str(repo),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert result.stderr.startswith("vepyr: error: could not resolve the demo")
    assert "Traceback" not in result.stderr


def test_build_plugin_cache_failed_clone_is_one_line(tmp_path):
    """git's own diagnostic is captured into the single error line."""
    missing = str(tmp_path / "no-such-repo")
    src, _ = _write_source(tmp_path)
    cache = _write_variation_shard(tmp_path)
    code = subprocess.run(
        [
            sys.executable,
            "-c",
            # Point the online clone at a missing repository, offline.
            "import sys, vepyr, vepyr.cli; "
            "vepyr._resolve_plugin_manifest.__wrapped__.__kwdefaults__"
            f"['repo_url'] = {missing!r}; "
            "sys.exit(vepyr.cli.main(sys.argv[1:]))",
            "build-plugin-cache",
            "--plugin",
            "demo",
            "--version",
            "v0.1.0",
            "--source",
            str(src),
            "--dir_cache",
            str(cache),
            "--plugin_cache_root",
            str(tmp_path / "pc"),
        ],
        capture_output=True,
        text=True,
    )
    assert code.returncode == 2
    assert code.stderr.startswith("vepyr: error: could not resolve the demo")
    assert code.stderr.count("\n") == 1, code.stderr
    assert "no-such-repo" in code.stderr


def test_build_cache_download_failure_becomes_exit_2(monkeypatch, capsys, tmp_path):
    """A network error during the download is a one-line error, exit 2."""
    import urllib.error

    def fail(*args, **kwargs):
        raise urllib.error.URLError("nodename nor servname provided")

    monkeypatch.setattr(vepyr, "_download_with_progress", fail)
    argv = ["build-cache", "--release", "116", "--cache_type", "merged"]
    assert main([*argv, "--dir_cache", str(tmp_path), "--no_progress"]) == 2
    err = capsys.readouterr().err
    assert err.startswith("vepyr: error: Could not download https://")
    assert err.count("\n") == 1
