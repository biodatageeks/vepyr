"""Tests for `vepyr download-cache` and `vepyr download-plugin-cache`."""

from __future__ import annotations

import pytest

import vepyr
from vepyr import _published
from vepyr.cli import (
    build_parser,
    download_cache_kwargs,
    download_plugin_cache_kwargs,
    main,
)

CACHE = (
    "download-cache",
    "--release",
    "116",
    "--cache_type",
    "merged",
    "--dir_cache",
    "/c",
)
PLUGIN = (
    "download-plugin-cache",
    "--plugin",
    "clinvar",
    "--release",
    "116",
    "--plugin_cache_root",
    "/pc",
)


def _parse(*argv):
    return build_parser().parse_args(list(argv))


def test_download_cache_minimal():
    args = _parse(*CACHE)
    assert (args.release, args.dir_cache) == (116, "/c")
    assert download_cache_kwargs(args) == {
        "cache_type": "merged",
        "show_progress": True,
    }


def test_download_cache_options():
    args = _parse(
        *CACHE,
        "--assembly",
        "GRCh38",
        "--chrom",
        "22",
        "--chrom",
        "X",
        "--revision",
        "main",
        "--no_progress",
    )
    assert download_cache_kwargs(args) == {
        "cache_type": "merged",
        "show_progress": False,
        "assembly": "GRCh38",
        "chroms": ["22", "X"],
        "revision": "main",
    }


def test_download_plugin_cache_options():
    args = _parse(*PLUGIN, "--chrom", "22", "--revision", "abc", "--no_progress")
    assert (args.plugin, args.release, args.plugin_cache_root) == (
        "clinvar",
        116,
        "/pc",
    )
    assert download_plugin_cache_kwargs(args) == {
        "show_progress": False,
        "chroms": ["22"],
        "revision": "abc",
    }


@pytest.mark.parametrize(
    "argv",
    [
        CACHE[:1] + CACHE[3:],  # no --release
        CACHE[:3] + CACHE[5:],  # no --cache_type
        CACHE[:5],  # no --dir_cache
        (*CACHE[:4], "vep", *CACHE[5:]),  # bad --cache_type
        PLUGIN[:1] + PLUGIN[3:],  # no --plugin
        PLUGIN[:5],  # no --plugin_cache_root
    ],
)
def test_required_and_invalid_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        _parse(*argv)
    assert excinfo.value.code == 2


def test_download_cache_prints_source_and_next_step(monkeypatch, capsys):
    calls = []

    def spy(release, cache_dir, **kwargs):
        calls.append((release, cache_dir, kwargs))
        return "/c/116_GRCh38_merged"

    monkeypatch.setattr(vepyr, "download_cache", spy)
    assert main([*CACHE, "--chrom", "22"]) == 0
    assert calls == [
        (116, "/c", {"cache_type": "merged", "show_progress": True, "chroms": ["22"]})
    ]
    out = capsys.readouterr().out.splitlines()
    assert out == [
        f"Downloaded biodatageeks/vepyr_116_GRCh38_merged @ "
        f"{_published.PINS[(116, 'GRCh38', 'merged')][:7]} (22) "
        "-> /c/116_GRCh38_merged",
        "Next: vepyr annotate --dir_cache /c/116_GRCh38_merged ...",
    ]


def test_download_plugin_cache_prints_source_and_next_step(monkeypatch, capsys):
    monkeypatch.setattr(
        vepyr, "download_plugin_cache", lambda *a, **k: "/pc/plugin/clinvar"
    )
    assert main([*PLUGIN, "--revision", "main"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out == [
        "Downloaded biodatageeks/vepyr_116_GRCh38_plugin_clinvar @ main "
        "-> /pc/plugin/clinvar",
        "Next: vepyr annotate ... --plugin_cache_root /pc --plugin clinvar",
    ]


@pytest.mark.parametrize("error", [ValueError, RuntimeError])
@pytest.mark.parametrize(
    ("argv", "name"), [(CACHE, "download_cache"), (PLUGIN, "download_plugin_cache")]
)
def test_api_errors_become_one_line_and_exit_2(monkeypatch, capsys, error, argv, name):
    def boom(*args, **kwargs):
        raise error("no such dataset")

    monkeypatch.setattr(vepyr, name, boom)
    assert main(list(argv)) == 2
    assert capsys.readouterr().err == "vepyr: error: no such dataset\n"


def test_dbnsfp_exits_2_without_a_network_call(capsys, tmp_path):
    argv = ["download-plugin-cache", "--plugin", "dbnsfp", "--release", "116"]
    assert main([*argv, "--plugin_cache_root", str(tmp_path)]) == 2
    assert "redistribution" in capsys.readouterr().err
