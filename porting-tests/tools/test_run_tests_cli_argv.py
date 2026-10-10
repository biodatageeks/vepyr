"""The ``[vepyr]`` -> ``vepyr annotate`` argv mapping (``run_tests.cli_argv``, #231)."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from run_tests.cli_argv import (
    UnmappableKey,
    annotate_argv,
    cache_dir,
    effective_runs,
)

ROOT = Path("/cache")
FASTA = ROOT / "fasta" / "ref.fa"
BASE: dict[str, object] = {
    "flavour": "merged",
    "required_contigs": ["chr21"],
    "everything": True,
    "preserve_record_layout": True,
    "reference_fasta": True,
}


def _argv(settings: dict[str, object]) -> list[str]:
    return annotate_argv(
        settings,
        input_vcf=Path("/t/input.vcf"),
        output_vcf=Path("/o/out.vcf"),
        cache_root=ROOT,
        fasta=FASTA,
    )


def _flag(argv: list[str], flag: str) -> str:
    return argv[argv.index(flag) + 1]


@pytest.mark.parametrize("size", [1, 2, 3, 5])
def test_buffer_size_flag(size):
    assert _flag(_argv({**BASE, "buffer_size": size}), "--buffer-size") == str(size)


def test_full_argv() -> None:
    assert _argv(BASE) == [
        "annotate",
        "--input_file",
        "/t/input.vcf",
        "--output_file",
        "/o/out.vcf",
        "--dir_cache",
        "/cache/116_GRCh38_merged",
        "--fasta",
        "/cache/fasta/ref.fa",
        "--cache_version",
        "116",
        "--everything",
        "--no_progress",
    ]


def test_everything_emits_flag() -> None:
    assert "--everything" in _argv(BASE)


def test_reference_fasta_emits_fasta() -> None:
    assert _flag(_argv(BASE), "--fasta") == str(FASTA)


@pytest.mark.parametrize("flavour", ["ensembl", "refseq", "merged"])
def test_flavour_picks_dir_cache(flavour: str) -> None:
    argv = _argv({**BASE, "flavour": flavour})
    assert _flag(argv, "--dir_cache") == f"/cache/116_GRCh38_{flavour}"
    assert cache_dir(ROOT, flavour) == ROOT / f"116_GRCh38_{flavour}"


def test_cli_defaults_emit_no_flag() -> None:
    """``preserve_record_layout = true`` and ``buffer_size = 5000`` are CLI defaults."""
    plain = _argv(BASE)
    assert _argv({**BASE, "buffer_size": 5000}) == plain
    assert not any("buffer" in a or "preserve" in a for a in plain)


def test_required_contigs_emits_no_flag() -> None:
    assert _argv({**BASE, "required_contigs": ["chr1", "chr2"]}) == _argv(BASE)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("preserve_record_layout", False),
        ("buffer_size", 0),
        ("buffer_size", -1),
        ("buffer_size", True),
        ("everything", False),
        ("reference_fasta", False),
        ("everything", 1),
        ("flavour", "gencode"),
        ("fields", "Consequence"),
    ],
)
def test_unmappable_raises(key: str, value: object) -> None:
    with pytest.raises(UnmappableKey) as caught:
        _argv({**BASE, key: value})
    assert caught.value.key == key
    assert caught.value.value == value
    assert key in str(caught.value)


def test_unknown_key_raises() -> None:
    with pytest.raises(UnmappableKey, match="unknown"):
        _argv({**BASE, "colour": "blue"})


def test_missing_flavour_raises() -> None:
    settings = dict(BASE)
    del settings["flavour"]
    with pytest.raises(UnmappableKey):
        _argv(settings)


def test_effective_runs_overrides() -> None:
    runs = effective_runs(BASE, [{"buffer_size": 5000}, {"flavour": "ensembl"}])
    assert runs == [{**BASE, "buffer_size": 5000}, {**BASE, "flavour": "ensembl"}]
    assert effective_runs(BASE) == [BASE]


def test_vepyr_run_overrides_map_or_raise() -> None:
    """All five runs of the buffer-size fixture are expressible."""
    repo = Path(__file__).resolve().parents[1]
    doc = tomllib.loads(
        (repo / "tests/data/runner_buffer_size_invariance/test.toml").read_text()
    )
    outcomes: list[object] = []
    for settings in effective_runs(doc["vepyr"], doc["vepyr_run"]):
        try:
            _argv(settings)
            outcomes.append("ok")
        except UnmappableKey as exc:
            outcomes.append((exc.key, exc.value))
    assert outcomes == ["ok"] * 5
