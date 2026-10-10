"""Tests for :mod:`pins`."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest

from pins import Pin, PinError, load_pins, main, render_table

REPO_ROOT = Path(__file__).resolve().parent.parent

EXPECTED_SHAS: Final[dict[str, str]] = {
    "hf_cache_ensembl": "15a048f0585a7a40d2d40cb1c2f7806638ee500b",
    "hf_cache_refseq": "d1b97a0a72d8ed94418b7a65562e89d8a0a5b3ca",
    "hf_cache_merged": "5b83dd8d249106c6cc3f1c04c522b4bec716cc97",
    "grch38_fasta": "1e74081a49ceb9739cc14c812fbb8b3db978eb80ba8e5350beb80d8ad8dfef3b",
}
"""Tripwire: editing any SHA in ``PINS.toml`` must fail here in the same PR."""


def test_every_real_pin_loads_and_is_well_formed() -> None:
    """The committed PINS.toml must satisfy every rule the validator enforces."""
    pins = load_pins(REPO_ROOT / "PINS.toml")
    assert set(pins) == set(EXPECTED_SHAS)
    assert {name: pin.sha for name, pin in pins.items()} == EXPECTED_SHAS
    assert all(p.role == "dataset" for p in pins.values())
    assert all(len(p.sha) == 40 for p in pins.values() if p.name != "grch38_fasta")
    assert len(pins["grch38_fasta"].sha) == 64


def test_a_short_sha_is_rejected(tmp_path: Path) -> None:
    """A truncated SHA is the exact failure this validator exists to catch (AC-2)."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        'schema_version = 1\n\n[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        'sha = "391f94bd"\n'
        'role = "dataset"\n'
        'note = "n"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="40 hex"):
        load_pins(bad)


def test_a_non_hex_sha_is_rejected(tmp_path: Path) -> None:
    """Forty characters is not enough; they must all be hex."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        'schema_version = 1\n\n[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        f'sha = "{"z" * 40}"\n'
        'role = "dataset"\n'
        'note = "n"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="40 hex"):
        load_pins(bad)


def test_a_missing_field_is_rejected(tmp_path: Path) -> None:
    """Every pin carries all five fields; a missing one names itself in the error."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        'schema_version = 1\n\n[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        f'sha = "{"a" * 40}"\n'
        'role = "dataset"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="note"):
        load_pins(bad)


def test_an_unknown_role_is_rejected(tmp_path: Path) -> None:
    """`role` is a closed set so a typo cannot invent a new class of pin."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        'schema_version = 1\n\n[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        f'sha = "{"a" * 40}"\n'
        'role = "whatever"\n'
        'note = "n"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="whatever"):
        load_pins(bad)


def test_render_table_shows_every_pin_with_a_short_sha() -> None:
    """The CLI rendering is a Markdown table a human can paste into a handoff."""
    table = render_table(load_pins(REPO_ROOT / "PINS.toml"))
    assert table.startswith("| component | ref | sha | role |")
    assert "15a048f0585a" in table
    assert table.count("\n") == 6  # header + separator + 4 rows, trailing newline


def test_pin_is_frozen() -> None:
    """A pin must not be mutable in-process; the file is the SSOT."""
    pin = Pin(
        name="x",
        repo="https://huggingface.co/datasets/o/d",
        ref="main",
        sha="a" * 40,
        role="dataset",
        note="n",
    )
    with pytest.raises(AttributeError):
        pin.sha = "b" * 40  # type: ignore[misc]


def test_a_wrong_schema_version_is_rejected(tmp_path: Path) -> None:
    """A file written against another schema must not be read as if it were this one."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        'schema_version = 2\n\n[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        f'sha = "{"a" * 40}"\n'
        'role = "dataset"\n'
        'note = "n"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="schema_version"):
        load_pins(bad)


def test_a_missing_schema_version_is_rejected(tmp_path: Path) -> None:
    """`schema_version` is mandatory, so an unversioned file cannot slip through."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        '[x]\n'
        'repo = "https://huggingface.co/datasets/o/d"\n'
        'ref = "main"\n'
        f'sha = "{"a" * 40}"\n'
        'role = "dataset"\n'
        'note = "n"\n',
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="schema_version"):
        load_pins(bad)


def test_a_file_with_no_pins_is_rejected(tmp_path: Path) -> None:
    """An empty pin set would make every downstream check vacuously green."""
    bad = tmp_path / "PINS.toml"
    bad.write_text("schema_version = 1\n", encoding="utf-8")
    with pytest.raises(PinError, match="no pins at all"):
        load_pins(bad)


def test_main_prints_the_table_and_exits_zero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Positive control for the CLI failure tests below."""
    assert main([str(REPO_ROOT / "PINS.toml")]) == 0
    captured = capsys.readouterr()
    assert captured.out.startswith("| component | ref | sha | role |")
    assert captured.err == ""


def test_main_reports_a_missing_file_without_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing file is a user error, not a crash: same contract, exit 1."""
    assert main([str(tmp_path / "absent.toml")]) == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("PINS.toml is invalid: ")
    assert "absent.toml" in captured.err
    assert captured.out == ""


def test_main_reports_corrupt_toml_without_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Malformed TOML raises TOMLDecodeError inside load_pins; main must absorb it."""
    corrupt = tmp_path / "PINS.toml"
    corrupt.write_text("schema_version = 1\n[x\n", encoding="utf-8")
    assert main([str(corrupt)]) == 1
    captured = capsys.readouterr()
    assert captured.err.startswith("PINS.toml is invalid: ")
    assert captured.out == ""


def _dataset_pin(repo: str, sha: str, extra: str = "") -> str:
    """A one-pin file with role ``dataset``; ``extra`` is appended verbatim."""
    return (
        "schema_version = 1\n\n[x]\n"
        f'repo = "{repo}"\nref = "main"\nsha = "{sha}"\nrole = "dataset"\nnote = "n"\n'
        + extra
    )


def test_every_dataset_pin_is_refetchable_from_a_known_host() -> None:
    """Three HF datasets (40-hex commit) and one Ensembl FTP file (64-hex sha256)."""
    pins = load_pins(REPO_ROOT / "PINS.toml")
    assert set(pins) == {
        "hf_cache_ensembl",
        "hf_cache_refseq",
        "hf_cache_merged",
        "grch38_fasta",
    }
    for name in ("hf_cache_ensembl", "hf_cache_refseq", "hf_cache_merged"):
        assert pins[name].repo == (
            f"https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_{name[9:]}"
        )
        assert len(pins[name].sha) == 40
    assert pins["grch38_fasta"].repo.startswith("https://ftp.ensembl.org/")
    assert pins["grch38_fasta"].ref.endswith(".fa.gz")
    assert len(pins["grch38_fasta"].sha) == 64


def test_a_dataset_pin_on_an_unknown_host_is_rejected(tmp_path: Path) -> None:
    """A dataset nobody can re-fetch is not a pin."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin("https://drive.google.com/x", "a" * 40), encoding="utf-8"
    )
    with pytest.raises(PinError, match="dataset pin must point at"):
        load_pins(bad)


def test_a_huggingface_dataset_pin_needs_a_40_hex_commit(tmp_path: Path) -> None:
    """A sha256 where a commit belongs would let a rebuild slip past `revision=`."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin("https://huggingface.co/datasets/o/d", "a" * 64), encoding="utf-8"
    )
    with pytest.raises(PinError, match="40 hex"):
        load_pins(bad)


def test_an_ensembl_ftp_pin_needs_a_64_hex_sha256(tmp_path: Path) -> None:
    """The FTP publishes only a BSD `sum`; the pin is our own sha256 of the file."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin("https://ftp.ensembl.org/pub/x", "a" * 40), encoding="utf-8"
    )
    with pytest.raises(PinError, match="64 hex"):
        load_pins(bad)


def test_the_fasta_pin_carries_ensembls_published_sum() -> None:
    """The BSD `sum` from release-116 CHECKSUMS rides on the pin; HF pins have none."""
    pins = load_pins(REPO_ROOT / "PINS.toml")
    assert pins["grch38_fasta"].ensembl_sum == "22450 861294"
    assert all(
        pins[n].ensembl_sum is None
        for n in ("hf_cache_ensembl", "hf_cache_refseq", "hf_cache_merged")
    )


def test_a_misspelt_optional_key_is_rejected_not_ignored(tmp_path: Path) -> None:
    """`ensembl_summ` must fail loudly: a dropped key skips the check it feeds."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin(
            "https://ftp.ensembl.org/pub/x", "a" * 64, 'ensembl_summ = "22450 861294"\n'
        ),
        encoding="utf-8",
    )
    with pytest.raises(PinError, match=r"unknown field\(s\) \['ensembl_summ'\]"):
        load_pins(bad)


def test_ensembl_sum_is_rejected_off_an_ensembl_ftp_dataset(tmp_path: Path) -> None:
    """A HuggingFace commit has no CHECKSUMS line; carrying one would be a lie."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin(
            "https://huggingface.co/datasets/o/d", "a" * 40, 'ensembl_sum = "1 2"\n'
        ),
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="belongs only to a dataset pin on"):
        load_pins(bad)


def test_a_malformed_ensembl_sum_is_rejected(tmp_path: Path) -> None:
    """`<checksum> <blocks>` or nothing — another shape cannot be compared."""
    bad = tmp_path / "PINS.toml"
    bad.write_text(
        _dataset_pin(
            "https://ftp.ensembl.org/pub/x", "a" * 64, 'ensembl_sum = "sha256:abc"\n'
        ),
        encoding="utf-8",
    )
    with pytest.raises(PinError, match="is not `<checksum> <blocks>`"):
        load_pins(bad)
