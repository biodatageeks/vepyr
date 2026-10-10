"""Validate downloaded cache provenance."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from run_tests import fetch
from run_tests.verdict import Exit, RunTestsError

CACHE_ENV = "VEPYR_CACHE_ROOT"
DATA_DIR = "tests/data"


def _pin_revision(pins: Mapping[fetch.Flavour, fetch.DatasetPin], flavour: str) -> str:
    """Pinned revision of ``flavour``, or exit 2 when ``PINS.toml`` never pinned it."""
    try:
        return pins[fetch.Flavour(flavour)].revision
    except (KeyError, ValueError) as exc:
        raise RunTestsError(
            Exit.USAGE, f"precheck: unknown flavour {flavour!r} in PINS.toml"
        ) from exc


def _pinned_fasta_name(fasta_pin: fetch.FastaPin | None, pins_toml: Path) -> str:
    """Uncompressed FASTA basename the fetch writes, straight off the pin.

    One source of truth with :func:`run_tests.fetch.fetch_fasta`: bumping
    ``[grch38_fasta].ref`` moves both the fetch and this precheck together.
    """
    if fasta_pin is None:
        raise RunTestsError(
            Exit.USAGE,
            f"{pins_toml}: no [{fetch.FASTA_PIN}] pin — "
            "precheck cannot name the reference FASTA",
        )
    return fasta_pin.fa_name


def precheck_cache(
    root: Path,
    *,
    pins_toml: Path,
    flavours: Sequence[str] | None = None,
) -> None:
    """Fail loud when ``root`` is not a usable ``./run_tests`` cache.

    When ``flavours`` is ``None``, every flavour recorded in ``PROVENANCE.json`` is
    checked (so a chr21-only ensembl fetch is not refused for missing refseq/merged).

    Raises:
        RunTestsError: exit 3 on revision clash, exit 4 on missing provenance/FASTA.
    """
    if not root.is_dir():
        raise RunTestsError(
            Exit.INCOMPLETE,
            f"{CACHE_ENV}={root} is not a directory. "
            f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
        )
    provenance_path = root / fetch.PROVENANCE
    if not provenance_path.is_file():
        raise RunTestsError(
            Exit.INCOMPLETE,
            f"{provenance_path} missing. "
            f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
        )
    try:
        provenance = fetch.read_provenance(root)
    except RunTestsError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RunTestsError(
            Exit.INCOMPLETE, f"{provenance_path} unreadable: {exc}"
        ) from exc
    if provenance is None:
        raise RunTestsError(
            Exit.INCOMPLETE,
            f"{provenance_path} missing. "
            f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
        )
    check_flavours: tuple[str, ...]
    if flavours is None:
        check_flavours = tuple(provenance.datasets)
        if not check_flavours:
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"{provenance_path} has no datasets. "
                f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
            )
    else:
        check_flavours = tuple(flavours)
    pins, fasta_pin = fetch.load_dataset_pins(pins_toml)
    fasta_name = _pinned_fasta_name(fasta_pin, pins_toml)
    for flavour in check_flavours:
        record = provenance.datasets.get(flavour)
        if record is None:
            raise RunTestsError(
                Exit.INCOMPLETE,
                f"flavour {flavour} never fetched into {root}. "
                f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
            )
        expected = _pin_revision(pins, flavour)
        if record.revision != expected:
            raise RunTestsError(
                Exit.REVISION,
                f"flavour {flavour}: revision on disk {record.revision} != "
                f"PINS.toml {expected}. "
                f"Use a new {CACHE_ENV} for the changed pin.",
            )
        for name in fetch.CACHE_METADATA:
            metadata = root / fetch.Flavour(flavour).dir_name / name
            if not metadata.is_file():
                raise RunTestsError(
                    Exit.INCOMPLETE,
                    f"cache metadata missing at {metadata}. "
                    f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
                )
    fasta = root / fetch.FASTA_DIR / fasta_name
    fai = Path(str(fasta) + ".fai")
    if not fasta.is_file() or not fai.is_file():
        raise RunTestsError(
            Exit.INCOMPLETE,
            f"reference FASTA missing at {fasta} (need .fa and .fai). "
            f"Run: VEPYR_CACHE_ROOT={root} ./run_tests <version-or-sha>",
        )
