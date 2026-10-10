"""Load and validate ``PINS.toml`` — dataset pins for the VEP cache 116 corpus.

This repository pins Hugging Face cache datasets and the GRCh38 FASTA only.
The engine under test is a run-time argument, not a pin in this file.

Run as a script to print the pins as a Markdown table::

    uv run --frozen python tools/pins.py
"""

from __future__ import annotations

import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, get_args

__all__ = [
    "Pin",
    "PinError",
    "Role",
    "load_pins",
    "render_table",
]

type Role = Literal["dataset"]
"""What a pin is *for*. Closed set: only corpus datasets in this repository."""

_ROLES: Final[frozenset[str]] = frozenset(get_args(Role.__value__))
_FIELDS: Final[tuple[str, ...]] = ("repo", "ref", "sha", "role", "note")
_OPTIONAL_FIELDS: Final[tuple[str, ...]] = ("ensembl_sum",)
"""Keys a pin may carry besides :data:`_FIELDS`. Any other key is rejected."""
_SHA_LEN: Final[int] = 40
_SHA256_LEN: Final[int] = 64
_ENSEMBL_FTP: Final[str] = "https://ftp.ensembl.org/"
"""The one host whose dataset pins are files, not git trees: ``sha`` is a sha256."""
_DATASET_HOSTS: Final[tuple[str, ...]] = (
    "https://huggingface.co/datasets/",
    _ENSEMBL_FTP,
)
"""Where a ``dataset`` pin may point. Anything else cannot be re-fetched later."""
_ENSEMBL_SUM: Final[re.Pattern[str]] = re.compile(r"[0-9]{1,5} [0-9]+")
"""Shape of a line in Ensembl's ``CHECKSUMS``: BSD ``sum`` checksum then blocks."""
_SCHEMA_VERSION: Final[int] = 1
"""The only ``schema_version`` this loader understands."""


class PinError(ValueError):
    """``PINS.toml`` violates the pin contract."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Pin:
    """One pinned external dataset revision.

    Attributes:
        name: Table name in ``PINS.toml`` (e.g. ``hf_cache_ensembl``).
        repo: Fetch URL — a ``huggingface.co/datasets/`` repository, or the
            ``ftp.ensembl.org`` directory holding the FASTA file.
        ref: Human-facing ref (HF branch) or the file name inside ``repo`` (FTP).
        sha: Full 40-character lowercase hex Hugging Face dataset commit, or
            the 64-hex sha256 of the uncompressed FASTA for an Ensembl FTP pin.
        role: Always ``dataset`` in this repository.
        note: Free prose recording anything surprising about the pin.
        ensembl_sum: For Ensembl FTP pins only, the ``<checksum> <blocks>`` line
            from Ensembl's ``CHECKSUMS`` (BSD ``sum``).
    """

    name: str
    repo: str
    ref: str
    sha: str
    role: Role
    note: str
    ensembl_sum: str | None = None

    @property
    def short_sha(self) -> str:
        """The first 12 hex characters, for tables and commit messages."""
        return self.sha[:12]


def load_pins(path: Path) -> dict[str, Pin]:
    """Parse ``PINS.toml`` and validate every pin.

    Args:
        path: Path to the ``PINS.toml`` file.

    Returns:
        Mapping of table name to :class:`Pin`, in file order.

    Raises:
        PinError: ``schema_version`` is absent or not :data:`_SCHEMA_VERSION`, a
            pin is missing a field or carries an unknown key, names an unknown
            role, points at a host outside :data:`_DATASET_HOSTS`, carries a
            malformed SHA, misuses ``ensembl_sum``, or declares no pins at all.
        OSError: The file cannot be read.
        tomllib.TOMLDecodeError: The file is not well-formed TOML.
        UnicodeDecodeError: The file is not valid UTF-8.
    """
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    version = raw.get("schema_version")
    if isinstance(version, bool) or version != _SCHEMA_VERSION:
        raise PinError(
            f"{path}: schema_version is {version!r}, expected {_SCHEMA_VERSION} "
            "— refusing to read a file written against another schema"
        )
    pins: dict[str, Pin] = {}
    for name, table in raw.items():
        if not isinstance(table, dict):
            continue  # scalars such as `schema_version` are not pins
        for field in _FIELDS:
            if field not in table:
                raise PinError(f"pin {name!r}: missing required field {field!r}")
        if unknown := sorted(set(table) - set(_FIELDS) - set(_OPTIONAL_FIELDS)):
            raise PinError(
                f"pin {name!r}: unknown field(s) {unknown} — a misspelt optional key "
                "would be ignored, and the check it feeds silently skipped"
            )
        if table["role"] not in _ROLES:
            raise PinError(
                f"pin {name!r}: unknown role {table['role']!r} "
                f"(expected one of {sorted(_ROLES)})"
            )
        sha = table["sha"]
        repo = str(table["repo"])
        if not repo.startswith(_DATASET_HOSTS):
            raise PinError(
                f"pin {name!r}: a dataset pin must point at one of "
                f"{_DATASET_HOSTS}, got {repo!r}"
            )
        wanted = _SHA256_LEN if repo.startswith(_ENSEMBL_FTP) else _SHA_LEN
        match sha:
            case str() as s if len(s) == wanted and all(
                c in "0123456789abcdef" for c in s
            ):
                pass
            case _:
                raise PinError(
                    f"pin {name!r}: sha {sha!r} is not {wanted} hex characters "
                    "— a short or non-hex sha makes the pin unverifiable"
                )
        ensembl_sum = table.get("ensembl_sum")
        if ensembl_sum is not None:
            if not repo.startswith(_ENSEMBL_FTP):
                raise PinError(
                    f"pin {name!r}: `ensembl_sum` belongs only to a dataset pin on "
                    f"{_ENSEMBL_FTP} — it is the BSD `sum` Ensembl prints in CHECKSUMS"
                )
            if not isinstance(ensembl_sum, str) or not _ENSEMBL_SUM.fullmatch(
                ensembl_sum
            ):
                raise PinError(
                    f"pin {name!r}: ensembl_sum {ensembl_sum!r} is not `<checksum> "
                    "<blocks>` as Ensembl's CHECKSUMS prints it"
                )
        pins[name] = Pin(
            name=name,
            repo=table["repo"],
            ref=table["ref"],
            sha=sha,
            role=table["role"],
            note=table["note"],
            ensembl_sum=ensembl_sum,
        )
    if not pins:
        raise PinError(f"{path} declares no pins at all — the gate would be vacuous")
    return pins


def render_table(pins: dict[str, Pin]) -> str:
    """Render pins as a GitHub-flavoured Markdown table.

    Args:
        pins: Result of :func:`load_pins`.

    Returns:
        A Markdown table ending in a newline.
    """
    lines = ["| component | ref | sha | role |", "|---|---|---|---|"]
    lines += [
        f"| `{p.name}` | `{p.ref}` | `{p.short_sha}` | {p.role} |"
        for p in pins.values()
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Print the pin table; exit non-zero if ``PINS.toml`` cannot be trusted.

    Args:
        argv: Arguments after the program name; defaults to :data:`sys.argv`.
            The first, if given, is the path to read.

    Returns:
        ``0`` when the table was printed, ``1`` when the file was rejected.
    """
    argv = sys.argv[1:] if argv is None else argv
    path = (
        Path(argv[0]) if argv else Path(__file__).resolve().parent.parent / "PINS.toml"
    )
    try:
        print(render_table(load_pins(path)), end="")
    except (PinError, OSError, tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        print(f"PINS.toml is invalid: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
