"""A data-test directory as ``bless`` sees it: ``test.toml``, ``input.vcf``, oracle.

``test.toml`` is edited line by line rather than re-serialised, so every comment
and every table ``bless`` does not own survives untouched. Each rewrite is parsed
back with :mod:`tomllib` and compared with the intended values before it is
written, so a document the line editor cannot handle (a multi-line string in a
key ``bless`` owns) is refused instead of corrupted.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from bless import BlessError

INPUT_NAME: Final[str] = "input.vcf"
ORACLE_NAME: Final[str] = "expected_output.vcf"
TOML_NAME: Final[str] = "test.toml"

NORMALIZE_COMMAND: Final[str] = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"
"""The ``[input] command`` that ``tools/normalize_input`` (#85) records.

Duplicated rather than imported because ``tools/normalize_input`` is a script,
not a module; ``tools/test_bless.py`` asserts the two stay identical.
"""


def body_md5(path: Path) -> str:
    """Return the md5 of the body of a VCF: every line not starting with ``#``.

    Lines are hashed as stored, line terminators included, so the value matches
    ``grep -v '^#' FILE | md5sum`` for a newline-terminated file.

    Args:
        path: VCF file to hash.

    Returns:
        Lower-case hex md5 digest.

    Raises:
        BlessError: If the file cannot be read.
    """
    digest = hashlib.md5(usedforsecurity=False)
    try:
        with path.open("rb") as fh:
            for line in fh:
                if not line.startswith(b"#"):
                    digest.update(line)
    except OSError as exc:
        raise BlessError(f"cannot read {path}: {exc.strerror or exc}") from exc
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class TestDir:
    """A data-test directory whose ``test.toml`` parsed.

    Attributes:
        root: The directory.
        config: Parsed ``test.toml``.
    """

    root: Path
    config: dict[str, Any]

    @property
    def toml_path(self) -> Path:
        """Path of ``test.toml``."""
        return self.root / TOML_NAME

    @property
    def input_vcf(self) -> Path:
        """Path of the normalised ``input.vcf``."""
        return self.root / INPUT_NAME

    @property
    def oracle(self) -> Path:
        """Path of ``expected_output.vcf``."""
        return self.root / ORACLE_NAME

    def table(self, name: str) -> dict[str, Any]:
        """Return table ``name`` of ``test.toml`` (empty when absent).

        Args:
            name: Top-level table name.

        Returns:
            The table, or ``{}``.

        Raises:
            BlessError: If ``name`` exists but is not a table.
        """
        value = self.config.get(name, {})
        if not isinstance(value, dict):
            raise BlessError(f"{self.toml_path}: `{name}` must be a table")
        return value

    def stored_md5(self) -> str:
        """Return ``[compare] body_md5``.

        Raises:
            BlessError: If it is missing or not a 32-hex-digit string.
        """
        value = self.table("compare").get("body_md5")
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
            raise BlessError(
                f"{self.toml_path}: [compare] body_md5 is missing or not a 32-digit "
                "md5; bless the directory first with `./bless <test-dir>`"
            )
        return value

    def require_normalised_input(self) -> None:
        """Refuse an ``input.vcf`` that did not come from ``tools/normalize_input``.

        Raises:
            BlessError: If ``input.vcf`` is missing, or ``[input] command`` is not
                the #85 command.
        """
        found = self.table("input").get("command")
        if found != NORMALIZE_COMMAND:
            what = "has no `command`" if found is None else f"has command {found!r}"
            raise BlessError(
                f"{self.toml_path}: [input] {what}; expected "
                f"command = {NORMALIZE_COMMAND!r} written by tools/normalize_input "
                "(#85) — re-create input.vcf with `tools/normalize_input <raw> "
                f"{self.root}`"
            )
        if not self.input_vcf.is_file():
            raise BlessError(
                f"{self.input_vcf} does not exist; create it with "
                f"`tools/normalize_input <raw> {self.root}`"
            )


def load(root: Path) -> TestDir:
    """Open a data-test directory.

    Args:
        root: Directory holding ``test.toml``.

    Returns:
        The parsed directory.

    Raises:
        BlessError: If the directory or ``test.toml`` is missing or invalid.
    """
    if not root.is_dir():
        raise BlessError(f"test directory {root} does not exist")
    toml_path = root / TOML_NAME
    try:
        config = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise BlessError(
            f"{toml_path} does not exist; a data-test directory needs test.toml "
            "(start it with tools/normalize_input)"
        ) from None
    except (OSError, UnicodeDecodeError) as exc:
        raise BlessError(f"cannot read {toml_path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise BlessError(f"{toml_path} is not valid TOML: {exc}") from exc
    return TestDir(root=root, config=config)


_TABLE_HEADER: Final[re.Pattern[str]] = re.compile(r"^\s*\[\[?\s*([^\]]+?)\s*\]\]?")


def _is_trivia(line: str) -> bool:
    """Say whether ``line`` is blank or a whole-line comment."""
    stripped = line.strip()
    return not stripped or stripped.startswith("#")


def set_keys(text: str, updates: Mapping[str, Mapping[str, str | list[str]]]) -> str:
    """Return ``text`` with string or string-list keys set in top-level tables.

    An existing ``key = ...`` line inside the table is replaced in place; a
    missing key is appended after the table's last key; a missing table is
    appended to the document. Everything else is kept byte for byte.

    Args:
        text: A TOML document.
        updates: ``{table: {key: value}}``; a value is written as a basic
            string or a one-line array of basic strings.

    Returns:
        The rewritten document.

    Raises:
        BlessError: If the result does not parse back to the intended values.
    """
    lines = text.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    for table, pairs in updates.items():
        rendered = {
            k: f"{k} = {json.dumps(v, ensure_ascii=False)}\n" for k, v in pairs.items()
        }
        start = next(
            (
                i
                for i, ln in enumerate(lines)
                if (m := _TABLE_HEADER.match(ln))
                and m.group(1) == table
                and "[[" not in ln
            ),
            None,
        )
        if start is None:
            if lines and lines[-1].strip():
                lines.append("\n")
            lines.append(f"[{table}]\n")
            lines.extend(rendered.values())
            continue
        end = next(
            (i for i in range(start + 1, len(lines)) if _TABLE_HEADER.match(lines[i])),
            len(lines),
        )
        pending = dict(rendered)
        for i in range(start + 1, end):
            key = lines[i].split("=", 1)[0].strip()
            if "=" in lines[i] and key in pending:
                lines[i] = pending.pop(key)
        insert_at = end
        while insert_at - 1 > start and _is_trivia(lines[insert_at - 1]):
            insert_at -= 1
        lines[insert_at:insert_at] = list(pending.values())
    rebuilt = "".join(lines)
    try:
        parsed = tomllib.loads(rebuilt)
    except tomllib.TOMLDecodeError as exc:
        raise BlessError(f"test.toml could not be updated safely: {exc}") from exc
    for table, pairs in updates.items():
        for key, value in pairs.items():
            if parsed.get(table, {}).get(key) != value:
                raise BlessError(
                    f"test.toml could not be updated safely: [{table}] {key} did not "
                    "round-trip (is it a multi-line or duplicated key?)"
                )
    return rebuilt


def write_atomically(path: Path, data: bytes | str | Iterable[bytes]) -> None:
    """Replace ``path`` with ``data`` via a same-directory temp file and rename.

    Args:
        path: Destination.
        data: Text (UTF-8), bytes, or an iterable of byte chunks.

    Raises:
        BlessError: If writing or renaming fails.
    """
    try:
        fd, tmp = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "wb") as fh:
                match data:
                    case str():
                        fh.write(data.encode("utf-8"))
                    case bytes():
                        fh.write(data)
                    case _:
                        for chunk in data:
                            fh.write(chunk)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
    except OSError as exc:
        raise BlessError(f"cannot write {path}: {exc.strerror or exc}") from exc
