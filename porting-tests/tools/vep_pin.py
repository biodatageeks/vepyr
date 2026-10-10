"""Load ``tools/vep_pin.toml``, the single VEP software pin (#239).

The pin names the Ensembl VEP release that produces every data-test oracle:
its Docker image (tag and digest), its upstream source (tag and commit) and the
cache version it annotates against. Code reads it through :func:`load_pin`
instead of spelling the literals.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, fields
from functools import cache
from pathlib import Path
from typing import Final, Self

PIN_FILE: Final[Path] = Path(__file__).resolve().parent / "vep_pin.toml"
"""``tools/vep_pin.toml``."""

UPSTREAM_REPO_URL: Final[str] = "https://github.com/Ensembl/ensembl-vep"
"""The upstream repository whose tag and commit the pin names."""

_DIGEST: Final[re.Pattern[str]] = re.compile(r"sha256:[0-9a-f]{64}")
_COMMIT: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{40}")


class PinError(ValueError):
    """``tools/vep_pin.toml`` is missing a key or holds a malformed value."""


@dataclass(frozen=True, slots=True, kw_only=True)
class VepPin:
    """The ``[vep]`` table of ``tools/vep_pin.toml``.

    Attributes:
        image_tag: Docker image tag, ``<repo>:<tag>``.
        image_digest: ``sha256:<64 hex>`` digest of that tag.
        upstream_tag: Upstream git tag of the same release.
        upstream_commit: 40-hex commit the tag points at.
        cache_version: VEP cache version (``--cache_version``).
    """

    image_tag: str
    image_digest: str
    upstream_tag: str
    upstream_commit: str
    cache_version: str

    @property
    def image_repo(self) -> str:
        """The image repository: ``image_tag`` without its ``:tag``."""
        return self.image_tag.split(":", 1)[0]

    @property
    def pinned_image(self) -> str:
        """The digest-pinned image reference, as ``[vep] image`` records it."""
        return f"{self.image_repo}@{self.image_digest}"

    @classmethod
    def parse(cls, text: str, *, source: str = "vep_pin.toml") -> Self:
        """Parse and validate the pin file's text.

        Args:
            text: TOML text with one ``[vep]`` table.
            source: Name used in error messages.

        Returns:
            The validated pin.

        Raises:
            PinError: The text is not TOML, or a key is missing or malformed.
        """
        try:
            table = tomllib.loads(text).get("vep")
        except tomllib.TOMLDecodeError as exc:
            raise PinError(f"{source}: not valid TOML: {exc}") from exc
        if not isinstance(table, dict):
            raise PinError(f"{source}: missing [vep] table")
        values: dict[str, str] = {}
        for field in fields(cls):
            match table.get(field.name):
                case str() as value if value:
                    values[field.name] = value
                case _:
                    raise PinError(
                        f"{source}: [vep] {field.name} must be a non-empty string"
                    )
        pin = cls(**values)
        if ":" not in pin.image_tag or "@" in pin.image_tag:
            raise PinError(f"{source}: [vep] image_tag must be <repo>:<tag>")
        if not _DIGEST.fullmatch(pin.image_digest):
            raise PinError(f"{source}: [vep] image_digest must be sha256:<64 hex>")
        if not _COMMIT.fullmatch(pin.upstream_commit):
            raise PinError(f"{source}: [vep] upstream_commit must be 40 lowercase hex")
        if not pin.cache_version.isdigit():
            raise PinError(f"{source}: [vep] cache_version must be an integer string")
        return pin


@cache
def load_pin(path: Path = PIN_FILE) -> VepPin:
    """Read and validate the pin file (cached per path).

    Args:
        path: The pin file; defaults to :data:`PIN_FILE`.

    Returns:
        The validated pin.

    Raises:
        PinError: The file is malformed.
        OSError: The file cannot be read.
    """
    return VepPin.parse(path.read_text(encoding="utf-8"), source=str(path))
