"""Download URLs declared by a fixture, independent of local filesystem paths.

URLs identify the intended datasets. An unverified checksum remains unverified;
replacing a local path with a download URL does not prove download provenance.
"""

import tomllib
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent


def is_download_url(value: str) -> bool:
    """Require an absolute HTTP(S) URL without credentials or whitespace."""
    try:
        url = urlsplit(value)
        return (
            url.scheme in {"http", "https"}
            and bool(url.hostname)
            and url.username is None
            and url.password is None
            and not any(c.isspace() for c in value)
        )
    except ValueError:
        return False


def cache_source(flavour: str) -> str:
    """The vepyr dataset at the revision declared in PINS.toml."""
    pins = tomllib.loads((ROOT / "PINS.toml").read_text())
    pin = pins[f"hf_cache_{flavour}"]
    return f"{pin['repo']}/tree/{pin['sha']}"


def vep_cache(flavour: str) -> str:
    """The corresponding native Ensembl release-116 cache archive."""
    if flavour not in {"ensembl", "merged", "refseq"}:
        raise ValueError(f"unknown cache flavour: {flavour!r}")
    suffix = "" if flavour == "ensembl" else f"_{flavour}"
    return (
        "https://ftp.ensembl.org/pub/release-116/variation/indexed_vep_cache/"
        f"homo_sapiens{suffix}_vep_116_GRCh38.tar.gz"
    )
