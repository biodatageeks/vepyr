"""Published vepyr caches on Hugging Face and the revisions this release pins.

Each cache and plugin cache is a public dataset repository. A pin is the full
commit hash of the dataset revision this vepyr release was validated against;
:func:`vepyr.download_cache` and :func:`vepyr.download_plugin_cache` fetch it
unless another revision is requested.
"""

from __future__ import annotations

HF_ORG = "biodatageeks"

# (release, assembly, name) -> commit; name is a cache type or "plugin_<plugin>".
PINS: dict[tuple[int, str, str], str] = {
    (116, "GRCh38", "ensembl"): "058d887ec86163333fcbdd20c28ef75683faa8b5",
    (116, "GRCh38", "refseq"): "ae28273ce0b4744201ddaecd71a7ea8bf2c19d95",
    (116, "GRCh38", "merged"): "5b83dd8d249106c6cc3f1c04c522b4bec716cc97",
    (116, "GRCh38", "plugin_cadd"): "bcfca1e030b6d372086f9b9f2bf7aae3295d321a",
    (116, "GRCh38", "plugin_spliceai"): "7158a9c34758b1afe8a56b9e433640b9d3845920",
    (116, "GRCh38", "plugin_alphamissense"): "00558a86d2ec30c5ed68ff291164786228534b19",
    (116, "GRCh38", "plugin_clinvar"): "44b0029ed830bd5d4666b5575610e6f3ae56a2ba",
    (
        116,
        "GRCh38",
        "plugin_phenotypeorthologous",
    ): "08b9b2ff0ae9a8d09c04ff1e6cb4ebb6838fd534",
}

_PLUGIN_PREFIX = "plugin_"


def cache_repo_id(release: int, assembly: str, cache_type: str) -> str:
    """Dataset repository of a core cache."""
    return f"{HF_ORG}/vepyr_{release}_{assembly}_{cache_type}"


def plugin_repo_id(release: int, assembly: str, plugin: str) -> str:
    """Dataset repository of a plugin cache."""
    return f"{HF_ORG}/vepyr_{release}_{assembly}_{_PLUGIN_PREFIX}{plugin}"


def _published(plugins: bool) -> str:
    """``"116 GRCh38: a, b; ..."`` for the pinned caches or plugin caches."""
    groups: dict[tuple[int, str], list[str]] = {}
    for release, assembly, name in PINS:
        if name.startswith(_PLUGIN_PREFIX) == plugins:
            groups.setdefault((release, assembly), []).append(
                name.removeprefix(_PLUGIN_PREFIX)
            )
    return "; ".join(
        f"{release} {assembly}: {', '.join(sorted(names))}"
        for (release, assembly), names in sorted(groups.items())
    )


def cache_revision(
    release: int, assembly: str, cache_type: str, revision: str | None
) -> str:
    """The revision to fetch: ``revision`` if given, else the pin."""
    if revision is not None:
        return revision
    pin = PINS.get((release, assembly, cache_type))
    if pin is None:
        raise ValueError(
            f"no published revision of {cache_repo_id(release, assembly, cache_type)} "
            f"is pinned in this vepyr release. Published caches: {_published(False)}. "
            "Pass revision= (e.g. 'main') to fetch an unpinned dataset, or convert "
            "an Ensembl VEP cache with build_cache()."
        )
    return pin


def plugin_revision(
    release: int, assembly: str, plugin: str, revision: str | None
) -> str:
    """The revision to fetch: ``revision`` if given, else the pin."""
    if plugin == "dbnsfp":
        raise ValueError(
            "dbNSFP is supported, but its licence does not allow redistribution, "
            "so no dbNSFP plugin cache is published; build it locally with "
            "build_plugin_cache()."
        )
    if revision is not None:
        return revision
    pin = PINS.get((release, assembly, _PLUGIN_PREFIX + plugin))
    if pin is None:
        raise ValueError(
            f"no published revision of {plugin_repo_id(release, assembly, plugin)} "
            f"is pinned in this vepyr release. Published plugin caches: "
            f"{_published(True)}. Pass revision= (e.g. 'main') to fetch an "
            "unpinned dataset, or build one with build_plugin_cache()."
        )
    return pin
