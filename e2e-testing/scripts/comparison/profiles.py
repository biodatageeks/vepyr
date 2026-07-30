"""Annotation profile x Ensembl release matrix and path derivation.

A profile selects a cache flavour, a VEP reference, and the pick-mode flags
passed to vepyr.annotate(). The release selects which build of both to use.
Both paths are derived rather than hardcoded, so adding a release means adding
one RELEASE_DIRS entry.
"""

import os
import sys
from dataclasses import dataclass, field

VEP_PICK_ORDER = "biotype,rank,mane_select,tsl,canonical,appris,ccds,length"
BACKEND = "parquet"

# Releases are strings: "115.2" is a directory name, and future releases may
# not be purely numeric.
RELEASES = ("115", "116")
RELEASE_DIRS = {"115": "115.2", "116": "116"}

DEFAULT_PROFILE = "merged"

DEFAULT_VCF_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
DEFAULT_FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"

SPEC_DOC = "docs/superpowers/specs/2026-07-28-merge-comparison-runners-design.md"


class ProfileUnavailable(RuntimeError):
    """Raised when a profile x release combination has no cache or no reference."""


@dataclass(frozen=True)
class Profile:
    """One comparison scenario.

    `flavour` picks the Parquet cache ({release}_GRCh38_{flavour}). `vep_basename`
    is the reference filename inside output/{release_dir}/, without extension.
    `suffix` is stored without a leading underscore; filename templates add the
    separators. `ignore_csq_order` marks profiles where Ensembl VEP emits already
    selected CSQ entries in Perl hash order, which carries no meaning.
    """

    flavour: str
    vep_basename: str
    suffix: str
    annotate_kwargs: dict = field(default_factory=dict)
    ignore_csq_order: bool = False


_PICK = {"pick_order": VEP_PICK_ORDER}

PROFILES = {
    "ensembl": Profile(
        flavour="ensembl",
        vep_basename="HG002_annotated_wgs_everything_hgvs_vep",
        suffix="ensembl",
    ),
    "merged": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged",
        suffix="merged",
    ),
    "refseq": Profile(
        flavour="refseq",
        vep_basename="HG002_annotated_wgs_everything_hgvs_refseq",
        suffix="refseq",
    ),
    "merged_flag_pick": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_flag_pick",
        suffix="merged_flag_pick",
        annotate_kwargs={"flag_pick": True, **_PICK},
    ),
    "merged_flag_pick_allele": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele",
        suffix="merged_flag_pick_allele",
        annotate_kwargs={"flag_pick_allele": True, **_PICK},
    ),
    # This local VEP artifact is misnamed: chr16 validation shows it is the
    # flag_pick_allele_gene reference, with unfiltered CSQs and PICK.
    "merged_flag_pick_allele_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick",
        suffix="merged_flag_pick_allele_gene",
        annotate_kwargs={"flag_pick_allele_gene": True, **_PICK},
    ),
    "merged_pick_filter": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_filter",
        suffix="merged_pick_filter",
        annotate_kwargs={"pick": True, **_PICK},
    ),
    "merged_pick_allele": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_allele",
        suffix="merged_pick_allele",
        annotate_kwargs={"pick_allele": True, **_PICK},
    ),
    "merged_per_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_per_gene",
        suffix="merged_per_gene",
        annotate_kwargs={"per_gene": True, **_PICK},
        ignore_csq_order=True,
    ),
    "merged_pick_allele_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene",
        suffix="merged_pick_allele_gene",
        annotate_kwargs={"pick_allele_gene": True, **_PICK},
        ignore_csq_order=True,
    ),
}


@dataclass(frozen=True)
class Resolved:
    profile: str
    release: str
    cache_dir: str
    vep_vcf: str | None
    annotate_kwargs: dict
    suffix: str
    ignore_csq_order: bool


def data_dir():
    return os.path.expanduser(
        os.path.expandvars(
            os.environ.get("DATA_VEPYR_DIR", "$HOME/workspace/data_vepyr")
        )
    )


# Legacy-location notices are emitted once per path. availability_table()
# probes every profile x release, so without this the same notice would repeat
# dozens of times in a single run.
_WARNED_LEGACY = set()


def _resolve_with_legacy_fallback(subdir, name, exists, warn=True):
    """Prefer $DATA/{subdir}/{name}, fall back to $DATA/{name} with a warning.

    Returns the preferred path when neither exists, so the caller's own existence
    check produces the error message rather than this helper inventing one.
    """
    preferred = os.path.join(data_dir(), subdir, name)
    if exists(preferred):
        return preferred
    legacy = os.path.join(data_dir(), name)
    if exists(legacy):
        if warn and legacy not in _WARNED_LEGACY:
            _WARNED_LEGACY.add(legacy)
            print(
                f"  Note: using legacy location {legacy}; move it under {subdir}/ "
                f"(see {SPEC_DOC})",
                file=sys.stderr,
            )
        return legacy
    return preferred


def default_input(name):
    """Resolve a default input file, preferring $DATA/input/ over the legacy root."""
    return _resolve_with_legacy_fallback("input", name, os.path.exists)


def cache_dir_for(profile_name, release, warn=True):
    """Resolve a Parquet cache, preferring $DATA/cache/ over the legacy root.

    `warn=False` is used by availability_table(), which probes every combination
    and would otherwise turn one diagnostic into a wall of notices.
    """
    name = f"{release}_GRCh38_{PROFILES[profile_name].flavour}"
    return _resolve_with_legacy_fallback("cache", name, os.path.isdir, warn=warn)


def raw_cache_dir_for(cache_type, release):
    """Resolve an extracted raw cache directory containing ``info.txt``.

    Official Ensembl extraction uses ``homo_sapiens`` for the Ensembl flavour.
    Existing vepyr workspaces may use the explicit ``homo_sapiens_ensembl``
    sibling naming used by merged and RefSeq. Accept both without deriving
    cache identity from the directory name; the builder validates ``info.txt``.
    """
    if cache_type not in ("ensembl", "merged", "refseq"):
        raise ValueError(f"unknown cache type {cache_type!r}")
    if release not in RELEASES:
        raise ValueError(f"unknown release {release!r}")

    if cache_type == "ensembl":
        species_dirs = ("homo_sapiens", "homo_sapiens_ensembl")
    else:
        species_dirs = (f"homo_sapiens_{cache_type}",)
    candidates = [
        os.path.join(data_dir(), species_dir, f"{release}_GRCh38")
        for species_dir in species_dirs
    ]
    for candidate in candidates:
        if os.path.isfile(os.path.join(candidate, "info.txt")):
            return candidate
    return candidates[0]


def vep_vcf_for(profile_name, release):
    """Return the reference path, preferring .vcf.gz, or None if neither exists."""
    base = os.path.join(
        data_dir(),
        "output",
        RELEASE_DIRS[release],
        PROFILES[profile_name].vep_basename,
    )
    for candidate in (base + ".vcf.gz", base + ".vcf"):
        if os.path.exists(candidate):
            return candidate
    return None


def availability_table():
    """Render which profile x release combinations have both a cache and a reference."""
    lines = [f"{'profile':<32} " + " ".join(f"{r:>12}" for r in RELEASES)]
    for name in sorted(PROFILES):
        cells = []
        for release in RELEASES:
            has_cache = os.path.isdir(cache_dir_for(name, release, warn=False))
            has_ref = vep_vcf_for(name, release) is not None
            if has_cache and has_ref:
                cells.append("ok")
            elif has_cache:
                cells.append("no reference")
            elif has_ref:
                cells.append("no cache")
            else:
                cells.append("-")
        lines.append(f"{name:<32} " + " ".join(f"{c:>12}" for c in cells))
    return "\n".join(lines)


def resolve(
    profile_name,
    release,
    cache_dir=None,
    vep_vcf=None,
    *,
    require_cache=True,
    require_reference=True,
):
    """Resolve a profile and release to concrete paths, or raise ProfileUnavailable.

    Runs before any other work so a bad combination fails in milliseconds rather
    than after a normalization pass. Explicit cache_dir / vep_vcf override the
    derived paths and skip their existence checks. The requirement flags support
    modes that only annotate (no VEP reference) or only summarize stored reports
    (no live cache or reference).
    """
    if profile_name not in PROFILES:
        raise ProfileUnavailable(
            f"Unknown profile {profile_name!r}. Known: {', '.join(sorted(PROFILES))}"
        )
    if release not in RELEASE_DIRS:
        raise ProfileUnavailable(
            f"Unknown release {release!r}. Known: {', '.join(RELEASES)}"
        )

    profile = PROFILES[profile_name]
    resolved_cache = cache_dir or cache_dir_for(profile_name, release)
    resolved_ref = vep_vcf or vep_vcf_for(profile_name, release)

    problems = []
    if require_cache and not os.path.isdir(resolved_cache):
        problems.append(f"no Parquet cache at {resolved_cache}")
    if require_reference and resolved_ref is None:
        problems.append(
            f"no VEP reference {profile.vep_basename}.vcf[.gz] under "
            f"output/{RELEASE_DIRS[release]}/"
        )
    if problems:
        raise ProfileUnavailable(
            f"Profile {profile_name!r} at release {release}: "
            + "; ".join(problems)
            + "\n\nAvailable combinations:\n"
            + availability_table()
        )

    return Resolved(
        profile=profile_name,
        release=release,
        cache_dir=resolved_cache,
        vep_vcf=resolved_ref,
        annotate_kwargs=dict(profile.annotate_kwargs),
        suffix=profile.suffix,
        ignore_csq_order=profile.ignore_csq_order,
    )
