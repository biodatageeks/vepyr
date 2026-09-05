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
    selected CSQ entries in Perl hash order, which carries no meaning. `plugins`
    names the plugin-cache manifests in the CSQ order of the VEP reference; a
    non-empty tuple makes resolve() require the plugin cache and pass both its
    root and that exact order to vepyr.annotate().
    """

    flavour: str
    vep_basename: str
    suffix: str
    annotate_kwargs: dict = field(default_factory=dict)
    ignore_csq_order: bool = False
    plugins: tuple = ()
    # Plugin references are generated one file per contig, under output/
    # {release_dir}/plugins/, rather than as a single WGS reference. When set,
    # this template (with {chrom}) and subdir locate them; vep_basename stays
    # as the profile's identity for reports and summaries.
    vep_per_contig: str = ""
    vep_subdir: str = ""


_PICK = {"pick_order": VEP_PICK_ORDER}

# The five plugin-cache manifest names in their release-116 VEP reference CSQ
# order. VEP emits --plugin blocks in flag order and appends --custom ClinVar.
_ALL_PLUGINS = ("spliceai", "cadd", "alphamissense", "dbnsfp", "clinvar")
_PLUGIN_REFERENCE = (
    "HG002_annotated_wgs_everything_hgvs_merged_clinvar_spliceai_cadd_am_dbnsfp"
)
# What generate_vep_plugin_references.sh actually writes, per contig.
_PLUGIN_PER_CONTIG = "HG002_chr{chrom}_5plugins_vep116_caddfix"


def plugin_reference_basename():
    """The reference basename shared by every plugin comparison profile."""
    return _PLUGIN_REFERENCE


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
    # Both profiles below read the same five-plugin VEP reference. "merged_plugins"
    # attaches the plugin cache, so every plugin CSQ field is comparable.
    # "merged_plugins_base" attaches nothing: compare_vcfs() then restricts to the
    # shared fields, which isolates whether a core-field difference comes from the
    # plugin machinery or predates it.
    "merged_plugins": Profile(
        flavour="merged",
        vep_basename=_PLUGIN_REFERENCE,
        suffix="merged_plugins",
        plugins=_ALL_PLUGINS,
        vep_per_contig=_PLUGIN_PER_CONTIG,
        vep_subdir="plugins",
    ),
    "merged_plugins_base": Profile(
        flavour="merged",
        vep_basename=_PLUGIN_REFERENCE,
        suffix="merged_plugins_base",
        vep_per_contig=_PLUGIN_PER_CONTIG,
        vep_subdir="plugins",
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
    plugin_cache_root: str | None = None


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


def plugin_cache_dir_for(release, warn=True):
    """Resolve the plugin cache built for one release.

    Plugin caches are per-release like the Parquet cache is, and live beside it
    under $DATA/cache/, so the same legacy fallback applies.
    """
    return _resolve_with_legacy_fallback(
        "cache", f"plugin_cache_{release}", os.path.isdir, warn=warn
    )


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


def _first_existing(base):
    for candidate in (base + ".vcf.gz", base + ".vcf"):
        if os.path.exists(candidate):
            return candidate
    return None


def vep_per_contig_vcf_for(profile_name, release, chrom):
    """Return one contig's plugin reference, or None when it does not exist."""
    profile = PROFILES[profile_name]
    if not profile.vep_per_contig:
        return None
    bare = str(chrom).removeprefix("chr")
    return _first_existing(
        os.path.join(
            data_dir(),
            "output",
            RELEASE_DIRS[release],
            profile.vep_subdir,
            profile.vep_per_contig.format(chrom=bare),
        )
    )


def vep_vcf_for(profile_name, release, chrom=None):
    """Return the reference path, preferring .vcf.gz, or None if neither exists.

    Plugin profiles have no single WGS reference: generate_vep_plugin_references.sh
    writes one file per contig. With a contig, resolve that file; without one,
    report the first contig that exists so availability still answers truthfully.
    """
    profile = PROFILES[profile_name]
    if profile.vep_per_contig:
        # Prefer the per-contig file; fall back to a whole-genome reference of
        # the same basename (a full VEP run with the plugins), which the
        # comparison restricts to the requested contig like any WGS reference.
        if chrom is not None:
            found = vep_per_contig_vcf_for(profile_name, release, chrom)
            if found:
                return found
        else:
            for candidate_chrom in range(1, 23):
                found = vep_per_contig_vcf_for(profile_name, release, candidate_chrom)
                if found:
                    return found
    return _first_existing(
        os.path.join(data_dir(), "output", RELEASE_DIRS[release], profile.vep_basename)
    )


def availability_table():
    """Render which profile x release combinations have both a cache and a reference."""
    lines = [f"{'profile':<32} " + " ".join(f"{r:>12}" for r in RELEASES)]
    for name in sorted(PROFILES):
        cells = []
        for release in RELEASES:
            has_cache = os.path.isdir(cache_dir_for(name, release, warn=False))
            has_ref = vep_vcf_for(name, release) is not None
            has_plugins = not PROFILES[name].plugins or os.path.isdir(
                plugin_cache_dir_for(release, warn=False)
            )
            if has_cache and has_ref and not has_plugins:
                cells.append("no plugins")
            elif has_cache and has_ref:
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
    plugin_cache_root=None,
    *,
    require_cache=True,
    require_reference=True,
    chrom=None,
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
    if vep_vcf:
        resolved_ref = vep_vcf
    elif profile.vep_per_contig and chrom is None:
        # Each contig is a separate file, so there is nothing to slice a
        # multi-contig run out of. Say so instead of reporting "unavailable" --
        # but only when a reference is actually needed. Modes that just
        # aggregate stored reports need none, and must not be blocked by the
        # absence of something they never read.
        if require_reference:
            raise ProfileUnavailable(
                f"profile {profile_name!r} uses per-contig references "
                f"({profile.vep_per_contig.format(chrom='N')}.vcf.gz under "
                f"output/{RELEASE_DIRS[release]}/{profile.vep_subdir}/). "
                "Request a single contig with --chroms, or pass --vep explicitly."
            )
        resolved_ref = None
    else:
        resolved_ref = vep_vcf_for(profile_name, release, chrom)
    resolved_plugin_cache = None
    if profile.plugins:
        resolved_plugin_cache = plugin_cache_root or plugin_cache_dir_for(release)

    problems = []
    if require_cache and not os.path.isdir(resolved_cache):
        problems.append(f"no Parquet cache at {resolved_cache}")
    if (
        require_cache
        and resolved_plugin_cache is not None
        and not os.path.isdir(resolved_plugin_cache)
    ):
        problems.append(
            f"no plugin cache at {resolved_plugin_cache} for plugins "
            f"{', '.join(profile.plugins)}"
        )
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

    annotate_kwargs = dict(profile.annotate_kwargs)
    if resolved_plugin_cache is not None:
        annotate_kwargs["plugin_cache_root"] = resolved_plugin_cache
        annotate_kwargs["plugins"] = list(profile.plugins)

    return Resolved(
        profile=profile_name,
        release=release,
        cache_dir=resolved_cache,
        vep_vcf=resolved_ref,
        annotate_kwargs=annotate_kwargs,
        suffix=profile.suffix,
        ignore_csq_order=profile.ignore_csq_order,
        plugin_cache_root=resolved_plugin_cache,
    )
