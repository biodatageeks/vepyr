from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
import shutil
import subprocess
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal

from tqdm import tqdm

log = logging.getLogger(__name__)

_COPY_CHUNK_SIZE = 8 * 1024 * 1024
_TABIX_EXECUTABLE = "tabix"
_BGZIP_EXECUTABLE = "bgzip"


@dataclass(frozen=True)
class PluginSourceSpec:
    name: str
    source_kind: str
    fetch_mode: Literal["download", "tabix_region", "zip_members"]
    supported_assemblies: tuple[str, ...]
    default_version: str
    download_filename: Callable[[str, str], str]
    output_filename: Callable[[str, str], str]
    download_url: Callable[[str, str], str]
    validate: Callable[[Path], None]
    filter_downloaded: Callable[[Path, Path, str, str, list[str]], None] | None = None
    prepare_download: Callable[[Path, Path, str, str, list[str] | None], None] | None = None
    release_to_version: dict[int, str] | None = None


def _normalize_plugin_name(plugin_name: str) -> str:
    return plugin_name.lower().strip()


def _normalize_assembly(assembly: str) -> str:
    value = assembly.strip()
    mapping = {
        "grch38": "GRCh38",
        "hg38": "GRCh38",
        "grch37": "GRCh37",
        "hg19": "GRCh37",
    }
    return mapping.get(value.lower(), value)


def _normalize_requested_chromosomes(chromosomes: list[str] | None) -> list[str] | None:
    if not chromosomes:
        return None
    ordered: list[str] = []
    seen: set[str] = set()
    for chrom in chromosomes:
        raw = chrom.strip()
        normalized = raw[3:] if raw.lower().startswith("chr") else raw
        if not normalized:
            continue
        canonical = (
            normalized.upper() if normalized.upper() in {"X", "Y", "M", "MT"} else normalized
        )
        if canonical == "MT":
            canonical = "M"
        if canonical not in seen:
            seen.add(canonical)
            ordered.append(canonical)
    return ordered or None


def _chromosome_scope_label(chromosomes: list[str] | None) -> str:
    return "all" if not chromosomes else "chromosomes-" + "-".join(chromosomes)


def _tabix_region_aliases(chrom: str) -> list[str]:
    aliases = [chrom]
    if chrom.upper() == "M":
        aliases.extend(["MT", "chrM", "chrMT"])
    elif chrom.lower().startswith("chr"):
        bare = chrom[3:]
        aliases.append(bare)
    else:
        aliases.append(f"chr{chrom}")
    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        if alias not in seen:
            seen.add(alias)
            deduped.append(alias)
    return deduped


def _download_with_progress(url: str, dest: Path) -> None:
    filename = dest.name
    request = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    log.info("Downloading %s", url)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            total = response.headers.get("Content-Length")
            total_size = int(total) if total and total.isdigit() else None
            with (
                tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Downloading {filename}",
                    miniters=1,
                ) as progress,
                open(dest, "wb") as handle,
            ):
                while True:
                    chunk = response.read(_COPY_CHUNK_SIZE)
                    if not chunk:
                        break
                    handle.write(chunk)
                    progress.update(len(chunk))
    except (urllib.error.URLError, OSError) as exc:
        curl = shutil.which("curl")
        if curl is None:
            raise
        log.warning("urllib download failed for %s, falling back to curl: %s", url, exc)
        result = subprocess.run(
            [curl, "--fail", "--location", "--output", str(dest), url],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(f"curl download failed for {url}: {stderr}") from exc


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_COPY_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_gzip_header_columns(path: Path, required_columns: list[str]) -> None:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for line in handle:
            if not line.strip():
                continue
            if line.startswith("##") or (
                line.startswith("#") and not line.startswith(("#CHROM", "#Chrom", "#chr"))
            ):
                continue
            header = line.rstrip("\n").split("\t")
            missing = [column for column in required_columns if column not in header]
            if missing:
                raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")
            return
    raise ValueError(f"{path} does not contain a readable header")


def _ensure_vcf_info_field(path: Path, info_field: str) -> None:
    info_pattern = f"##INFO=<ID={info_field},"
    saw_header = False
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        for line in handle:
            if line.startswith(info_pattern):
                saw_header = True
            if line.startswith("#CHROM"):
                if not saw_header:
                    raise ValueError(f"{path} does not declare INFO field {info_field}")
                return
    raise ValueError(f"{path} does not contain a complete VCF header")


def _validate_alphamissense(path: Path) -> None:
    _ensure_gzip_header_columns(
        path,
        [
            "#CHROM",
            "POS",
            "REF",
            "ALT",
            "genome",
            "uniprot_id",
            "transcript_id",
            "protein_variant",
            "am_pathogenicity",
            "am_class",
        ],
    )


def _validate_cadd(path: Path) -> None:
    _ensure_gzip_header_columns(
        path,
        ["#Chrom", "Pos", "Ref", "Alt", "RawScore", "PHRED"],
    )


def _validate_dbnsfp(path: Path) -> None:
    _ensure_gzip_header_columns(
        path,
        [
            "#chr",
            "pos(1-based)",
            "ref",
            "alt",
            "SIFT4G_score",
            "SIFT4G_pred",
            "Polyphen2_HDIV_score",
            "Polyphen2_HVAR_score",
            "LRT_score",
            "LRT_pred",
            "MutationTaster_score",
            "REVEL_score",
            "MetaSVM_score",
            "MetaSVM_pred",
            "MetaLR_score",
            "MetaLR_pred",
            "GERP++_RS",
            "phyloP100way_vertebrate",
            "phyloP30way_mammalian",
            "phastCons100way_vertebrate",
            "phastCons30way_mammalian",
            "SiPhy_29way_logOdds",
            "CADD_raw",
            "CADD_phred",
            "FATHMM_score",
            "FATHMM_pred",
            "PROVEAN_score",
            "PROVEAN_pred",
            "VEST4_score",
            "BayesDel_addAF_score",
            "BayesDel_noAF_score",
            "MutationTaster_pred",
        ],
    )


def _validate_spliceai(path: Path) -> None:
    _ensure_vcf_info_field(path, "SpliceAI")


def _validate_clinvar(path: Path) -> None:
    _ensure_vcf_info_field(path, "CLNSIG")


def _tsv_chrom_filter(
    input_path: Path,
    output_path: Path,
    _assembly: str,
    _version: str,
    chromosomes: list[str],
) -> None:
    allowed = set(chromosomes)
    header_index: int | None = None
    with (
        gzip.open(input_path, "rt", encoding="utf-8", newline="") as in_handle,
        gzip.open(output_path, "wt", encoding="utf-8", newline="") as out_handle,
    ):
        for line in in_handle:
            if not line:
                continue
            if line.startswith("##") or (
                line.startswith("#") and not line.startswith(("#CHROM", "#Chrom", "#chr"))
            ):
                out_handle.write(line)
                continue

            parts = line.rstrip("\n").split("\t")
            if header_index is None:
                out_handle.write(line)
                for candidate in ("#CHROM", "#Chrom", "#chr", "chrom", "Chrom"):
                    if candidate in parts:
                        header_index = parts.index(candidate)
                        break
                if header_index is None:
                    raise ValueError(
                        f"{input_path} does not contain a recognizable chromosome column"
                    )
                continue

            chrom = parts[header_index]
            normalized = chrom[3:] if chrom.lower().startswith("chr") else chrom
            normalized = (
                normalized.upper() if normalized.upper() in {"X", "Y", "M", "MT"} else normalized
            )
            if normalized == "MT":
                normalized = "M"
            if normalized in allowed:
                out_handle.write(line)


def _prepare_dbnsfp(
    archive_path: Path,
    output_path: Path,
    _assembly: str,
    version: str,
    chromosomes: list[str] | None,
) -> None:
    allowed = set(chromosomes) if chromosomes else None
    member_pattern = re.compile(rf"dbNSFP{re.escape(version)}_variant\.chr(?:[0-9]+|X|Y|M)\.gz$")
    desired_order = {str(i): i for i in range(1, 23)}
    desired_order.update({"X": 23, "Y": 24, "M": 25})

    def member_key(name: str) -> tuple[int, str]:
        match = re.search(r"\.chr([0-9]+|X|Y|M)\.gz$", name)
        chrom = match.group(1) if match else "ZZZ"
        return (desired_order.get(chrom, 99), chrom)

    def include_member(name: str) -> bool:
        if not member_pattern.search(name):
            return False
        if allowed is None:
            return True
        match = re.search(r"\.chr([0-9]+|X|Y|M)\.gz$", name)
        chrom = match.group(1) if match else ""
        return chrom in allowed

    with zipfile.ZipFile(archive_path) as archive:
        members = sorted(
            (name for name in archive.namelist() if include_member(name)), key=member_key
        )
        if not members:
            raise ValueError(
                f"{archive_path} does not contain dbNSFP members for the requested chromosomes"
            )

        with gzip.open(output_path, "wt", encoding="utf-8", newline="") as out_handle:
            header_written = False
            for member in members:
                with archive.open(member, "r") as raw_member:
                    with gzip.open(raw_member, "rt", encoding="utf-8", newline="") as member_handle:
                        first_line = True
                        for line in member_handle:
                            if first_line:
                                if not header_written:
                                    out_handle.write(line)
                                    header_written = True
                                first_line = False
                                continue
                            out_handle.write(line)

    _validate_dbnsfp(output_path)


def _run_tabix_region_slice(source: str, output_path: Path, chromosomes: list[str]) -> None:
    requested_regions = [alias for chrom in chromosomes for alias in _tabix_region_aliases(chrom)]
    tabix_proc = subprocess.Popen(
        [_TABIX_EXECUTABLE, "-h", source, *requested_regions],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert tabix_proc.stdout is not None
    with open(output_path, "wb") as handle:
        bgzip_proc = subprocess.Popen(
            [_BGZIP_EXECUTABLE, "-c"],
            stdin=tabix_proc.stdout,
            stdout=handle,
            stderr=subprocess.PIPE,
        )
        tabix_proc.stdout.close()
        _, bgzip_err = bgzip_proc.communicate()
    tabix_err = tabix_proc.communicate()[1]
    if tabix_proc.returncode != 0:
        raise RuntimeError(
            f"tabix region extraction failed for {source}: {tabix_err.decode().strip()}"
        )
    if bgzip_proc.returncode != 0:
        raise RuntimeError(f"bgzip compression failed for {source}: {bgzip_err.decode().strip()}")


def _download_to_path(url: str, dest: Path, *, force: bool = False) -> None:
    tmp_dest = dest.with_name(dest.name + ".tmp")
    if tmp_dest.exists():
        tmp_dest.unlink()
    if dest.exists():
        if not force:
            return
        dest.unlink()
    try:
        _download_with_progress(url, tmp_dest)
        tmp_dest.replace(dest)
    finally:
        if tmp_dest.exists():
            tmp_dest.unlink()


def _raw_download_path(
    source_dir: Path,
    spec: PluginSourceSpec,
    assembly: str,
    version: str,
    chromosomes: list[str] | None,
    final_path: Path,
) -> Path:
    raw_name = spec.download_filename(assembly, version)
    raw_path = source_dir / raw_name
    if chromosomes and spec.fetch_mode == "download" and spec.filter_downloaded is not None:
        if raw_path == final_path:
            raw_path = source_dir / f"{raw_name}.download"
    return raw_path


def _alphamissense_filename(assembly: str, _version: str) -> str:
    return "AlphaMissense_hg38.tsv.gz" if assembly == "GRCh38" else "AlphaMissense_hg19.tsv.gz"


def _alphamissense_url(assembly: str, _version: str) -> str:
    return f"https://storage.googleapis.com/dm_alphamissense/{_alphamissense_filename(assembly, _version)}"


def _cadd_download_filename(_assembly: str, _version: str) -> str:
    return "whole_genome_SNVs.tsv.gz"


def _cadd_url(assembly: str, version: str) -> str:
    return f"https://krishna.gs.washington.edu/download/CADD/{version}/{assembly}/whole_genome_SNVs.tsv.gz"


def _cadd_indel_download_filename(_assembly: str, _version: str) -> str:
    return "gnomad.genomes.r4.0.indel.tsv.gz"


def _cadd_indel_url(assembly: str, version: str) -> str:
    return (
        f"https://krishna.gs.washington.edu/download/CADD/{version}/{assembly}/"
        "gnomad.genomes.r4.0.indel.tsv.gz"
    )


def _spliceai_filename(assembly: str, version: str) -> str:
    return f"spliceai_scores.masked.snv.ensembl_mane.{assembly.lower()}.{version}.vcf.gz"


def _spliceai_url(assembly: str, version: str) -> str:
    filename = _spliceai_filename(assembly, version)
    return f"https://ftp.ensembl.org/pub/data_files/homo_sapiens/{assembly}/variation_plugins/{filename}"


def _dbnsfp_download_filename(_assembly: str, version: str) -> str:
    return f"dbNSFP{version}.zip"


def _dbnsfp_output_filename(assembly: str, version: str) -> str:
    return f"dbNSFP{version}_{assembly.lower()}.gz"


def _dbnsfp_url(_assembly: str, version: str) -> str:
    return f"https://dbnsfp.s3.amazonaws.com/dbNSFP{version}.zip"


def _clinvar_filename(_assembly: str, _version: str) -> str:
    return "clinvar.vcf.gz"


def _clinvar_url(assembly: str, _version: str) -> str:
    return f"https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_{assembly}/clinvar.vcf.gz"


PLUGIN_SOURCE_SPECS: dict[str, PluginSourceSpec] = {
    "alphamissense": PluginSourceSpec(
        name="alphamissense",
        source_kind="tsv_gz",
        fetch_mode="download",
        supported_assemblies=("GRCh37", "GRCh38"),
        default_version="latest",
        download_filename=_alphamissense_filename,
        output_filename=_alphamissense_filename,
        download_url=_alphamissense_url,
        validate=_validate_alphamissense,
        filter_downloaded=_tsv_chrom_filter,
    ),
    "cadd": PluginSourceSpec(
        name="cadd",
        source_kind="tsv_gz",
        fetch_mode="download",
        supported_assemblies=("GRCh37", "GRCh38"),
        default_version="v1.7",
        download_filename=_cadd_download_filename,
        output_filename=_cadd_download_filename,
        download_url=_cadd_url,
        validate=_validate_cadd,
        filter_downloaded=_tsv_chrom_filter,
    ),
    "cadd_snv": PluginSourceSpec(
        name="cadd_snv",
        source_kind="tsv_gz",
        fetch_mode="download",
        supported_assemblies=("GRCh37", "GRCh38"),
        default_version="v1.7",
        download_filename=_cadd_download_filename,
        output_filename=_cadd_download_filename,
        download_url=_cadd_url,
        validate=_validate_cadd,
        filter_downloaded=_tsv_chrom_filter,
    ),
    "cadd_indel": PluginSourceSpec(
        name="cadd_indel",
        source_kind="tsv_gz",
        fetch_mode="download",
        supported_assemblies=("GRCh37", "GRCh38"),
        default_version="v1.7",
        download_filename=_cadd_indel_download_filename,
        output_filename=_cadd_indel_download_filename,
        download_url=_cadd_indel_url,
        validate=_validate_cadd,
        filter_downloaded=_tsv_chrom_filter,
    ),
    "spliceai": PluginSourceSpec(
        name="spliceai",
        source_kind="vcf_gz",
        fetch_mode="tabix_region",
        supported_assemblies=("GRCh38",),
        default_version="110",
        download_filename=_spliceai_filename,
        output_filename=_spliceai_filename,
        download_url=_spliceai_url,
        validate=_validate_spliceai,
    ),
    "dbnsfp": PluginSourceSpec(
        name="dbnsfp",
        source_kind="tsv_gz",
        fetch_mode="zip_members",
        supported_assemblies=("GRCh38",),
        default_version="4.9c",
        download_filename=_dbnsfp_download_filename,
        output_filename=_dbnsfp_output_filename,
        download_url=_dbnsfp_url,
        validate=_validate_dbnsfp,
        prepare_download=_prepare_dbnsfp,
    ),
    "clinvar": PluginSourceSpec(
        name="clinvar",
        source_kind="vcf_gz",
        fetch_mode="tabix_region",
        supported_assemblies=("GRCh37", "GRCh38"),
        default_version="latest",
        download_filename=_clinvar_filename,
        output_filename=_clinvar_filename,
        download_url=_clinvar_url,
        validate=_validate_clinvar,
    ),
}


def fetch_plugin_source(
    plugin_name: str,
    cache_dir: str,
    *,
    assembly: str = "GRCh38",
    release: int | None = None,
    version: str | None = None,
    chromosomes: list[str] | None = None,
    force: bool = False,
    keep_archive: bool = False,
) -> str:
    """Download, slice if possible, and validate a plugin source file suitable for ``build_plugin()``."""
    normalized_name = _normalize_plugin_name(plugin_name)
    spec = PLUGIN_SOURCE_SPECS.get(normalized_name)
    if spec is None:
        raise ValueError(
            f"Unknown plugin '{plugin_name}'. Supported: {', '.join(sorted(PLUGIN_SOURCE_SPECS))}"
        )

    normalized_assembly = _normalize_assembly(assembly)
    if normalized_assembly not in spec.supported_assemblies:
        raise ValueError(
            f"Plugin '{normalized_name}' does not support assembly {normalized_assembly}. "
            f"Supported: {', '.join(spec.supported_assemblies)}"
        )

    resolved_chromosomes = _normalize_requested_chromosomes(chromosomes)
    resolved_version = version
    if resolved_version is None:
        resolved_version = (
            spec.release_to_version.get(release, spec.default_version)
            if spec.release_to_version
            else spec.default_version
        )

    source_dir = (
        Path(cache_dir)
        / "plugin_sources"
        / normalized_name
        / normalized_assembly
        / resolved_version
        / _chromosome_scope_label(resolved_chromosomes)
    )
    source_dir.mkdir(parents=True, exist_ok=True)

    final_path = source_dir / spec.output_filename(normalized_assembly, resolved_version)
    metadata_path = source_dir / "source.json"
    source_url = spec.download_url(normalized_assembly, resolved_version)

    if final_path.exists() and not force:
        spec.validate(final_path)
        if not metadata_path.exists():
            _write_source_metadata(
                metadata_path,
                plugin_name=normalized_name,
                assembly=normalized_assembly,
                resolved_version=resolved_version,
                source_url=source_url,
                source_path=final_path,
                chromosomes=resolved_chromosomes,
                fetch_mode=spec.fetch_mode,
            )
        log.info("Reusing existing plugin source: %s", final_path)
        return str(final_path)

    if final_path.exists():
        final_path.unlink()

    download_path = _raw_download_path(
        source_dir,
        spec,
        normalized_assembly,
        resolved_version,
        resolved_chromosomes,
        final_path,
    )
    tmp_output_path = final_path.with_name(final_path.name + ".tmp")
    if tmp_output_path.exists():
        tmp_output_path.unlink()

    if spec.fetch_mode == "tabix_region" and resolved_chromosomes:
        index_path = download_path.with_name(download_path.name + ".tbi")
        try:
            _download_to_path(source_url, download_path, force=force)
            _download_to_path(f"{source_url}.tbi", index_path, force=force)
            _run_tabix_region_slice(str(download_path), tmp_output_path, resolved_chromosomes)
            tmp_output_path.replace(final_path)
        finally:
            if tmp_output_path.exists():
                tmp_output_path.unlink()
    else:
        tmp_download_path = download_path.with_name(download_path.name + ".tmp")
        if tmp_download_path.exists():
            tmp_download_path.unlink()

        if download_path.exists() and force:
            download_path.unlink()

        if not download_path.exists():
            try:
                _download_with_progress(source_url, tmp_download_path)
                tmp_download_path.replace(download_path)
            finally:
                if tmp_download_path.exists():
                    tmp_download_path.unlink()

        if spec.prepare_download is not None:
            try:
                spec.prepare_download(
                    download_path,
                    tmp_output_path,
                    normalized_assembly,
                    resolved_version,
                    resolved_chromosomes,
                )
                tmp_output_path.replace(final_path)
            finally:
                if tmp_output_path.exists():
                    tmp_output_path.unlink()
            if not keep_archive and download_path.exists():
                download_path.unlink()
        elif spec.filter_downloaded is not None and resolved_chromosomes:
            try:
                spec.filter_downloaded(
                    download_path,
                    tmp_output_path,
                    normalized_assembly,
                    resolved_version,
                    resolved_chromosomes,
                )
                tmp_output_path.replace(final_path)
            finally:
                if tmp_output_path.exists():
                    tmp_output_path.unlink()
        else:
            if download_path != final_path:
                download_path.replace(final_path)

    spec.validate(final_path)
    _write_source_metadata(
        metadata_path,
        plugin_name=normalized_name,
        assembly=normalized_assembly,
        resolved_version=resolved_version,
        source_url=source_url,
        source_path=final_path,
        chromosomes=resolved_chromosomes,
        fetch_mode=spec.fetch_mode,
    )
    log.info("Plugin '%s': source ready at %s", normalized_name, final_path)
    return str(final_path)


def _write_source_metadata(
    metadata_path: Path,
    *,
    plugin_name: str,
    assembly: str,
    resolved_version: str,
    source_url: str,
    source_path: Path,
    chromosomes: list[str] | None,
    fetch_mode: str,
) -> None:
    payload = {
        "plugin_name": plugin_name,
        "assembly": assembly,
        "resolved_version": resolved_version,
        "source_url": source_url,
        "downloaded_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "filename": source_path.name,
        "sha256": _hash_file(source_path),
        "chromosomes": chromosomes,
        "fetch_mode": fetch_mode,
    }
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
