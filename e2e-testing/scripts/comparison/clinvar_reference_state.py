"""Pinned input checks and resumability helpers for the ClinVar VEP reference."""

import argparse
import hashlib
import re
import subprocess
from pathlib import Path


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def pinned_fasta_sha256(pins_path):
    pins = Path(pins_path).read_text(encoding="utf-8")
    section = re.search(r"(?ms)^\[grch38_fasta\]\s*\n(.*?)(?=^\[|\Z)", pins)
    if section is None:
        raise ValueError(f"no [grch38_fasta] section in {pins_path}")
    match = re.search(r'(?m)^\s*sha\s*=\s*"([0-9a-f]{64})"\s*$', section[1])
    if match is None:
        raise ValueError(f"no valid GRCh38 FASTA SHA-256 in {pins_path}")
    return match[1]


def validate_and_index_fasta(fasta_path, pins_path, indexer=None):
    """Validate the pinned release-116 sequence, then regenerate its FAI."""
    fasta = Path(fasta_path)
    if not fasta.is_file():
        raise ValueError(f"missing release-116 FASTA at {fasta}")
    expected = pinned_fasta_sha256(pins_path)
    actual = _sha256(fasta)
    if actual != expected:
        raise ValueError(
            f"release-116 FASTA SHA-256 mismatch for {fasta}: "
            f"got {actual}, want {expected}"
        )

    Path(f"{fasta}.fai").unlink(missing_ok=True)
    if indexer is None:
        subprocess.run(["samtools", "faidx", str(fasta)], check=True)
    else:
        indexer(fasta)
    return actual


def rebuild_vcf_index(vcf_path, indexer=None):
    """Replace a possibly stale tabix index before using a normalized VCF."""
    if indexer is None:
        subprocess.run(["tabix", "-f", "-p", "vcf", str(vcf_path)], check=True)
    else:
        indexer(vcf_path)


def validate_cache_archive(path, expected_sum):
    """Check Ensembl's BSD sum and discard a bad archive so retries can restart."""
    result = subprocess.run(
        ["sum", str(path)], capture_output=True, text=True, encoding="utf-8", check=True
    )
    actual = " ".join(result.stdout.split()[:2])
    if actual != expected_sum:
        Path(path).unlink(missing_ok=True)
        raise ValueError(
            f"checksum mismatch for {path}: got {actual!r}, want {expected_sum!r}; "
            "the bad archive was removed"
        )


def validate_unique_contigs(contigs):
    """Reject duplicate contig work before the reference generator forks jobs."""
    seen = set()
    duplicates = []
    for contig in contigs:
        if contig in seen and contig not in duplicates:
            duplicates.append(contig)
        seen.add(contig)
    if duplicates:
        raise ValueError("duplicate contigs: " + ", ".join(duplicates))


def build_provenance(fasta_sha256, cache_identity, image, input_body_md5, vep_args):
    lines = [
        f"fasta_sha256={fasta_sha256}",
        f"cache_identity={cache_identity}",
        f"image={image}",
        f"input_body_md5={input_body_md5}",
    ]
    lines.extend(f"vep_arg={arg}" for arg in vep_args)
    return "\n".join(lines)


def contig_is_current(vcf_path, index_path, stamp_path, expected_provenance):
    """Return true only for a complete VCF/index pair with matching inputs."""
    vcf = Path(vcf_path)
    index = Path(index_path)
    stamp = Path(stamp_path)
    if not (
        vcf.is_file()
        and vcf.stat().st_size
        and index.is_file()
        and index.stat().st_size
    ):
        return False
    if not stamp.is_file():
        return False
    return stamp.read_text(encoding="utf-8").removesuffix("\n") == expected_provenance


def remove_incomplete_contig(vcf_path, index_path, stamp_path):
    """Remove a partial VCF/index publication, preserving complete stale pairs."""
    paths = [Path(vcf_path), Path(index_path), Path(stamp_path)]
    if not any(path.exists() for path in paths):
        return False
    complete_pair = all(path.is_file() and path.stat().st_size for path in paths[:2])
    if complete_pair:
        return False
    for path in paths:
        path.unlink(missing_ok=True)
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    sha256 = commands.add_parser("sha256")
    sha256.add_argument("path")
    fasta = commands.add_parser("validate-fasta")
    fasta.add_argument("fasta")
    fasta.add_argument("pins")
    index_vcf = commands.add_parser("index-vcf")
    index_vcf.add_argument("vcf")
    cache_archive = commands.add_parser("validate-cache-archive")
    cache_archive.add_argument("path")
    cache_archive.add_argument("expected_sum")
    unique_contigs = commands.add_parser("validate-contigs")
    unique_contigs.add_argument("contigs", nargs="+")
    provenance = commands.add_parser("provenance")
    provenance.add_argument("fasta_sha256")
    provenance.add_argument("cache_identity")
    provenance.add_argument("image")
    provenance.add_argument("input_body_md5")
    provenance.add_argument("--vep-arg", action="append", default=[])
    current = commands.add_parser("contig-current")
    current.add_argument("vcf")
    current.add_argument("index")
    current.add_argument("stamp")
    current.add_argument("expected_provenance")
    cleanup = commands.add_parser("remove-incomplete-contig")
    cleanup.add_argument("vcf")
    cleanup.add_argument("index")
    cleanup.add_argument("stamp")
    args = parser.parse_args(argv)

    try:
        if args.command == "sha256":
            print(_sha256(args.path))
        elif args.command == "validate-fasta":
            print(validate_and_index_fasta(args.fasta, args.pins))
        elif args.command == "index-vcf":
            rebuild_vcf_index(args.vcf)
        elif args.command == "validate-cache-archive":
            validate_cache_archive(args.path, args.expected_sum)
        elif args.command == "validate-contigs":
            validate_unique_contigs(args.contigs)
        elif args.command == "provenance":
            print(
                build_provenance(
                    args.fasta_sha256,
                    args.cache_identity,
                    args.image,
                    args.input_body_md5,
                    args.vep_arg,
                )
            )
        elif args.command == "contig-current":
            return (
                0
                if contig_is_current(
                    args.vcf, args.index, args.stamp, args.expected_provenance
                )
                else 1
            )
        elif args.command == "remove-incomplete-contig":
            return (
                0 if remove_incomplete_contig(args.vcf, args.index, args.stamp) else 1
            )
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
