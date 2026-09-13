"""Build the vepyr/annotate test data for nf-core/test-datasets.

usage: uv run python nf-core-module/stage_testdata.py <output-dir>

Writes <output-dir>/data/genomics/homo_sapiens/vepyr/:

- input.vcf.gz (+ .tbi): 1,000 consecutive normalized HG002 chr22 records,
  chr22:20572272-21735973. Of the windows ending before 25 Mb, this one carries
  the most coding annotation (missense, HGVSp, SIFT/PolyPhen) plus motif and
  regulatory hits; the first 1,000 records are pericentromeric and have none.
- reference.fa.gz (+ .fai, .gzi): GRCh38 22:1-<last record + 10 kb>, bgzip
- cache.tar.gz: an Ensembl release-116 `ensembl` Parquet cache trimmed to the
  rows those records read, as one top-level `cache/` directory (UNTAR strips it)

Everything is cut from the offline HG002 chr22 parity fixture in
tests/data/hg002_chr22, whose cache is itself trimmed from the full 116 cache.
The subset is trimmed again with that fixture's rules (prepare.py). Nothing is
written unless annotating the subset -- against the fixture cache and then
against the trimmed one -- reproduces the record-body md5 Ensembl VEP 116
`--everything` produced for these 1,000 records.

Needs the project environment, Git LFS files pulled, and samtools, bgzip and
tabix on PATH.
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "data" / "hg002_chr22"
SKIP = 7150  # records before the window, in fixture (coordinate) order
RECORDS = 1000
FASTA_BUFFER = 10_000

# Strict body md5 of these records in Ensembl VEP 116 --everything output
# (output/116/HG002_annotated_wgs_everything_hgvs_vep.vcf.gz, chr22:20572272-21735973).
EXPECTED_BODY_MD5 = "1d4a92b815eb6193e6f4546f4ab35978"


def load_fixture_prepare():
    spec = importlib.util.spec_from_file_location(
        "hg002_chr22_prepare", FIXTURE / "prepare.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Trim from the fixture's cache, not the full workspace cache.
    module.CACHE_SRC = FIXTURE / "cache"
    return module


def run(cmd, **kwargs):
    return subprocess.run(cmd, check=True, **kwargs)


def subset_input(dst: Path) -> int:
    """Header plus RECORDS records after SKIP; returns the last record's position."""
    plain = dst.with_suffix("")
    last_pos = 0
    seen = 0
    kept = 0
    with (
        subprocess.Popen(
            ["bgzip", "-dc", str(FIXTURE / "input_chr22.vcf.gz")],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc,
        open(plain, "w") as out,
    ):
        for line in proc.stdout:
            if line.startswith("#"):
                out.write(line)
                continue
            seen += 1
            if seen <= SKIP:
                continue
            if kept == RECORDS:
                break
            out.write(line)
            last_pos = int(line.split("\t", 2)[1])
            kept += 1
        proc.stdout.close()
    if kept != RECORDS:
        raise SystemExit(f"fixture has only {kept} records")
    run(["bgzip", "-f", str(plain)])
    run(["tabix", "-f", "-p", "vcf", str(dst)])
    return last_pos


def subset_fasta(dst: Path, end: int) -> None:
    """22:1-end from the fixture FASTA, header renamed back to `22`, bgzip + indexes."""
    region = run(
        ["samtools", "faidx", str(FIXTURE / "chr22.fa.gz"), f"22:1-{end}"],
        capture_output=True,
    ).stdout
    region = region.replace(f">22:1-{end}".encode(), b">22", 1)
    with open(dst, "wb") as out:
        run(["bgzip", "-c", "-l", "9"], input=region, stdout=out)
    run(["samtools", "faidx", str(dst)])


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__.split("\n\n")[1])
    target = Path(sys.argv[1]) / "data" / "genomics" / "homo_sapiens" / "vepyr"
    prepare = load_fixture_prepare()

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        input_vcf = tmp / "input.vcf.gz"
        fasta = tmp / "reference.fa.gz"

        last_pos = subset_input(input_vcf)
        end = last_pos + FASTA_BUFFER
        subset_fasta(fasta, end)
        print(f"input: {RECORDS} records, chr22 up to {last_pos}; FASTA 22:1-{end}")

        full_vcf = tmp / "full.vcf.gz"
        prepare.annotate(input_vcf, prepare.CACHE_SRC, fasta, full_vcf)
        digest = prepare.body_digest(full_vcf)
        if digest != (EXPECTED_BODY_MD5, RECORDS):
            raise SystemExit(
                f"fixture cache does not reproduce VEP for the subset: {digest}"
            )

        cache = tmp / "stage" / "cache"
        print("trimming the cache:")
        prepare.trim_cache(full_vcf, input_vcf, cache)

        trimmed_vcf = tmp / "trimmed.vcf.gz"
        prepare.annotate(input_vcf, cache, fasta, trimmed_vcf)
        digest = prepare.body_digest(trimmed_vcf)
        if digest != (EXPECTED_BODY_MD5, RECORDS):
            raise SystemExit(f"trimmed cache does not reproduce VEP: {digest}")

        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        # One top-level cache/ entry, so UNTAR applies --strip-components 1.
        with tarfile.open(target / "cache.tar.gz", "w:gz") as tar:
            tar.add(cache, arcname="cache")
        for f in (
            input_vcf,
            Path(f"{input_vcf}.tbi"),
            fasta,
            Path(f"{fasta}.fai"),
            Path(f"{fasta}.gzi"),
        ):
            shutil.copy2(f, target / f.name)

    size = sum(f.stat().st_size for f in target.iterdir())
    print(
        f"\nbody md5 {EXPECTED_BODY_MD5} reproduced; staged {size / 1e6:.1f} MB to {target}"
    )


if __name__ == "__main__":
    main()
