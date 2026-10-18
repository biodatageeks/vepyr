"""Regenerate the chrMT codon-table fixture (vepyr#200) from local data.

Inputs, all under $DATA_VEPYR_DIR (default ~/workspace/data_vepyr):
  homo_sapiens_merged/116_GRCh38/{info.txt,MT/}   raw Ensembl VEP 116 merged cache
  cache/116_GRCh38_merged/                         published converted cache
                                                   (biodatageeks/vepyr_116_GRCh38_merged@5b83dd8)
  input/Homo_sapiens.GRCh38.dna.primary_assembly.fa

and the ClinVar 20261011 chrMT codon_table fixture (input.vcf, vep.vcf) from
e2e-testing/golden/116/clinvar/mismatches/chrMT/codon_table/ on branch
analysis/clinvar-chrmt-mismatches, passed as the first argument.

Writes into this directory:
  raw/116_GRCh38_merged/  raw MT cache directory, rebuilt by the test with build_cache()
  published/      the published chrMT shards, codon_table null on every row
  reference.fa.gz MT only (bgzip, .fai, .gzi)
  input.vcf (+ .gz/.tbi), vep116.golden.vcf
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = Path(os.environ.get("DATA_VEPYR_DIR", Path.home() / "workspace/data_vepyr"))
RAW = DATA / "homo_sapiens_merged/116_GRCh38"
PUBLISHED = DATA / "cache/116_GRCh38_merged"
FASTA = DATA / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
ENTITIES = [
    "exon",
    "motif",
    "regulatory",
    "transcript",
    "translation_core",
    "translation_sift",
    "variation",
]


def main(fixture_dir):
    fixture_dir = Path(fixture_dir)

    shutil.rmtree(HERE / "raw", ignore_errors=True)
    # build_cache() derives the release from the canonical directory name.
    raw_out = HERE / "raw" / "116_GRCh38_merged"
    (raw_out / "MT").mkdir(parents=True)
    shutil.copy2(RAW / "info.txt", raw_out / "info.txt")
    for name in ["1-1000000.gz", "1-1000000_reg.gz", "all_vars.gz", "all_vars.gz.csi"]:
        shutil.copy2(RAW / "MT" / name, raw_out / "MT" / name)

    pub_out = HERE / "published"
    shutil.rmtree(pub_out, ignore_errors=True)
    pub_out.mkdir()
    shutil.copy2(PUBLISHED / "reference_policy.json", pub_out / "reference_policy.json")
    for entity in ENTITIES:
        (pub_out / entity).mkdir()
        entries = [
            entry
            for entry in json.loads(
                (PUBLISHED / entity / "chrom_manifest.json").read_text()
            )
            if entry["chrom"] == "chrMT"
        ]
        for entry in entries:
            shutil.copy2(
                PUBLISHED / entity / entry["dataset"],
                pub_out / entity / entry["dataset"],
            )
        (pub_out / entity / "chrom_manifest.json").write_text(
            json.dumps(entries, indent=2) + "\n"
        )

    fasta = HERE / "reference.fa"
    with open(fasta, "w") as out:
        subprocess.run(["samtools", "faidx", str(FASTA), "MT"], stdout=out, check=True)
    subprocess.run(["bgzip", "-f", str(fasta)], check=True)
    subprocess.run(["samtools", "faidx", str(HERE / "reference.fa.gz")], check=True)

    shutil.copy2(fixture_dir / "input.vcf", HERE / "input.vcf")
    with open(HERE / "input.vcf.gz", "wb") as out:
        subprocess.run(["bgzip", "-c", str(HERE / "input.vcf")], stdout=out, check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", str(HERE / "input.vcf.gz")], check=True)
    shutil.copy2(fixture_dir / "vep.vcf", HERE / "vep116.golden.vcf")


if __name__ == "__main__":
    main(sys.argv[1])
