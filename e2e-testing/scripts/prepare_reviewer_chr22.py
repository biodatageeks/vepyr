#!/usr/bin/env python3
"""Maintainer-only: package existing Ensembl VEP 116 outputs for the reviewer.

This does not generate truth with vepyr. Supply the original VEP WGS baselines
and the seven per-contig pick references from release 116. See e2e-testing/README.md.
"""

import argparse
import json
from pathlib import Path
import subprocess
import urllib.request

from comparison import vcfio
from comparison.profiles import PROFILES
from md5_concordance import digest_vcf
from reviewer_chr22 import CORE_PROFILES, GOLDEN, MANIFEST, ROOT, checksum


def file_metadata(path):
    return {"size": path.stat().st_size, "sha256": checksum(path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vep-dir", type=Path, required=True)
    parser.add_argument("--pick-dir", type=Path, required=True)
    args = parser.parse_args()
    # Refuse accidental replacement: new truth must be an explicit reviewable change.
    if MANIFEST.exists():
        parser.error(
            f"{MANIFEST} already exists; move the old fixture directory aside first"
        )
    GOLDEN.mkdir(parents=True, exist_ok=True)
    inputs = "tests/data/hg002_chr22"
    manifest = {
        "release": "116",
        "vep_version": "116.0",
        "chrom": "chr22",
        "input_vcf": f"{inputs}/input_chr22.vcf.gz",
        "fasta": f"{inputs}/chr22.fa.gz",
        "normalization": "bcftools norm -m -both (already applied to input_chr22.vcf.gz)",
        "inputs": {},
        "caches": {},
        "profiles": {},
    }
    for name in (
        "input_chr22.vcf.gz",
        "input_chr22.vcf.gz.tbi",
        "chr22.fa.gz",
        "chr22.fa.gz.fai",
        "chr22.fa.gz.gzi",
    ):
        path = ROOT / inputs / name
        manifest["inputs"][str(path.relative_to(ROOT))] = file_metadata(path)
    records = vcfio.count_data_lines(str(ROOT / manifest["input_vcf"]))
    for name in CORE_PROFILES:
        profile = PROFILES[name]
        if name in ("ensembl", "merged", "refseq"):
            source = args.vep_dir / f"{profile.vep_basename}.vcf.gz"
        else:
            source = args.pick_dir / f"HG002_chr22_{name}_vep116.vcf.gz"
        identity = vcfio.parse_vep_header(str(source))
        if identity["vep_version"] != "116.0":
            raise ValueError(f"Expected VEP 116.0: {source}")
        destination = GOLDEN / f"{name}.vcf.gz"
        partial = destination.with_suffix(".partial")
        with partial.open("wb") as output:
            query = subprocess.Popen(
                ["tabix", "-h", str(source), "chr22"], stdout=subprocess.PIPE
            )
            try:
                subprocess.run(
                    ["bgzip", "-c"], stdin=query.stdout, stdout=output, check=True
                )
            finally:
                query.stdout.close()
                status = query.wait()
            if status:
                raise RuntimeError(f"tabix failed for {source}")
        partial.replace(destination)
        digests = {
            mode: digest_vcf(str(destination), mode, profile.ignore_csq_order)
            for mode in ("strict", "canonical")
        }
        if not records or digests["strict"].records != records:
            raise ValueError(f"Reference record count disagrees with input: {source}")
        manifest["profiles"][name] = {
            "path": str(destination.relative_to(ROOT)),
            **file_metadata(destination),
            "source": source.name,
            "vep_identity": identity,
            "records": records,
            "ignore_csq_order": profile.ignore_csq_order,
            **{f"{mode}_body_md5": digest.body for mode, digest in digests.items()},
        }
        print(
            f"Packaged {name}: {records} records, {destination.stat().st_size / 1e6:.1f} MB",
            flush=True,
        )
    for flavour in ("ensembl", "merged", "refseq"):
        repo = f"biodatageeks/vepyr_116_GRCh38_{flavour}"
        with urllib.request.urlopen(
            f"https://huggingface.co/api/datasets/{repo}?blobs=true", timeout=60
        ) as response:
            info = json.load(response)
        files = {}
        for entry in info["siblings"]:
            path = entry["rfilename"]
            if not path.endswith(("/chr22.parquet", "/chrom_manifest.json")):
                continue
            digest = (
                {"sha256": entry["lfs"]["sha256"]}
                if "lfs" in entry
                else {"git_blob": entry["blobId"]}
            )
            files[path] = {"size": entry["size"], **digest}
        if len(files) != 14:
            raise ValueError(
                f"Expected seven chr22 shards and seven manifests in {repo}"
            )
        manifest["caches"][flavour] = {
            "repo_id": repo,
            "revision": info["sha"],
            "files": files,
        }
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
