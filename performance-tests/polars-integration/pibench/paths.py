"""Every path and constant the benchmark uses, overridable by environment."""

from __future__ import annotations

import os
from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parents[1]  # performance-tests/polars-integration
REPO_ROOT = PKG_ROOT.parents[1]
DATA = Path(os.environ.get("DATA_VEPYR_DIR", Path.home() / "workspace/data_vepyr"))
WORK = Path(os.environ.get("PIBENCH_WORK", DATA / "polars_integration/chr22"))
INPUT_VCF = WORK / "input/HG002_chr22.vcf.gz"
FASTA = WORK / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
VEPYR_CACHE = {
    "ensembl": DATA / "cache/116_GRCh38_ensembl",
    "merged": DATA / "cache/116_GRCh38_merged",
}
VEP_CACHE = {
    "ensembl": DATA / "homo_sapiens_ensembl/116_GRCh38",
    "merged": DATA / "homo_sapiens_merged/116_GRCh38",
}
PLUGIN_CACHE_ROOT = DATA / "plugin_cache_116"
PLUGIN_CODE = DATA / "output/116/plugins/plugin_code"
PLUGIN_SLICES = WORK / "vep_plugin_ref/slices"
PANELS = PKG_ROOT / "panels"
VEP_IMAGE = os.environ.get("VEP_IMAGE", "ensemblorg/ensembl-vep:release_116.0")
EXPECTED_RECORDS = 50_861
FORK_TO_WORKERS = {0: 1, 1: 2, 3: 4, 7: 8}
MAX_LOAD = float(os.environ.get("PIBENCH_MAX_LOAD", "2.0"))
