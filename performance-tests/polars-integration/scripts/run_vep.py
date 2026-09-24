"""Time Ensembl VEP 116 on chr22 at --fork 0/1/3/7, core (Ensembl cache) and plugins (merged + 5 plugins).

Core flags match PR #117's macOS runs. Plugin flags match
e2e-testing/scripts/build_vep_plugin_reference.sh, whose --plugin order fixes the CSQ layout.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pibench.measure import environment, repeat, write_json  # noqa: E402
from pibench.paths import (
    DATA,
    FASTA,
    INPUT_VCF,
    PKG_ROOT,
    PLUGIN_CODE,
    PLUGIN_SLICES,
    VEP_CACHE,
    VEP_IMAGE,
    WORK,
)  # noqa: E402

DBNSFP_COLS = (
    "SIFT4G_score,SIFT4G_pred,Polyphen2_HDIV_score,Polyphen2_HVAR_score,"
    "MutationTaster_score,MutationTaster_pred,PROVEAN_score,PROVEAN_pred,VEST4_score,"
    "MetaSVM_score,MetaSVM_pred,MetaLR_score,MetaLR_pred,REVEL_score,GERP++_RS,"
    "phyloP100way_vertebrate,phastCons100way_vertebrate,CADD_raw,CADD_phred"
)


def in_data(p: Path) -> str:
    return "/data/" + str(p.resolve().relative_to(DATA.resolve()))


def vep_command(mode: str, fork: int, out: Path) -> list[str]:
    docker = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--env",
        "HOME=/tmp",
        "-v",
        f"{DATA}:/data",
    ]
    common = [
        "--offline",
        "--vcf",
        "--no_stats",
        "--force_overwrite",
        "--everything",
        "--fasta",
        in_data(FASTA),
        "--input_file",
        in_data(INPUT_VCF),
        "--output_file",
        in_data(out),
    ]
    fork_args = ["--fork", str(fork)] if fork else []
    if mode == "core":
        cache = VEP_CACHE["ensembl"]
        return [
            *docker,
            "-v",
            f"{cache}:/opt/vep/.vep/homo_sapiens/116_GRCh38:ro",
            VEP_IMAGE,
            "vep",
            "--dir",
            "/opt/vep/.vep",
            "--cache",
            "--assembly",
            "GRCh38",
            "--hgvs",
            *common,
            *fork_args,
        ]
    s = in_data(PLUGIN_SLICES)
    return [
        *docker,
        "-v",
        f"{PLUGIN_CODE}:/plugins:ro",
        VEP_IMAGE,
        "vep",
        "--cache",
        "--cache_version",
        "116",
        "--dir_cache",
        "/data",
        "--merged",
        *common,
        "--dir_plugins",
        "/plugins",
        "--custom",
        f"{s}/clinvar_chr22.vcf.gz,ClinVar,vcf,exact,0,CLNSIG,CLNREVSTAT,CLNDN,CLNVC,CLNVI",
        "--plugin",
        f"SpliceAI,snv={s}/spliceai_chr22.vcf.gz,indel={s}/spliceai_chr22.vcf.gz",
        "--plugin",
        f"CADD,snv={s}/cadd_snv_chr22.tsv.gz,indels={s}/cadd_indel_chr22.tsv.gz",
        "--plugin",
        f"AlphaMissense,file={s}/alphamissense_chr22.tsv.gz",
        "--plugin",
        f"dbNSFP,{s}/dbNSFP5.3.1a_grch38_chr22.gz,{DBNSFP_COLS}",
        *fork_args,
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--run-dir",
        required=True,
        type=Path,
        help="outputs/116/<name> under the package",
    )
    ap.add_argument("--modes", nargs="+", default=["core", "plugins"])
    ap.add_argument("--forks", nargs="+", type=int, default=[0, 1, 3, 7])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmups", type=int, default=1)
    a = ap.parse_args()
    run_dir = PKG_ROOT / a.run_dir
    write_json(run_dir / "env.json", environment())
    for mode in a.modes:
        for fork in a.forks:
            out_dir = WORK / "vep" / mode / f"fork{fork}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out = (
                out_dir / "rep0.vcf"
            )  # every repeat overwrites it; the last run's file is kept
            res = repeat(
                vep_command(mode, fork, out),
                run_dir / "logs" / "vep" / f"{mode}_fork{fork}",
                warmups=a.warmups,
                repeats=a.repeats,
            )
            res.update(mode=mode, fork=fork, processes=fork + 1, output=str(out))
            write_json(run_dir / "vep" / f"{mode}_fork{fork}.json", res)
            print(f"{mode} fork{fork}: median {res['median_wall_s']:.1f}s", flush=True)


if __name__ == "__main__":
    main()
