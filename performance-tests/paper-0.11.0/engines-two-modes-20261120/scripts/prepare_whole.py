"""Assemble whole-genome caches and inputs for the single-invocation mode from the per-chromosome ones.

Usage: prepare_whole.py <merged|ensembl>
For one cache flavour, under <R>/whole/<cache>/:
  cache-veprs/       vep-rs JSON cache: every chromosome directory of the per-chromosome caches (real directories,
                     files hard-linked; symlinked variation files re-linked), info.json with all contigs
  cache-variation/   variation cache for fastVEP: the per-chromosome directories, symlinks as in the originals
  fastvep-models.jsonl.gz  the per-chromosome transcript models concatenated (multi-member gzip)
The fastVEP transcript cache is written from the concatenated models by the cache writer (run separately).
Nothing is converted or filtered here: the whole-genome caches hold exactly the transcripts, regulatory features
and variation files of the 25 per-chromosome caches.
"""

import json
import os
import shutil
import sys
from pathlib import Path

R = Path("/Users/mwiewior/research/benchmarks/engines-two-modes-20261120")
M = Path(
    "/Users/mwiewior/research/benchmarks/merged-engine-comparison-20261107/all-chromosomes-w8/caches"
)
W = Path(
    "/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/hg002-w8/caches"
)
XMT = Path(
    "/Users/mwiewior/research/benchmarks/ensembl-engine-comparison-20261108/clinvar-x-y-mt/caches"
)
CONTIGS = [str(c) for c in range(1, 23)] + ["X", "Y", "MT"]

cache = sys.argv[1]


def source(contig):
    if cache == "merged":
        return M / f"chr{contig}"
    return (W if (W / f"chr{contig}").is_dir() else XMT) / f"chr{contig}"


def place(src, dst):
    """Hard-link a regular file; re-create a symlink with its resolved target."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        return
    if src.is_symlink():
        dst.symlink_to(src.resolve())
    else:
        os.link(src, dst)


out = R / "whole" / cache
veprs, variation = out / "cache-veprs", out / "cache-variation"
counts = {}
for contig in CONTIGS:
    root = source(contig)
    n = 0
    for kind in ("transcripts", "regulatory", "variations"):
        d = root / "cache-veprs" / kind / contig
        if not d.is_dir():
            continue
        (veprs / kind / contig).mkdir(parents=True, exist_ok=True)
        for p in sorted(d.iterdir()):
            place(p, veprs / kind / contig / p.name)
            n += 1
    for p in sorted((root / "cache-variation" / contig).iterdir()):
        place(p, variation / contig / p.name)
    counts[contig] = n
first = source("1")
info = json.loads((first / "cache-veprs" / "info.json").read_text())
info["contig_order"] = CONTIGS
(veprs / "info.json").write_text(json.dumps(info, indent=2) + "\n")
place(first / "cache-veprs" / "chr_synonyms.txt", veprs / "chr_synonyms.txt")
for name in ("info.txt", "chr_synonyms.txt"):
    place(first / "cache-variation" / name, variation / name)
models = out / "fastvep-models.jsonl.gz"
if not models.exists():
    with open(str(models) + ".tmp", "wb") as dst:
        for contig in CONTIGS:
            with open(
                source(contig) / f"fastvep-models-{contig}.jsonl.gz", "rb"
            ) as src:
                shutil.copyfileobj(src, dst, 1 << 24)
    os.rename(str(models) + ".tmp", models)
alignment = {
    c: json.loads((source(c) / "catalogue-alignment.json").read_text())[
        "matched_transcripts"
    ]
    for c in CONTIGS
}
json.dump(
    {
        "cache": cache,
        "veprs_files": counts,
        "matched_transcripts": alignment,
        "matched_transcripts_total": sum(alignment.values()),
        "models_bytes": models.stat().st_size,
    },
    open(out / "prepared.json", "w"),
    indent=1,
)
print(
    cache,
    "vep-rs files",
    sum(counts.values()),
    "transcripts",
    sum(alignment.values()),
    "models bytes",
    models.stat().st_size,
)
