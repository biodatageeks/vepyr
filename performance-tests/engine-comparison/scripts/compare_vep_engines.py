#!/usr/bin/env python3
"""Compare vepyr, vep-rs and fastVEP against Ensembl VEP on one chromosome.

The script builds vep-rs and fastVEP from source, prepares each engine's cache
from the same Ensembl VEP cache vepyr's Parquet cache was converted from,
times every engine at each thread count on the Ensembl and merged caches, and
compares every CSQ column of every engine's output with an Ensembl VEP
``--everything`` reference, cell by cell.

Steps (each skips work whose outputs already exist; ``--force`` redoes it):

    build    clone and build vep-rs and fastVEP, install vepyr from PyPI
    caches   convert the Ensembl cache for vep-rs, build fastVEP's caches
    run      time every engine, cache, mode and thread count
    compare  score each engine's output against the Ensembl VEP reference
    report   write results.json and report.md
    all      every step in order

See ../README.md for the inputs, the method and how to read the report.
"""

from __future__ import annotations

import argparse
import filecmp
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

VEP_RS_REPO = "https://github.com/natera-open-source/vep-rs.git"
FASTVEP_REPO = "https://github.com/Huang-lab/fastVEP.git"
ENSEMBL_GFF3_URL = (
    "https://ftp.ensembl.org/pub/release-{release}/gff3/homo_sapiens/"
    "Homo_sapiens.{assembly}.{release}.chromosome.{chrom}.gff3.gz"
)
NCBI_ANNOTATION_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/all/annotation_releases/9606/{release}/{file}.gz"
CACHES = ("ensembl", "merged")

# Engine modes. vep-rs runs on two conversions of the same Storable cache, with
# and without the SIFT/PolyPhen matrices; fastVEP looks co-located variants up
# either through the Perl cache's tabix file or through an .osa2 database.
MODES = {
    "vepyr": ("default",),
    "vep-rs": ("sift", "nosift"),
    "fastvep": ("osa2", "tabix"),
}
# The mode whose CSQ output is scored column by column.
SCORED_MODE = {"vepyr": "default", "vep-rs": "sift", "fastvep": "tabix"}
LABEL = {"vepyr": "vepyr", "vep-rs": "vep-rs", "fastvep": "fastVEP"}

RSS_BYTES_PER_UNIT = 1 if sys.platform == "darwin" else 1024


# --------------------------------------------------------------------------
# Small helpers


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd, cwd=None, env=None, stdout=None, check=True):
    log("$ " + " ".join(str(c) for c in cmd))
    return subprocess.run(
        [str(c) for c in cmd], cwd=cwd, env=env, stdout=stdout, check=check
    )


def capture(cmd, cwd=None) -> str:
    return subprocess.run(
        [str(c) for c in cmd], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def require_tools(*names: str) -> None:
    missing = [n for n in names if shutil.which(n) is None]
    if missing:
        sys.exit(f"missing required tools on PATH: {', '.join(missing)}")


def dir_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(
        p.stat().st_size for p in path.rglob("*") if p.is_file() and not p.is_symlink()
    )


def mb(n: int | float) -> float:
    return round(n / 1048576, 1)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def download(url: str, dest: Path) -> Path:
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    log(f"download {url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        shutil.copyfileobj(resp, out)
    tmp.rename(dest)
    return dest


def read_info(cache_dir: Path) -> dict:
    info = {}
    for line in open(cache_dir / "info.txt"):
        key, _, value = line.rstrip("\n").partition("\t")
        if key:
            info[key] = value
    return info


def body_md5(vcf: Path) -> str:
    h = hashlib.md5()
    with open(vcf, "rb") as fh:
        for line in fh:
            if not line.startswith(b"#"):
                h.update(line)
    return h.hexdigest()


# --------------------------------------------------------------------------
# Configuration


class Config:
    def __init__(self, a: argparse.Namespace):
        self.work = a.work_dir.resolve()
        self.chrom = a.chrom
        self.input_vcf = a.input_vcf.resolve()
        self.fasta = a.fasta.resolve()
        self.vep_cache = {
            "ensembl": a.vep_cache_ensembl.resolve(),
            "merged": a.vep_cache_merged.resolve(),
        }
        self.vepyr_cache = {
            "ensembl": a.vepyr_cache_ensembl.resolve(),
            "merged": a.vepyr_cache_merged.resolve(),
        }
        self.reference = {
            "ensembl": a.reference_ensembl.resolve(),
            "merged": a.reference_merged.resolve(),
        }
        self.vepyr_version = a.vepyr_version
        self.vepyr_bin = a.vepyr_bin.resolve() if a.vepyr_bin else None
        self.vep_rs_repo, self.vep_rs_ref = a.vep_rs_repo, a.vep_rs_ref
        self.fastvep_repo, self.fastvep_ref = a.fastvep_repo, a.fastvep_ref
        self.refseq_gff_url = a.refseq_gff_url
        self.threads = [int(t) for t in a.threads.split(",")]
        self.repeats = a.repeats
        self.tabix_repeats = a.fastvep_tabix_repeats
        self.warmup = not a.no_warmup
        self.tools = a.tools.split(",")
        self.force = a.force
        info = read_info(self.vep_cache["ensembl"])
        self.assembly = info.get("assembly", "GRCh38")
        self.release = a.release or int(self.vep_cache["ensembl"].name.split("_")[0])

    # Locations inside the work directory
    def src(self, name):
        return self.work / "src" / name

    @property
    def vep_rs_bin(self):
        return self.src("vep-rs") / "target" / "release" / "vep"

    @property
    def fastvep_bin(self):
        return self.src("fastVEP") / "target" / "release" / "fastvep"

    @property
    def vepyr(self):
        return (
            self.vepyr_bin
            or self.work / f"venv-vepyr-{self.vepyr_version}" / "bin" / "vepyr"
        )

    def vep_rs_cache(self, cache, mode):
        return self.work / "caches" / "vep-rs" / cache / mode

    def fastvep_cache_bin(self, cache):
        return self.work / "caches" / "fastvep" / f"{cache}.bin"

    def fastvep_vardir(self, cache):
        return self.work / "caches" / "fastvep" / f"vardir-{cache}"

    def fastvep_osa2_dir(self, cache):
        return self.work / "caches" / "fastvep" / f"osa2-{cache}"

    def run_dir(self, tool, cache, mode):
        return self.work / "runs" / f"{tool}-{cache}-{mode}"

    def output_vcf(self, tool, cache, mode, threads):
        return self.run_dir(tool, cache, mode) / f"t{threads}.vcf"

    def reference_slice(self, cache):
        return self.work / "reference" / f"{cache}.chr{self.chrom}.vcf"

    @property
    def results(self):
        return self.work / "results"


# --------------------------------------------------------------------------
# build


def clone(url: str, ref: str, dest: Path) -> str:
    if not dest.exists():
        run(["git", "clone", "--quiet", url, dest])
    run(["git", "fetch", "--quiet", "origin", "--tags"], cwd=dest)
    target = ref
    if capture(["git", "ls-remote", "origin", f"refs/heads/{ref}"], cwd=dest):
        target = f"origin/{ref}"
    run(["git", "checkout", "--quiet", "--detach", target], cwd=dest)
    return capture(["git", "rev-parse", "HEAD"], cwd=dest)


def step_build(cfg: Config) -> None:
    require_tools("git", "cargo")
    commits = {}
    if "vep-rs" in cfg.tools:
        commits["vep-rs"] = clone(cfg.vep_rs_repo, cfg.vep_rs_ref, cfg.src("vep-rs"))
        run(["cargo", "build", "--release", "-p", "vep-cli"], cwd=cfg.src("vep-rs"))
    if "fastvep" in cfg.tools:
        commits["fastvep"] = clone(
            cfg.fastvep_repo, cfg.fastvep_ref, cfg.src("fastVEP")
        )
        run(
            ["cargo", "build", "--release", "-p", "fastvep-cli"], cwd=cfg.src("fastVEP")
        )
    if "vepyr" in cfg.tools and cfg.vepyr_bin is None and not cfg.vepyr.exists():
        venv = cfg.vepyr.parent.parent
        run([sys.executable, "-m", "venv", venv])
        run([venv / "bin" / "pip", "install", "--quiet", f"vepyr=={cfg.vepyr_version}"])
    cfg.work.mkdir(parents=True, exist_ok=True)
    (cfg.work / "commits.json").write_text(json.dumps(commits, indent=1))


# --------------------------------------------------------------------------
# caches: vep-rs


def vep_rs_reads_gzipped_variations(cfg: Config) -> bool:
    """True when this vep-rs build loads `.json.gz` variation shards.

    Releases up to 0.4.0 read only plain `.json` variation shards and skip any
    other file without a warning, so writing gzipped shards for them would
    silently drop every co-located variant.
    """
    src = (
        cfg.src("vep-rs") / "crates" / "vep-cli" / "src" / "json_cache.rs"
    ).read_text()
    start = src.find("pub fn load_all_variations")
    end = src.find("\n}\n", start)
    return start >= 0 and "read_shard(" in src[start:end]


def perl_env(cfg: Config) -> dict:
    """Perl VEP's container ships JSON.pm; a plain Perl may only have JSON::PP."""
    env = dict(os.environ)
    if (
        subprocess.run(["perl", "-MJSON", "-e", "1"], capture_output=True).returncode
        != 0
    ):
        shim = cfg.work / "perllib"
        shim.mkdir(parents=True, exist_ok=True)
        (shim / "JSON.pm").write_text(
            "package JSON;\nuse JSON::PP ();\nour @ISA = ('JSON::PP');\n"
            "sub true { JSON::PP::true }\nsub false { JSON::PP::false }\n1;\n"
        )
        env["PERL5LIB"] = f"{shim}:{env.get('PERL5LIB', '')}"
    return env


def converter_carries_matrices(cfg: Config) -> bool:
    """True when the checkout's storable_to_json.pl writes the matrices itself."""
    text = (cfg.src("vep-rs") / "scripts" / "data" / "storable_to_json.pl").read_text()
    sub_at = text.find("sub extract_prediction_matrix")
    end = text.find("\n}\n", sub_at)
    return sub_at >= 0 and "$pm->{matrix}" in text[sub_at:end]


def converter_with_matrices(cfg: Config) -> Path:
    """storable_to_json.pl, patched if needed to carry the SIFT/PolyPhen matrices.

    The Storable keeps each matrix gzip-compressed in the `matrix` field; vep-rs
    reads the same bytes base64-encoded from `predictions_data`. Converters up to
    vep-rs 0.4.0 copy `predictions_data` only when it already exists, which it
    never does in a Storable, so they get a patched copy; a converter that reads
    `matrix` itself is used as it is.
    """
    original = cfg.src("vep-rs") / "scripts" / "data" / "storable_to_json.pl"
    if converter_carries_matrices(cfg):
        return original
    patched = cfg.work / "caches" / "vep-rs" / "storable_to_json_with_matrices.pl"
    text = original.read_text()
    sub_at = text.find("sub extract_prediction_matrix")
    ret_at = text.find("    return \\%out;", sub_at)
    if sub_at < 0 or ret_at < 0:
        sys.exit(
            "storable_to_json.pl changed shape: extract_prediction_matrix not found"
        )
    insert = (
        "    if (!defined $out{predictions_data} && defined $pm->{matrix} && length $pm->{matrix}) {\n"
        "        require MIME::Base64;\n"
        "        $out{predictions_data} = MIME::Base64::encode_base64($pm->{matrix}, '');\n"
        "    }\n"
    )
    text = text[:ret_at] + insert + text[ret_at:]
    patched.parent.mkdir(parents=True, exist_ok=True)
    patched.write_text(text)
    return patched


def strip_matrices(node) -> None:
    """Drop every prediction matrix from a converted transcript shard, in place."""
    if isinstance(node, dict):
        for m in (node.get("protein_function_predictions") or {}).values():
            if isinstance(m, dict):
                m.pop("predictions_data", None)
        for v in node.values():
            strip_matrices(v)
    elif isinstance(node, list):
        for v in node:
            strip_matrices(v)


def gzip_tree(src: Path, dest: Path, without_matrices: bool = False) -> None:
    for f in sorted(src.rglob("*.json")):
        out = dest / f.relative_to(src)
        out = out.with_name(out.name + ".gz")
        out.parent.mkdir(parents=True, exist_ok=True)
        if without_matrices:
            data = json.loads(f.read_bytes())
            strip_matrices(data)
            with gzip.open(out, "wt", compresslevel=6) as fo:
                json.dump(data, fo, separators=(",", ":"))
            continue
        with open(f, "rb") as fi, gzip.open(out, "wb", compresslevel=6) as fo:
            shutil.copyfileobj(fi, fo)


def all_vars_to_vep_rs_json(
    cache_dir: Path, chrom: str, out_dir: Path, gz: bool
) -> int:
    """Write a Perl cache's tabix all_vars.gz as vep-rs variations/<chrom>/<start>-<end>.json."""
    cols = read_info(cache_dir)["variation_cols"].split(",")
    bins = defaultdict(list)
    with gzip.open(cache_dir / chrom / "all_vars.gz", "rt") as fh:
        for line in fh:
            rec = {
                c: v
                for c, v in zip(cols, line.rstrip("\n").split("\t"))
                if v not in (".", "") and c != "chr"
            }
            start = (int(rec["start"]) - 1) // 1_000_000 * 1_000_000 + 1
            bins[start].append(rec)
    d = out_dir / "variations" / chrom
    d.mkdir(parents=True, exist_ok=True)
    n = 0
    for start, recs in bins.items():
        name = f"{start}-{start + 999_999}.json"
        opener = (
            (lambda p: gzip.open(p, "wt", compresslevel=6))
            if gz
            else (lambda p: open(p, "w"))
        )
        with opener(d / (name + (".gz" if gz else ""))) as out:
            json.dump(recs, out)
        n += len(recs)
    return n


def build_vep_rs_caches(cfg: Config) -> dict:
    gz_variations = vep_rs_reads_gzipped_variations(cfg)
    env = perl_env(cfg)
    # A converter that already carries the matrices serves both modes; the
    # nosift cache then has them stripped from its shards.
    carries = converter_carries_matrices(cfg)
    converters = {
        "nosift": cfg.src("vep-rs") / "scripts" / "data" / "storable_to_json.pl",
        "sift": converter_with_matrices(cfg),
    }
    shared_vars = {}
    for cache in CACHES:
        vep_cache = cfg.vep_cache[cache]
        # The two caches usually share one all_vars.gz; convert it once.
        all_vars = vep_cache / cfg.chrom / "all_vars.gz"
        var_root = None
        for prev, root in shared_vars.items():
            if filecmp.cmp(prev, all_vars, shallow=False):
                var_root = root
        if var_root is None:
            var_root = cfg.work / "caches" / "vep-rs" / f"variations-{cache}"
            if cfg.force or not (var_root / "variations" / cfg.chrom).exists():
                shutil.rmtree(var_root, ignore_errors=True)
                n = all_vars_to_vep_rs_json(
                    vep_cache, cfg.chrom, var_root, gz_variations
                )
                log(f"vep-rs {cache}: {n:,} co-located variants")
            shared_vars[all_vars] = var_root
        for mode, converter in converters.items():
            dest = cfg.vep_rs_cache(cache, mode)
            if not cfg.force and (dest / "transcripts" / cfg.chrom).exists():
                continue
            shutil.rmtree(dest, ignore_errors=True)
            raw = dest.parent / f"{mode}.raw"
            shutil.rmtree(raw, ignore_errors=True)
            run(["perl", converter, vep_cache, raw, cfg.chrom], env=env)
            gzip_tree(
                raw / "transcripts",
                dest / "transcripts",
                without_matrices=mode == "nosift" and carries,
            )
            shutil.copy(raw / "info.json", dest / "info.json")
            shutil.rmtree(raw)
            (dest / "variations").symlink_to(var_root / "variations")
    return {
        "gzipped_variation_shards": gz_variations,
        "converter_carries_matrices": carries,
    }


# --------------------------------------------------------------------------
# caches: fastVEP


def refseq_gff_url(cfg: Config) -> str:
    if cfg.refseq_gff_url:
        return cfg.refseq_gff_url
    # e.g. "GCF_000001405.40-RS_2025_08 - GCF_000001405.40_GRCh38.p14_genomic.gff"
    source = read_info(cfg.vep_cache["merged"])["source_refseq"]
    release, _, file = (s.strip() for s in source.partition(" - "))
    return NCBI_ANNOTATION_URL.format(release=release, file=file)


def contig_synonyms(cache_dir: Path, chrom: str) -> set:
    names = {chrom}
    for line in open(cache_dir / "chr_synonyms.txt"):
        a, _, b = line.rstrip("\n").partition("\t")
        if a == chrom:
            names.add(b)
        elif b == chrom:
            names.add(a)
    return names


def subset_gff(src: Path, dest: Path, contigs: set) -> None:
    if dest.exists():
        return
    with gzip.open(src, "rt") as fi, open(dest, "w") as fo:
        for line in fi:
            if line.startswith("#") or line.split("\t", 1)[0] in contigs:
                fo.write(line)


def all_vars_to_vcf(cache_dir: Path, chrom: str, fasta: Path, out: Path) -> int:
    """Write all_vars.gz as a VCF with anchored alleles, for a custom_vcf .osa2.

    fastVEP looks an .osa2 up by the input's own (POS, REF, ALT), so cache
    alleles in VEP's style (C/-) are anchored on the preceding base from the
    FASTA; the caller then left-aligns and trims them with `bcftools norm`.
    """
    info = read_info(cache_dir)
    cols = info["variation_cols"].split(",")
    for line in open(str(fasta) + ".fai"):
        name, length, offset, linebases, linewidth = line.split("\t")[:5]
        if name == chrom:
            length, offset, linebases, linewidth = (
                int(length),
                int(offset),
                int(linebases),
                int(linewidth),
            )
            break
    else:
        sys.exit(f"contig {chrom} not in {fasta}.fai")
    with open(fasta, "rb") as fh:
        fh.seek(offset)
        raw = fh.read((length // linebases + 1) * linewidth)
    seq = raw.replace(b"\n", b"").replace(b"\r", b"")[:length].decode().upper()
    comp = str.maketrans("ACGTN", "TGCAN")
    allele_ok = re.compile(r"^[ACGTN]+$|^-$")
    skip = {
        "chr",
        "start",
        "end",
        "allele_string",
        "strand",
        "var_synonyms",
        "variation_name",
    }
    keep_cols = [c for c in cols if c not in skip]
    recs = []
    with gzip.open(cache_dir / chrom / "all_vars.gz", "rt") as fh:
        for line in fh:
            d = dict(zip(cols, line.rstrip("\n").split("\t")))
            alleles = d["allele_string"].upper().split("/")
            if d.get("strand") == "-1":
                alleles = [a if a == "-" else a.translate(comp)[::-1] for a in alleles]
            if len(alleles) < 2 or not all(allele_ok.match(a) for a in alleles):
                continue
            pos = int(d["start"])
            if "-" in alleles or len({len(a) for a in alleles}) > 1:
                pos -= 1
                if pos < 1:
                    continue
                alleles = [seq[pos - 1] + ("" if a == "-" else a) for a in alleles]
            alts = [a for a in dict.fromkeys(alleles[1:]) if a != alleles[0]]
            if not alts:
                continue
            fields = [f"RS={d['variation_name']}"] + [
                f"{c}={re.sub(r'[ ;=]', '_', d[c]).replace(',', '&')}"
                for c in keep_cols
                if d.get(c) not in (None, ".", "")
            ]
            recs.append((pos, alleles[0], ",".join(alts), ";".join(fields)))
    recs.sort(key=lambda r: r[0])
    with open(out, "w") as o:
        o.write(f"##fileformat=VCFv4.2\n##contig=<ID={chrom},length={length}>\n")
        for c in ["RS"] + keep_cols:
            o.write(f'##INFO=<ID={c},Number=.,Type=String,Description="{c}">\n')
        o.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for pos, ref, alt, inf in recs:
            o.write(f"{chrom}\t{pos}\t.\t{ref}\t{alt}\t.\t.\t{inf}\n")
    return len(recs)


def build_fastvep_caches(cfg: Config) -> dict:
    require_tools("tabix", "bcftools")
    dl = cfg.work / "downloads"
    ens_gz = download(
        ENSEMBL_GFF3_URL.format(
            release=cfg.release, assembly=cfg.assembly, chrom=cfg.chrom
        ),
        dl / f"ensembl-{cfg.release}-chr{cfg.chrom}.gff3.gz",
    )
    ens_gff = dl / f"ensembl-{cfg.release}-chr{cfg.chrom}.gff3"
    if not ens_gff.exists():
        with gzip.open(ens_gz, "rb") as fi, open(ens_gff, "wb") as fo:
            shutil.copyfileobj(fi, fo)
    refseq_url = refseq_gff_url(cfg)
    refseq_gz = download(refseq_url, dl / Path(refseq_url).name)
    refseq_gff = dl / f"refseq-chr{cfg.chrom}.gff3"
    subset_gff(
        refseq_gz, refseq_gff, contig_synonyms(cfg.vep_cache["merged"], cfg.chrom)
    )

    gff3 = {
        "ensembl": f"Ensembl={ens_gff}",
        "merged": f"Ensembl={ens_gff},RefSeq={refseq_gff}",
    }
    for cache in CACHES:
        out = cfg.fastvep_cache_bin(cache)
        if cfg.force or not out.exists():
            out.parent.mkdir(parents=True, exist_ok=True)
            cmd = [
                cfg.fastvep_bin,
                "cache",
                "--gff3",
                gff3[cache],
                "--fasta",
                cfg.fasta,
                "-o",
                out,
                "--no-progress",
            ]
            if cache == "merged":
                cmd += ["--synonyms", cfg.vep_cache[cache] / "chr_synonyms.txt"]
            run(cmd)
        # --cache-dir needs all_vars.gz with a .tbi; the Perl cache ships a .csi.
        # Symlink the cache's files into a directory of our own and index there,
        # so the shared cache is never written to.
        vardir = cfg.fastvep_vardir(cache)
        chrom_dir = vardir / cfg.chrom
        if cfg.force or not (chrom_dir / "all_vars.gz.tbi").exists():
            shutil.rmtree(vardir, ignore_errors=True)
            chrom_dir.mkdir(parents=True)
            for name in ("info.txt", "chr_synonyms.txt"):
                (vardir / name).symlink_to(cfg.vep_cache[cache] / name)
            (chrom_dir / "all_vars.gz").symlink_to(
                cfg.vep_cache[cache] / cfg.chrom / "all_vars.gz"
            )
            run(
                [
                    "tabix",
                    "-f",
                    "-s",
                    "1",
                    "-b",
                    "5",
                    "-e",
                    "5",
                    chrom_dir / "all_vars.gz",
                ]
            )
        osa_dir = cfg.fastvep_osa2_dir(cache)
        if cfg.force or not list(osa_dir.glob("*.osa2")):
            shutil.rmtree(osa_dir, ignore_errors=True)
            osa_dir.mkdir(parents=True)
            raw_vcf = osa_dir.parent / f"vepcache-{cache}.vcf"
            norm_vcf = osa_dir.parent / f"vepcache-{cache}.norm.vcf.gz"
            n = all_vars_to_vcf(cfg.vep_cache[cache], cfg.chrom, cfg.fasta, raw_vcf)
            log(f"fastVEP {cache}: {n:,} co-located variants written for the .osa2")
            tmp = osa_dir.parent / f"sort-{cache}"
            norm = subprocess.Popen(
                [
                    "bcftools",
                    "norm",
                    "-f",
                    str(cfg.fasta),
                    "-m",
                    "-both",
                    "-c",
                    "w",
                    "-Ou",
                    str(raw_vcf),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                [
                    "bcftools",
                    "sort",
                    "-m",
                    "4G",
                    "-T",
                    str(tmp),
                    "-Oz",
                    "-o",
                    str(norm_vcf),
                ],
                stdin=norm.stdout,
                check=True,
                stderr=subprocess.DEVNULL,
            )
            if norm.wait() != 0:
                sys.exit("bcftools norm failed on the .osa2 source VCF")
            raw_vcf.unlink()
            run(
                [
                    cfg.fastvep_bin,
                    "sa-build",
                    "--source",
                    "custom_vcf",
                    "--name",
                    "vepcache",
                    "--format",
                    "osa2",
                    "--assembly",
                    cfg.assembly,
                    "--no-progress",
                    "-i",
                    norm_vcf,
                    "-o",
                    osa_dir / f"vepcache_chr{cfg.chrom}",
                ]
            )
            norm_vcf.unlink()
    return {"ensembl_gff3": str(ens_gz), "refseq_gff": refseq_url}


def step_caches(cfg: Config) -> None:
    meta = {}
    if "vep-rs" in cfg.tools:
        meta["vep-rs"] = build_vep_rs_caches(cfg)
    if "fastvep" in cfg.tools:
        meta["fastvep"] = build_fastvep_caches(cfg)
    (cfg.work / "caches" / "meta.json").write_text(json.dumps(meta, indent=1))


# --------------------------------------------------------------------------
# run


def engine_command(
    cfg: Config, tool: str, cache: str, mode: str, threads: int, out: Path
):
    env = dict(os.environ)
    if tool == "vepyr":
        cmd = [
            cfg.vepyr,
            "annotate",
            "-i",
            cfg.input_vcf,
            "-o",
            out,
            "--dir_cache",
            cfg.vepyr_cache[cache],
            "--fasta",
            cfg.fasta,
            "--fork",
            threads,
        ]
    elif tool == "vep-rs":
        # --fork sizes only the annotation pool; the variation load runs on
        # rayon's global pool, which RAYON_NUM_THREADS caps, and BGZF input
        # decompression has its own thread count.
        env["RAYON_NUM_THREADS"] = str(threads)
        cmd = [
            cfg.vep_rs_bin,
            "-i",
            cfg.input_vcf,
            "-o",
            out,
            "--vcf",
            "--everything",
            "--hgvs",
            "--offline",
            "--assembly",
            cfg.assembly,
            "--json_cache",
            cfg.vep_rs_cache(cache, mode),
            "--fasta",
            cfg.fasta,
            "--force_overwrite",
            "--no_stats",
            "--fork",
            threads,
            "--decompression_threads",
            "1",
        ]
    else:
        env["RAYON_NUM_THREADS"] = str(threads)
        cmd = [
            cfg.fastvep_bin,
            "annotate",
            "-i",
            cfg.input_vcf,
            "-o",
            out,
            "--fasta",
            cfg.fasta,
            "--transcript-cache",
            cfg.fastvep_cache_bin(cache),
            "--everything",
            "--hgvs",
            "--output-format",
            "vcf",
        ]
        cmd += (
            ["--sa-dir", cfg.fastvep_osa2_dir(cache)]
            if mode == "osa2"
            else ["--cache-dir", cfg.fastvep_vardir(cache)]
        )
    return [str(c) for c in cmd], env


def timed(cmd, env, log_path: Path) -> dict:
    """Run one command; report wall time, CPU time and peak RSS from wait4."""
    with open(log_path, "w") as lf:
        t0 = time.perf_counter()
        proc = subprocess.Popen(cmd, env=env, stdout=lf, stderr=subprocess.STDOUT)
        _, status, ru = os.wait4(proc.pid, 0)
        wall = time.perf_counter() - t0
    rc = os.waitstatus_to_exitcode(status)
    proc.returncode = rc
    if rc != 0:
        sys.exit(f"command failed ({rc}), see {log_path}: {' '.join(cmd)}")
    return {
        "wall_s": round(wall, 3),
        "user_s": round(ru.ru_utime, 3),
        "sys_s": round(ru.ru_stime, 3),
        "max_rss_bytes": ru.ru_maxrss * RSS_BYTES_PER_UNIT,
    }


def step_run(cfg: Config) -> None:
    configs = [(t, c, m) for t in cfg.tools for c in CACHES for m in MODES[t]]
    timings_path = cfg.results / "timings.json"
    cfg.results.mkdir(parents=True, exist_ok=True)
    timings = (
        json.loads(timings_path.read_text())
        if timings_path.exists() and not cfg.force
        else {}
    )
    for threads in cfg.threads:
        for tool, cache, mode in configs:
            key = f"{tool}|{cache}|{mode}|{threads}"
            repeats = (
                cfg.tabix_repeats
                if (tool, mode) == ("fastvep", "tabix")
                else cfg.repeats
            )
            if len(timings.get(key, [])) >= repeats:
                continue
            rd = cfg.run_dir(tool, cache, mode)
            rd.mkdir(parents=True, exist_ok=True)
            out = cfg.output_vcf(tool, cache, mode, threads)
            cmd, env = engine_command(cfg, tool, cache, mode, threads, out)
            # One discarded warm-up per configuration puts the caches in the page
            # cache, except for fastVEP's tabix mode, which takes minutes a run.
            if cfg.warmup and (tool, mode) != ("fastvep", "tabix"):
                log(f"warm-up {key}")
                timed(cmd, env, rd / f"t{threads}-warmup.log")
            runs = []
            for i in range(repeats):
                log(f"run {key} #{i + 1}")
                runs.append(timed(cmd, env, rd / f"t{threads}-r{i + 1}.log"))
                log(f"    {runs[-1]}")
            timings[key] = runs
            timings_path.write_text(json.dumps(timings, indent=1))


# --------------------------------------------------------------------------
# compare


def csq_format(path: Path) -> list:
    with open(path) as fh:
        for line in fh:
            if line.startswith("##INFO=<ID=CSQ"):
                return line.split("Format: ")[1].rstrip().rstrip('">').split("|")
            if not line.startswith("##"):
                break
    sys.exit(f"no CSQ header in {path}")


def iter_records(path: Path):
    with open(path) as fh:
        for line in fh:
            if line[0] == "#":
                continue
            c = line.rstrip("\n").split("\t", 8)
            yield (c[0].removeprefix("chr"), c[1], c[3], c[4]), c[7]


def csq_entries(info: str, fields: list, fastvep: bool) -> dict:
    csq = next((kv[4:] for kv in info.split(";") if kv.startswith("CSQ=")), "")
    out = {}
    if not csq:
        return out
    fi = {f: i for i, f in enumerate(fields)}
    for e in csq.split(","):
        p = e.split("|")
        if fastvep:
            # fastVEP writes GFF3 IDs (rna-NM_...) and '-' placeholders on
            # intergenic rows; rewrite those to Ensembl's form. Annotations are
            # left as they are.
            for f in ("Feature", "HGVSc"):
                if f in fi and p[fi[f]].startswith("rna-"):
                    p[fi[f]] = p[fi[f]][4:]
            if p[fi["Feature"]] == "-":
                for f in ("Gene", "Feature_type", "Feature", "BIOTYPE"):
                    if p[fi[f]] in ("-", "Transcript"):
                        p[fi[f]] = ""
        d = dict(zip(fields, p))
        out[(d.get("Allele", ""), d.get("Feature_type", ""), d.get("Feature", ""))] = d
    return out


def paired(ref_path: Path, eng_path: Path):
    """Yield (key, ref_info, eng_info) in reference order; tolerates reordering."""
    pending = {}
    eng = iter_records(eng_path)
    for key, rinfo in iter_records(ref_path):
        while key not in pending:
            try:
                k, i = next(eng)
            except StopIteration:
                break
            pending[k] = i
        yield key, rinfo, pending.pop(key, None)


def compare_engine(ref_path: Path, eng_path: Path, fastvep: bool) -> dict:
    ref_fields, eng_fields = csq_format(ref_path), csq_format(eng_path)
    stats = {
        f: {
            "eq": 0,
            "mismatch": 0,
            "ref_nonempty": 0,
            "eng_nonempty": 0,
            "ref_all_nonempty": 0,
        }
        for f in ref_fields
    }
    examples = {}
    rows = {
        "matched": 0,
        "only_ref": 0,
        "only_engine": 0,
        "identical": 0,
        "records": 0,
        "records_missing": 0,
    }
    only_ref_by_type = defaultdict(int)
    for key, rinfo, einfo in paired(ref_path, eng_path):
        rows["records"] += 1
        r = csq_entries(rinfo, ref_fields, False)
        e = csq_entries(einfo, eng_fields, fastvep) if einfo is not None else {}
        if einfo is None:
            rows["records_missing"] += 1
        for t, rv in r.items():
            for f in ref_fields:
                if rv.get(f, ""):
                    stats[f]["ref_all_nonempty"] += 1
            ev = e.get(t)
            if ev is None:
                rows["only_ref"] += 1
                only_ref_by_type[t[1]] += 1
                continue
            rows["matched"] += 1
            same_row = True
            for f in ref_fields:
                a, b = rv.get(f, ""), ev.get(f, "")
                s = stats[f]
                s["ref_nonempty"] += bool(a)
                s["eng_nonempty"] += bool(b)
                if a == b:
                    s["eq"] += 1
                else:
                    s["mismatch"] += 1
                    same_row = False
                    if f not in examples:
                        examples[f] = {
                            "variant": ":".join(key),
                            "feature": t[2],
                            "ensembl": a[:120],
                            "engine": b[:120],
                        }
            rows["identical"] += same_row
        rows["only_engine"] += len(set(e) - set(r))
    for f, s in stats.items():
        s["emitted"] = f in eng_fields
        s["status"] = column_status(s)
    return {
        "fields": ref_fields,
        "engine_fields": eng_fields,
        "extra_fields": [f for f in eng_fields if f not in ref_fields],
        "columns": stats,
        "examples": examples,
        "rows": rows,
        "only_ref_by_type": dict(only_ref_by_type),
    }


def column_status(s: dict) -> str:
    """exact | empty (no Ensembl value anywhere) | never_filled | near (>=99%) | off | not_emitted."""
    if not s["emitted"]:
        return "not_emitted"
    if s["mismatch"] == 0:
        if s["ref_all_nonempty"] == 0:
            return "empty"
        if s["ref_nonempty"] == 0:
            # Ensembl's values are all on rows this engine does not write
            # (the motif columns, for engines without regulatory annotation).
            return "never_filled"
        return "exact"
    total = s["eq"] + s["mismatch"]
    return "near" if total and s["eq"] / total >= 0.99 else "off"


def colocated_coverage(ref_path: Path, eng_path: Path) -> dict:
    """How many records with an Ensembl co-located ID the .osa2 lookup finds (FV_VEPCACHE RS)."""
    fields = csq_format(ref_path)
    iv = fields.index("Existing_variation")
    c = defaultdict(int)
    for _, rinfo, einfo in paired(ref_path, eng_path):
        csq = next((kv[4:] for kv in rinfo.split(";") if kv.startswith("CSQ=")), "")
        ref_ids = {
            x
            for e in csq.split(",")
            for x in (e.split("|")[iv].split("&") if len(e.split("|")) > iv else [])
            if x
        }
        m = re.search(r"FV_VEPCACHE=([^;\t]*)", einfo or "")
        eng_ids = set(re.findall(r"RS=([^&|,]+)", m.group(1))) if m else set()
        if ref_ids:
            c["ensembl_records_with_ids"] += 1
            c["found"] += bool(ref_ids & eng_ids)
            c["same_id_set"] += ref_ids == eng_ids
        elif eng_ids:
            c["engine_only"] += 1
    return dict(c)


def slice_reference(cfg: Config, cache: str) -> Path:
    out = cfg.reference_slice(cache)
    if out.exists() and not cfg.force:
        return out
    require_tools("tabix")
    out.parent.mkdir(parents=True, exist_ok=True)
    ref = cfg.reference[cache]
    contigs = capture(["tabix", "-l", ref]).split()
    contig = next((c for c in (cfg.chrom, f"chr{cfg.chrom}") if c in contigs), None)
    if contig is None:
        sys.exit(f"chromosome {cfg.chrom} not in {ref}")
    with open(out, "w") as fo:
        run(["tabix", "-H", ref], stdout=fo)
        fo.flush()
        run(["tabix", ref, contig], stdout=fo)
    return out


def step_compare(cfg: Config) -> None:
    path = cfg.results / "comparison.json"
    result = json.loads(path.read_text()) if path.exists() and not cfg.force else {}
    for cache in CACHES:
        ref = slice_reference(cfg, cache)
        for tool in cfg.tools:
            for mode in MODES[tool]:
                key = f"{tool}|{cache}|{mode}"
                if key in result:
                    continue
                outs = [cfg.output_vcf(tool, cache, mode, t) for t in cfg.threads]
                outs = [o for o in outs if o.exists()]
                if not outs:
                    continue
                entry = {
                    "thread_outputs_identical": len({body_md5(o) for o in outs}) == 1
                }
                if (tool, mode) == ("fastvep", "osa2"):
                    log(f"co-located coverage {key}")
                    entry["colocated"] = colocated_coverage(ref, outs[-1])
                if mode == SCORED_MODE[tool] or tool == "vep-rs":
                    log(f"compare {key}")
                    entry.update(
                        compare_engine(ref, outs[-1], fastvep=tool == "fastvep")
                    )
                result[key] = entry
                path.write_text(json.dumps(result, indent=1))


# --------------------------------------------------------------------------
# cache sizes


def storable_matrix_bytes(cache_dir: Path, chrom: str) -> int:
    """Bytes of the unique SIFT/PolyPhen matrices inside a Storable chromosome."""
    script = r"""
use Storable qw(fd_retrieve); use Scalar::Util qw(refaddr);
my $b = 0;
for my $f (grep { !/_reg\.gz$/ } glob("$ARGV[0]/*-*.gz")) {
  open(my $fh, "-|", "gzip", "-dc", $f) or die; my $d = fd_retrieve($fh); my %seen;
  for my $c (keys %$d) { for my $t (@{$d->{$c}}) {
    my $p = $t->{_variation_effect_feature_cache}{protein_function_predictions} or next;
    for my $m (values %$p) { next unless ref $m && defined $m->{matrix}; next if $seen{refaddr $m}++; $b += length $m->{matrix} } } } }
print $b;
"""
    return int(capture(["perl", "-e", script, cache_dir / chrom]))


def cache_sizes(cfg: Config) -> dict:
    out = {}
    for cache in CACHES:
        vc = cfg.vep_cache[cache] / cfg.chrom
        transcripts = sum(
            f.stat().st_size
            for f in vc.glob("*-*.gz")
            if not f.name.endswith("_reg.gz")
        )
        regulatory = sum(f.stat().st_size for f in vc.glob("*_reg.gz"))
        all_vars = sum(f.stat().st_size for f in vc.glob("all_vars.gz*"))
        matrices = storable_matrix_bytes(cfg.vep_cache[cache], cfg.chrom)
        sizes = {
            "perl_storable": {
                "with": transcripts + regulatory + all_vars,
                "without": transcripts - matrices + regulatory + all_vars,
                "transcripts": transcripts,
                "matrices": matrices,
                "variation": all_vars,
            }
        }
        root = cfg.vepyr_cache[cache]
        parts = {
            d.name: (d / f"chr{cfg.chrom}.parquet").stat().st_size
            for d in root.iterdir()
            if (d / f"chr{cfg.chrom}.parquet").exists()
        }
        total = sum(parts.values())
        sizes["vepyr"] = {
            "with": total,
            "without": total - parts.get("translation_sift", 0),
            "parts": parts,
        }
        if "vep-rs" in cfg.tools:
            variations = dir_size(
                (cfg.vep_rs_cache(cache, "sift") / "variations").resolve()
            )
            with_t = dir_size(cfg.vep_rs_cache(cache, "sift") / "transcripts")
            without_t = dir_size(cfg.vep_rs_cache(cache, "nosift") / "transcripts")
            sizes["vep-rs"] = {
                "with": with_t + variations,
                "without": without_t + variations,
                "transcripts_with": with_t,
                "transcripts_without": without_t,
                "variation": variations,
            }
        if "fastvep" in cfg.tools:
            tbin = cfg.fastvep_cache_bin(cache).stat().st_size
            chrom_dir = cfg.fastvep_vardir(cache) / cfg.chrom
            tabix = (chrom_dir / "all_vars.gz").resolve().stat().st_size + (
                chrom_dir / "all_vars.gz.tbi"
            ).stat().st_size
            osa2 = dir_size(cfg.fastvep_osa2_dir(cache))
            sizes["fastvep_tabix"] = {
                "with": None,
                "without": tbin + tabix,
                "transcripts": tbin,
                "variation": tabix,
            }
            sizes["fastvep_osa2"] = {
                "with": None,
                "without": tbin + osa2,
                "transcripts": tbin,
                "variation": osa2,
            }
        out[cache] = sizes
    return out


# --------------------------------------------------------------------------
# report


def host_info() -> dict:
    info = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
    }
    try:
        if sys.platform == "darwin":
            info["cpu"] = capture(["sysctl", "-n", "machdep.cpu.brand_string"])
            info["memory_bytes"] = int(capture(["sysctl", "-n", "hw.memsize"]))
        else:
            info["cpu"] = next(
                line.split(":", 1)[1].strip()
                for line in open("/proc/cpuinfo")
                if line.startswith("model name")
            )
            info["memory_bytes"] = (
                int(
                    next(
                        line.split()[1]
                        for line in open("/proc/meminfo")
                        if line.startswith("MemTotal")
                    )
                )
                * 1024
            )
    except (OSError, StopIteration, subprocess.CalledProcessError):
        pass
    info["logical_cpus"] = os.cpu_count()
    return info


def summarize_timings(timings: dict) -> dict:
    out = {}
    for key, runs in timings.items():
        out[key] = {
            "runs": len(runs),
            "wall_s": round(statistics.median(r["wall_s"] for r in runs), 2),
            "cpu_s": round(
                statistics.median(r["user_s"] + r["sys_s"] for r in runs), 2
            ),
            "max_rss_gib": round(
                statistics.median(r["max_rss_bytes"] for r in runs) / 2**30, 2
            ),
        }
    return out


STATUS_MD = {
    "exact": "exact",
    "empty": "empty in both",
    "never_filled": "never filled",
    "not_emitted": "not emitted",
}


def status_cell(s: dict) -> str:
    if s["status"] in STATUS_MD:
        return STATUS_MD[s["status"]]
    total = s["eq"] + s["mismatch"]
    return f"{s['eq'] / total:.3%}"


def exact_count(cmp_entry: dict) -> int:
    return sum(
        1 for s in cmp_entry["columns"].values() if s["status"] in ("exact", "empty")
    )


def step_report(cfg: Config) -> None:
    timings = json.loads((cfg.results / "timings.json").read_text())
    comparison = json.loads((cfg.results / "comparison.json").read_text())
    commits = (
        json.loads((cfg.work / "commits.json").read_text())
        if (cfg.work / "commits.json").exists()
        else {}
    )
    meta = (
        json.loads((cfg.work / "caches" / "meta.json").read_text())
        if (cfg.work / "caches" / "meta.json").exists()
        else {}
    )
    vepyr_version = capture([cfg.vepyr, "--version"]) if "vepyr" in cfg.tools else None
    ref_header = next(
        line.strip()
        for line in open(cfg.reference_slice("ensembl"))
        if line.startswith("##VEP=")
    )
    sizes = cache_sizes(cfg)
    summary = summarize_timings(timings)
    results = {
        "host": host_info(),
        "chromosome": cfg.chrom,
        "input": {
            "path": str(cfg.input_vcf),
            "sha256": sha256(cfg.input_vcf),
            "records": comparison.get("vepyr|ensembl|default", {})
            .get("rows", {})
            .get("records"),
        },
        "reference_vep": ref_header,
        "versions": {
            "vepyr": vepyr_version,
            "vep-rs": commits.get("vep-rs"),
            "fastvep": commits.get("fastvep"),
        },
        "cache_meta": meta,
        "threads": cfg.threads,
        "timings": summary,
        "timings_raw": timings,
        "cache_sizes": sizes,
        "comparison": comparison,
    }
    (cfg.results / "results.json").write_text(json.dumps(results, indent=1))
    (cfg.results / "report.md").write_text(render_markdown(cfg, results))
    log(f"wrote {cfg.results / 'report.md'}")


def render_markdown(cfg: Config, res: dict) -> str:
    cmp_ = res["comparison"]
    tools = [t for t in ("vepyr", "vep-rs", "fastvep") if t in cfg.tools]
    scored = {t: SCORED_MODE[t] for t in tools}
    L = []
    h = res["host"]
    L += [
        f"# vepyr, vep-rs and fastVEP on chr{cfg.chrom}",
        "",
        f"Reference: `{res['reference_vep'][:80]}…`",
        "",
        f"- Host: {h.get('cpu', '?')}, {h.get('logical_cpus')} logical CPUs, "
        f"{round(h.get('memory_bytes', 0) / 2**30)} GiB, {h['platform']}",
        f"- vepyr: {res['versions']['vepyr']}",
        f"- vep-rs: `{res['versions']['vep-rs']}`",
        f"- fastVEP: `{res['versions']['fastvep']}`",
        f"- Input: `{Path(res['input']['path']).name}`, {res['input']['records']:,} records, "
        f"SHA-256 `{res['input']['sha256']}`",
        "",
    ]

    # Columns with no mismatches
    L += [
        "## CSQ columns with no mismatches",
        "",
        "A column counts when every row the engine shares with Ensembl VEP holds the same value, and Ensembl "
        "has no value in it on rows the engine does not write.",
        "",
        "| Cache | Ensembl columns | " + " | ".join(LABEL[t] for t in tools) + " |",
        "|---|---|" + "---|" * len(tools),
    ]
    for cache in CACHES:
        entries = {t: cmp_.get(f"{t}|{cache}|{scored[t]}") for t in tools}
        n = len(next(e for e in entries.values() if e)["fields"])
        L.append(
            f"| {cache} | {n} | "
            + " | ".join(str(exact_count(e)) if e else "–" for e in entries.values())
            + " |"
        )
    L += [
        "",
        "| Engine (scored mode) | Columns emitted | Of Ensembl's | Rows identical | Rows missing / extra |",
        "|---|---|---|---|---|",
    ]
    for t in tools:
        e = cmp_.get(f"{t}|ensembl|{scored[t]}")
        if not e:
            continue
        r = e["rows"]
        total = r["matched"] + r["only_ref"]
        L.append(
            f"| {LABEL[t]} ({scored[t]}) | {len(e['engine_fields'])} | "
            f"{len(e['engine_fields']) - len(e['extra_fields'])} | {r['identical'] / total:.2%} | "
            f"{r['only_ref']:,} / {r['only_engine']:,} |"
        )

    # Per-column tables
    for cache in CACHES:
        entries = {t: cmp_.get(f"{t}|{cache}|{scored[t]}") for t in tools}
        first = next(e for e in entries.values() if e)
        L += [
            "",
            f"### Every column, {cache} cache",
            "",
            "| Column | " + " | ".join(LABEL[t] for t in tools) + " |",
            "|---|" + "---|" * len(tools),
        ]
        for f in first["fields"]:
            L.append(
                f"| `{f}` | "
                + " | ".join(
                    status_cell(entries[t]["columns"][f]) if entries[t] else "–"
                    for t in tools
                )
                + " |"
            )
        for t in tools:
            if entries[t] and entries[t]["extra_fields"]:
                L.append(
                    f"\n{LABEL[t]} also writes: "
                    + ", ".join(f"`{f}`" for f in entries[t]["extra_fields"])
                )

    # Cache sizes
    s = res["cache_sizes"]
    L += [
        "",
        f"## Cache size on disk, chr{cfg.chrom} (MB)",
        "",
        "| Engine and format | ensembl, with SIFT/PolyPhen | ensembl, without | merged, with | merged, without |",
        "|---|---|---|---|---|",
    ]
    rows = [
        ("perl_storable", "Perl VEP Storable (source)"),
        ("vepyr", "vepyr, Parquet"),
        ("vep-rs", "vep-rs, JSON"),
        ("fastvep_tabix", "fastVEP, `--cache-dir`"),
        ("fastvep_osa2", "fastVEP, `.osa2`"),
    ]
    for key, label in rows:
        if key not in s["ensembl"]:
            continue
        cells = []
        for cache in CACHES:
            v = s[cache][key]
            cells += [
                "n/a" if v["with"] is None else f"{mb(v['with']):,}",
                f"{mb(v['without']):,}",
            ]
        L.append(f"| {label} | " + " | ".join(cells) + " |")

    # Timings
    L += [
        "",
        "## Run time and memory",
        "",
        "Median of the timed runs after one warm-up (fastVEP `--cache-dir`: no warm-up). CPU-seconds are user "
        "plus system time.",
        "",
        "| Engine | Mode | Cache | "
        + " | ".join(
            f"{n} thr wall (s) | {n} thr CPU-s | {n} thr RSS (GiB)" for n in cfg.threads
        )
        + " |",
        "|---|---|---|" + "---|---|---|" * len(cfg.threads),
    ]
    for t in tools:
        for mode in MODES[t]:
            for cache in CACHES:
                cells = []
                for n in cfg.threads:
                    v = res["timings"].get(f"{t}|{cache}|{mode}|{n}")
                    cells += (
                        [str(v["wall_s"]), str(v["cpu_s"]), str(v["max_rss_gib"])]
                        if v
                        else ["–"] * 3
                    )
                L.append(
                    f"| {LABEL[t]} | {mode} | {cache} | " + " | ".join(cells) + " |"
                )

    # Determinism and co-located coverage
    L += [
        "",
        "## Same output at every thread count",
        "",
        "| Engine | Mode | Cache | Identical |",
        "|---|---|---|---|",
    ]
    for key, e in cmp_.items():
        t, cache, mode = key.split("|")
        L.append(
            f"| {LABEL[t]} | {mode} | {cache} | {'yes' if e['thread_outputs_identical'] else 'no'} |"
        )
    for cache in CACHES:
        c = cmp_.get(f"fastvep|{cache}|osa2", {}).get("colocated")
        if c:
            n = c.get("ensembl_records_with_ids", 0)
            L += [
                "",
                f"fastVEP `.osa2`, {cache}: found a co-located ID for {c.get('found', 0):,} of the {n:,} records "
                f"Ensembl matches ({c.get('found', 0) / max(n, 1):.2%}); identical ID set on {c.get('same_id_set', 0):,}. "
                "These values go to the `FV_VEPCACHE` INFO field, not to CSQ.",
            ]
    L.append("")
    return "\n".join(L)


# --------------------------------------------------------------------------
# CLI


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "step", choices=("build", "caches", "run", "compare", "report", "all")
    )
    p.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="clones, builds, caches, outputs and results",
    )
    p.add_argument(
        "--chrom",
        default="22",
        help="chromosome to compare, as the Ensembl cache names it (default: 22)",
    )
    p.add_argument(
        "--input-vcf",
        type=Path,
        required=True,
        help="bgzipped, indexed input normalised with bcftools norm -m -both",
    )
    p.add_argument(
        "--fasta",
        type=Path,
        required=True,
        help="Ensembl primary-assembly FASTA with a .fai",
    )
    p.add_argument(
        "--vep-cache-ensembl",
        type=Path,
        required=True,
        help="Perl VEP cache, e.g. .../homo_sapiens/116_GRCh38",
    )
    p.add_argument(
        "--vep-cache-merged",
        type=Path,
        required=True,
        help="Perl VEP merged cache, e.g. .../homo_sapiens_merged/116_GRCh38",
    )
    p.add_argument(
        "--vepyr-cache-ensembl",
        type=Path,
        required=True,
        help="vepyr Parquet cache, e.g. .../116_GRCh38_ensembl",
    )
    p.add_argument(
        "--vepyr-cache-merged",
        type=Path,
        required=True,
        help="vepyr Parquet cache, e.g. .../116_GRCh38_merged",
    )
    p.add_argument(
        "--reference-ensembl",
        type=Path,
        required=True,
        help="Ensembl VEP --everything --hgvs --vcf output, tabix-indexed",
    )
    p.add_argument(
        "--reference-merged", type=Path, required=True, help="the same with --merged"
    )
    p.add_argument(
        "--release",
        type=int,
        help="Ensembl release (default: from the cache directory name)",
    )
    p.add_argument(
        "--vepyr-version",
        default="0.10.0",
        help="vepyr release installed from PyPI (default: 0.10.0)",
    )
    p.add_argument(
        "--vepyr-bin",
        type=Path,
        help="use this vepyr executable instead of installing one",
    )
    p.add_argument("--vep-rs-repo", default=VEP_RS_REPO)
    p.add_argument(
        "--vep-rs-ref", default="main", help="branch, tag or commit (default: main)"
    )
    p.add_argument("--fastvep-repo", default=FASTVEP_REPO)
    p.add_argument(
        "--fastvep-ref",
        default="master",
        help="branch, tag or commit (default: master)",
    )
    p.add_argument(
        "--refseq-gff-url",
        help="RefSeq GFF for fastVEP's merged cache (default: the release the merged cache names)",
    )
    p.add_argument(
        "--threads", default="1,2", help="comma-separated thread counts (default: 1,2)"
    )
    p.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="timed runs per configuration (default: 3)",
    )
    p.add_argument(
        "--fastvep-tabix-repeats",
        type=int,
        default=1,
        help="timed runs for fastVEP --cache-dir, which takes minutes a run (default: 1)",
    )
    p.add_argument(
        "--no-warmup", action="store_true", help="skip the discarded warm-up run"
    )
    p.add_argument(
        "--tools",
        default="vepyr,vep-rs,fastvep",
        help="engines to include (default: all three)",
    )
    p.add_argument(
        "--force", action="store_true", help="redo work whose outputs already exist"
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = Config(args)
    steps = {
        "build": step_build,
        "caches": step_caches,
        "run": step_run,
        "compare": step_compare,
        "report": step_report,
    }
    for name in list(steps) if args.step == "all" else [args.step]:
        log(f"== {name}")
        steps[name](cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
