#!/usr/bin/env python3
"""Whole-genome parity manifest: prepare / record-staged / check.

`prepare` pins the goldens, inputs and cache revisions from the maintainer's
verified data. `record-staged` snapshots what stage-data.sh put on the runner.
`check` compares that snapshot with the manifest (sizes only, no re-hash: the
read-only mount protects contents).
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PROFILES = {"ensembl": "vep", "merged": "merged", "refseq": "refseq"}
DEFAULT_CHR22 = REPO / "e2e-testing" / "golden" / "116" / "chr22" / "manifest.json"
VCF = "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
FASTA = "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
RESTAGE = "re-run ci/wgs-runner/stage-data.sh"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 * 1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _entry(data: Path, rel: str) -> dict:
    p = data / rel
    return {"sha256": _sha256(p), "size": p.stat().st_size}


def _golden_rel(suffix: str) -> str:
    return f"output/116/HG002_annotated_wgs_everything_hgvs_{suffix}.vcf.gz"


def _canonical(manifest: dict) -> str:
    return json.dumps(manifest, indent=2, sort_keys=True)


def _manifest_sha(manifest: dict) -> str:
    return hashlib.sha256(_canonical(manifest).encode()).hexdigest()


def build_manifest(data: Path, chr22_manifest: Path, profiles: dict[str, str]) -> dict:
    chr22 = json.loads(Path(chr22_manifest).read_text(encoding="utf-8"))
    profs, inputs = {}, {VCF: _entry(data, VCF)}
    for name, suffix in profiles.items():
        rel = _golden_rel(suffix)
        profs[name] = {"golden": rel, **_entry(data, rel)}
        inputs[rel + ".tbi"] = _entry(data, rel + ".tbi")
    inputs[VCF + ".tbi"] = _entry(data, VCF + ".tbi")
    caches = {
        f: {
            "repo_id": chr22["caches"][f]["repo_id"],
            "revision": chr22["caches"][f]["revision"],
        }
        for f in profiles
    }
    return {
        "schema_version": 1,
        "release": "116",
        "profiles": profs,
        "inputs": inputs,
        "fasta": FASTA,
        "caches": caches,
    }


def _tracked_files(manifest: dict) -> list[str]:
    return [p["golden"] for p in manifest["profiles"].values()] + list(
        manifest["inputs"]
    )


def record_staged(data: Path, manifest: dict) -> dict:
    files = {rel: _entry(data, rel) for rel in _tracked_files(manifest)}
    cache_files = {}
    for flavour in manifest["caches"]:
        root = data / f"cache/116_GRCh38_{flavour}"
        for p in sorted(root.rglob("*")):
            if p.is_file():
                cache_files[p.relative_to(data).as_posix()] = p.stat().st_size
    return {
        "manifest_sha256": _manifest_sha(manifest),
        "files": files,
        "cache_files": cache_files,
        "caches": {f: c["revision"] for f, c in manifest["caches"].items()},
    }


def check_staged(data: Path, manifest: dict, staged: dict) -> list[str]:
    problems = []
    if staged.get("manifest_sha256") != _manifest_sha(manifest):
        problems.append("manifest changed since staging")
    expected = {p["golden"]: (n, p["sha256"]) for n, p in manifest["profiles"].items()}
    expected.update({rel: (rel, e["sha256"]) for rel, e in manifest["inputs"].items()})
    sfiles = staged.get("files", {})
    for rel, (label, sha) in expected.items():
        s = sfiles.get(rel)
        if s is None:
            problems.append(f"{rel} ({label}): not recorded in STAGED.json")
            continue
        if s["sha256"] != sha:
            problems.append(f"{rel} ({label}): staged sha256 differs from manifest")
        p = data / rel
        if not p.is_file():
            problems.append(f"{rel} ({label}): missing on disk")
        elif p.stat().st_size != s["size"]:
            problems.append(
                f"{rel} ({label}): size {p.stat().st_size} != staged {s['size']} (truncated or replaced)"
            )
    for rel, size in staged.get("cache_files", {}).items():
        p = data / rel
        if not p.is_file():
            problems.append(f"{rel}: cache file missing on disk")
        elif p.stat().st_size != size:
            problems.append(
                f"{rel}: cache file size {p.stat().st_size} != staged {size}"
            )
    for f, c in manifest["caches"].items():
        if staged.get("caches", {}).get(f) != c["revision"]:
            problems.append(
                f"cache {f}: staged revision {staged.get('caches', {}).get(f)} != manifest {c['revision']}"
            )
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--data", type=Path, required=True)
    p.add_argument("--chr22-manifest", type=Path, default=DEFAULT_CHR22)
    p.add_argument("--out", type=Path, required=True)
    r = sub.add_parser("record-staged")
    r.add_argument("--data", type=Path, required=True)
    r.add_argument("--manifest", type=Path, required=True)
    r.add_argument("--out", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("--data", type=Path, required=True)
    c.add_argument("--manifest", type=Path, required=True)
    a = ap.parse_args(argv)

    if a.cmd == "prepare":
        m = build_manifest(a.data, a.chr22_manifest, PROFILES)
        if a.out.is_file():
            previous = json.loads(a.out.read_text(encoding="utf-8"))
            if previous.get("clinvar_profiles"):
                m["clinvar_profiles"] = previous["clinvar_profiles"]
        a.out.write_text(_canonical(m) + "\n", encoding="utf-8")
        return 0
    manifest = json.loads(a.manifest.read_text(encoding="utf-8"))
    if a.cmd == "record-staged":
        a.out.write_text(
            json.dumps(record_staged(a.data, manifest), indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        return 0
    sp = a.data / "STAGED.json"
    if not sp.is_file():
        print(f"no STAGED.json under {a.data} — {RESTAGE}", file=sys.stderr)
        return 3
    problems = check_staged(
        a.data, manifest, json.loads(sp.read_text(encoding="utf-8"))
    )
    if problems:
        print("\n".join(problems), file=sys.stderr)
        print(RESTAGE, file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
