"""Summarise whole/<set>/<chromosome>.json written by scan_all.zsh.

Usage: aggregate.py [set ...]
Per set: chromosomes scanned, records, CSQ entries, entries per Feature_type, declared fields, the fields that have
no value on any chromosome, and for each of the remaining rarely filled fields the chromosomes without a value.
For engine sets the fields are also compared with the VEP set of the same cache and data set.
"""

import json
import sys
from pathlib import Path

D = Path(__file__).resolve().parent / "whole"
ORDER = [str(c) for c in range(1, 23)] + ["X", "Y", "MT"]


def load(name):
    out = {}
    for c in ORDER:
        p = D / name / f"{c}.json"
        if p.exists():
            out[c] = json.load(open(p))
    return out


def summary(name):
    chrom = load(name)
    if not chrom:
        return None
    fields = next(iter(chrom.values()))["fields"]
    filled = set().union(*(set(v["filled"]) for v in chrom.values()))
    feat = {}
    for v in chrom.values():
        for k, n in v["feature_types"].items():
            feat[k or "intergenic"] = feat.get(k or "intergenic", 0) + n
    return {
        "chromosomes": list(chrom),
        "records": sum(v["records"] for v in chrom.values()),
        "entries": sum(v["entries"] for v in chrom.values()),
        "feature_types": feat,
        "fields": fields,
        "filled": [f for f in fields if f in filled],
        "never": [f for f in fields if f not in filled],
        "per_chromosome_never": {c: v["never"] for c, v in chrom.items()},
    }


names = sys.argv[1:] or sorted(p.name for p in D.iterdir() if p.is_dir())
res = {n: summary(n) for n in names}
for n, s in res.items():
    if not s:
        print(f"\n== {n}: nothing scanned yet")
        continue
    print(
        f"\n== {n}: {len(s['chromosomes'])} chromosomes ({s['chromosomes'][0]}..{s['chromosomes'][-1]}), "
        f"{s['records']:,} records, {s['entries']:,} CSQ entries"
    )
    print(
        "   entries by feature type:",
        {k: f"{v:,}" for k, v in sorted(s["feature_types"].items())},
    )
    print(
        f"   declared {len(s['fields'])}, with a value somewhere {len(s['filled'])}, never {len(s['never'])}: {s['never']}"
    )
    if n.startswith("vep-") and not n.startswith("vep-rs-"):
        rare = {}
        for c, never in s["per_chromosome_never"].items():
            for f in never:
                if f not in s["never"]:
                    rare.setdefault(f, []).append(c)
        for f, cs in rare.items():
            print(f"   {f}: no value on chromosomes {cs}")
    else:
        cache_ds = (
            n.split("-", 1)[1] if not n.startswith("vep-rs-") else n[len("vep-rs-") :]
        )
        ref = res.get("vep-" + cache_ds) or summary("vep-" + cache_ds)
        if ref:
            vf = ref["fields"]
            declared = [f for f in vf if f in s["fields"]]
            print(
                f"   of the {len(vf)} VEP fields ({len(ref['filled'])} filled by VEP on {len(ref['chromosomes'])} chromosomes): "
                f"declared {len(declared)}, filled {len([f for f in vf if f in s['filled']])}"
            )
            print(
                "   VEP fields not declared:", [f for f in vf if f not in s["fields"]]
            )
            print(
                "   declared, never filled:", [f for f in declared if f in s["never"]]
            )
            print("   own fields:", [f for f in s["fields"] if f not in vf])
