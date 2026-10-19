#!/usr/bin/env python3
"""ClinVar progress panel: records and CSQ entries fixed per issue after a merge round.

The panel (``$DATA_VEPYR_DIR/clinvar_panel``, built once from the ClinVar
20261011 triage; see its README.md) holds every ClinVar record on which vepyr
0.10.0 differed from Ensembl VEP 116.0, 9,249 in all:

  in/<contig>.vcf        the ClinVar header plus the differing records
  vep116_0/<contig>.vcf  Ensembl VEP 116.0 on exactly those records
  ledger.tsv             one row per (record, differing CSQ entry) -> issue(s)

This script annotates ``in/`` with a vepyr build (``output_vcf=``, workers=1),
compares each record with VEP strictly and each CSQ entry field by field, joins
the result with the ledger and writes, under ``--out``:

  progress_<label>.tsv     per issue: records/entries attributed and now identical
  entries_<label>.tsv      per (record, entry): its state now, read back by --prev
  regressions_<label>.tsv  entries that differ now but did not before, and
                           attributed entries that now differ in other fields
  summary_<label>.md       a block ready to paste into the tracking issue (#240)
  run_<label>.json         provenance: vepyr version, cache, timings

Two rules shape every count:

- ``CLIN_SIG`` order is VEP-nondeterministic (vepyr#238). VEP joins a
  multi-valued CLIN_SIG from a Perl hash's keys, so its ``%3B`` groups are
  compared sorted on both sides, and a record that matches only after the sort
  is counted as identical and listed as VEP-nondeterministic.
- Two records are left out of every count, as the ClinVar fixtures leave them
  out: 20:63674046 and 7:117544096. Annotated on their own, vepyr writes a
  different HGNC_ID for one RefSeq transcript than in the full run, so the
  panel cannot judge them.

vepyr#201 (MT-end HGVS flank) is a confirmed VEP bug whose fix is deferred; its
records are reported as "known VEP bug (deferred)", never as a target.

Example::

    clinvar_progress.py --python .venv/bin/python \\
        --cache ~/workspace/data_vepyr/cache/116_GRCh38_merged --label R0-baseline
    clinvar_progress.py --python .venv/bin/python --cache <overlay> --label R1 \\
        --prev ~/workspace/data_vepyr/clinvar_panel/progress/progress_R0-baseline.tsv
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import csv
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

DATA = Path(os.environ.get("DATA_VEPYR_DIR", Path.home() / "workspace" / "data_vepyr"))
DEFAULT_PANEL = DATA / "clinvar_panel"
DEFAULT_FASTA = DATA / "input" / "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
CONTIGS = [str(n) for n in range(1, 23)] + ["X", "MT"]

# (contig, pos): per-record annotation gives a different HGNC_ID than the full
# run (e2e/clinvar-parity README), so the panel cannot judge these records.
EXCLUDED = {("20", "63674046"), ("7", "117544096")}
# Issue whose fix is deferred because the difference is a confirmed VEP bug.
DEFERRED = {"vepyr#201": "known VEP bug (deferred)"}

# vepyr#238: the CSQ sub-field VEP fills from a Perl hash's keys, and the
# separator it joins them with (`;`, percent-encoded inside CSQ).
CLIN_SIG_FIELD = "CLIN_SIG"
CLIN_SIG_SEPARATOR = "%3B"

RecordKey = tuple[str, str, str, str, str]  # contig, pos, ref, alt, clinvar_id


# --------------------------------------------------------------------------
# VCF parsing and comparison
# --------------------------------------------------------------------------


def read_vcf(path: Path) -> tuple[list[str], dict[tuple[str, str, str, str], str]]:
    """Header lines and {(pos, id, ref, alt): line} of a plain-text VCF."""
    header: list[str] = []
    body: dict[tuple[str, str, str, str], str] = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                header.append(line)
                continue
            cols = line.split("\t", 5)
            body.setdefault((cols[1], cols[2], cols[3], cols[4]), line.rstrip("\n"))
    return header, body


def csq_format(header: list[str]) -> list[str]:
    for line in header:
        if line.startswith("##INFO=<ID=CSQ"):
            match = re.search(r'Format: ([^"]+)"', line)
            if match:
                return match.group(1).split("|")
    raise ValueError("no ##INFO=<ID=CSQ ... Format: ...> header line")


def sort_clin_sig(value: str) -> str:
    """CLIN_SIG with its %3B groups sorted; the '&' order inside a group stays."""
    if CLIN_SIG_SEPARATOR not in value:
        return value
    return CLIN_SIG_SEPARATOR.join(sorted(value.split(CLIN_SIG_SEPARATOR)))


def split_record(line: str, fmt: list[str]):
    """(non-CSQ columns, {(allele, feature): {field: value}}) of one record.

    Entries are keyed by Allele and Feature; a repeated key gets a ``#n``
    suffix so a duplicated entry stays visible.
    """
    cols = line.split("\t")
    rest, csq = [], []
    for kv in cols[7].split(";"):
        if kv.startswith("CSQ="):
            csq = kv[4:].split(",")
        else:
            rest.append(kv)
    entries: dict[tuple[str, str], dict[str, str]] = {}
    fi, ai = fmt.index("Feature"), fmt.index("Allele")
    for raw in csq:
        values = raw.split("|")
        base = (values[ai], values[fi])
        key, n = base, 1
        while key in entries:
            n += 1
            key = (base[0], f"{base[1]}#{n}")
        entries[key] = dict(zip(fmt, values))
    non_csq = "\t".join(cols[:7] + [";".join(rest)] + cols[8:])
    return non_csq, entries


def entry_diffs(
    vep_line: str, vepyr_line: str, vep_fmt: list[str], vepyr_fmt: list[str]
) -> tuple[dict[str, list[str]], bool]:
    """Differing entries of one record, as {feature: [fields]}, and whether a
    CLIN_SIG value matched only after sorting (VEP-nondeterministic).

    Pseudo features: ``INFO`` for the non-CSQ columns, ``ORDER`` when the
    entries are equal but in another order.
    """
    rest_a, ea = split_record(vep_line, vep_fmt)
    rest_b, eb = split_record(vepyr_line, vepyr_fmt)
    out: dict[str, list[str]] = {}
    nondeterministic = False
    if rest_a != rest_b:
        out["INFO"] = ["INFO"]
    for key in list(ea) + [k for k in eb if k not in ea]:
        feature = key[1]
        if key not in eb:
            out[feature] = ["ONLY_IN_VEP"]
            continue
        if key not in ea:
            out[feature] = ["ONLY_IN_VEPYR"]
            continue
        fields = []
        for name in vep_fmt:
            a, b = ea[key].get(name, ""), eb[key].get(name, "")
            if a == b:
                continue
            if name == CLIN_SIG_FIELD and sort_clin_sig(a) == sort_clin_sig(b):
                nondeterministic = True
                continue
            fields.append(name)
        for name in vepyr_fmt:
            if name not in vep_fmt and eb[key].get(name, ""):
                fields.append(name)
        if fields:
            out[feature] = fields
    if not out and [k for k in ea if k in eb] != [k for k in eb if k in ea]:
        out["ORDER"] = ["CSQ_ORDER"]
    return out, nondeterministic


# --------------------------------------------------------------------------
# ledger
# --------------------------------------------------------------------------


def load_ledger(path: Path) -> list[dict[str, str]]:
    with open(path) as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def record_key(row: dict[str, str]) -> RecordKey:
    return (row["contig"], row["pos"], row["ref"], row["alt"], row["clinvar_id"])


def issue_order(issue: str) -> tuple[int, str]:
    number = re.search(r"(\d+)$", issue)
    return (int(number.group(1)) if number else 0, issue)


# --------------------------------------------------------------------------
# annotation
# --------------------------------------------------------------------------

ANNOTATE = (
    "import sys, vepyr; vepyr.annotate(sys.argv[1], sys.argv[2], "
    "reference_fasta=sys.argv[3], output_vcf=sys.argv[4], workers=1, "
    "show_progress=False)"
)


def annotate_contig(
    python: str, panel: Path, cache: str, fasta: str, out: Path, contig: str
) -> float:
    start = time.perf_counter()
    subprocess.run(
        [
            python,
            "-c",
            ANNOTATE,
            str(panel / "in" / f"chr{contig}.vcf"),
            cache,
            fasta,
            str(out),
        ],
        check=True,
    )
    return time.perf_counter() - start


def vepyr_version(python: str) -> str:
    done = subprocess.run(
        [python, "-c", "import importlib.metadata as m; print(m.version('vepyr'))"],
        capture_output=True,
        text=True,
    )
    return done.stdout.strip() if done.returncode == 0 else "unknown"


# --------------------------------------------------------------------------
# progress
# --------------------------------------------------------------------------


def evaluate(panel: Path, vepyr_dir: Path, contigs: list[str]):
    """Per record: (identical, nondeterministic, {feature: fields}) for every
    panel record, keyed by RecordKey."""
    state: dict[RecordKey, tuple[bool, bool, dict[str, list[str]]]] = {}
    for contig in contigs:
        h_in, records = read_vcf(panel / "in" / f"chr{contig}.vcf")
        h_vep, vep = read_vcf(panel / "vep116_0" / f"chr{contig}.vcf")
        h_vy, vy = read_vcf(vepyr_dir / f"chr{contig}.vcf")
        vep_fmt, vy_fmt = csq_format(h_vep), csq_format(h_vy)
        for pos, cid, ref, alt in records:
            key = (contig, pos, ref, alt, cid)
            k = (pos, cid, ref, alt)
            if k not in vy:
                state[key] = (False, False, {"RECORD": ["ONLY_IN_VEP"]})
                continue
            if vep[k] == vy[k]:
                state[key] = (True, False, {})
                continue
            diffs, nondeterministic = entry_diffs(vep[k], vy[k], vep_fmt, vy_fmt)
            state[key] = (not diffs, nondeterministic and not diffs, diffs)
    return state


def read_prev_entries(prev_progress: Path) -> dict[tuple, str] | None:
    """The entries_<label>.tsv written next to a previous progress_<label>.tsv."""
    name = prev_progress.name
    if not name.startswith("progress_"):
        raise SystemExit(f"--prev must be a progress_<label>.tsv, got {name}")
    path = prev_progress.with_name("entries_" + name[len("progress_") :])
    if not path.exists():
        raise SystemExit(f"--prev: {path} is missing")
    with open(path) as fh:
        return {
            (
                r["contig"],
                r["pos"],
                r["ref"],
                r["alt"],
                r["clinvar_id"],
                r["feature"],
            ): r["state"]
            for r in csv.DictReader(fh, delimiter="\t")
        }


def build_progress(ledger, state, prev_progress_rows=None, prev_entries=None):
    """Aggregate per issue and collect the regression list."""
    by_record: dict[RecordKey, list[dict[str, str]]] = collections.defaultdict(list)
    for row in ledger:
        by_record[record_key(row)].append(row)

    issues: dict[str, dict] = {}
    entries_out = []
    regressions = []
    for key, rows in by_record.items():
        excluded = key[:2] in EXCLUDED
        identical, nondeterministic, diffs = state.get(
            key, (False, False, {"RECORD": ["NOT_RUN"]})
        )
        attributed_features = set()
        for row in rows:
            feature = row["feature"]
            attributed_features.add(feature)
            if feature == "-":
                entry_identical = identical
                fields_now = sorted({f for fs in diffs.values() for f in fs})
            else:
                entry_identical = feature not in diffs and "RECORD" not in diffs
                fields_now = diffs.get(feature, [])
            state_now = (
                "excluded"
                if excluded
                else ("identical" if entry_identical else "differs")
            )
            entries_out.append(
                (key, feature, state_now, ",".join(fields_now), row["issues"])
            )
            if excluded:
                continue
            for issue in [i for i in row["issues"].split(",") if i]:
                agg = issues.setdefault(
                    issue,
                    {
                        "repos": set(),
                        "records": set(),
                        "records_identical": set(),
                        "entries": 0,
                        "entries_identical": 0,
                    },
                )
                agg["repos"] |= {r for r in row["repos"].split(",") if r}
                agg["records"].add(key)
                if identical:
                    agg["records_identical"].add(key)
                agg["entries"] += 1
                agg["entries_identical"] += int(entry_identical)
            # fields outside the attributed diff_fields
            attributed_fields = {f for f in row["diff_fields"].split(",") if f}
            if not entry_identical and attributed_fields and feature != "-":
                extra = [f for f in fields_now if f not in attributed_fields]
                if extra:
                    regressions.append(
                        (
                            key,
                            feature,
                            "new fields on an attributed entry",
                            ",".join(extra),
                            row["issues"],
                        )
                    )
        if excluded:
            continue
        # entries that differ now but were not differing in the triage baseline
        if "-" not in attributed_features:
            for feature, fields in diffs.items():
                if feature not in attributed_features and feature != "RECORD":
                    regressions.append(
                        (
                            key,
                            feature,
                            "differs now, identical in the triage baseline",
                            ",".join(fields),
                            "",
                        )
                    )
                    entries_out.append((key, feature, "differs", ",".join(fields), ""))
    if prev_entries is not None:
        for key, feature, state_now, fields, issues_ in entries_out:
            if (
                state_now == "differs"
                and prev_entries.get((*key, feature)) == "identical"
            ):
                regressions.append(
                    (key, feature, "differs now, identical in --prev", fields, issues_)
                )

    prev_identical = {}
    for r in prev_progress_rows or []:
        prev_identical[r["issue"]] = int(r["records_now_identical"])
    table = []
    for issue in sorted(
        issues, key=lambda i: (-len(issues[i]["records"]), issue_order(i))
    ):
        agg = issues[issue]
        now = len(agg["records_identical"])
        table.append(
            {
                "issue": issue,
                "repo": ",".join(sorted(agg["repos"])),
                "records_attributed": len(agg["records"]),
                "records_now_identical": now,
                "entries_attributed": agg["entries"],
                "entries_now_identical": agg["entries_identical"],
                "delta_vs_prev": ""
                if issue not in prev_identical
                else f"{now - prev_identical[issue]:+d}",
                "status": DEFERRED.get(issue, ""),
            }
        )
    return table, entries_out, regressions


def fmt_key(key: RecordKey) -> str:
    contig, pos, ref, alt, cid = key
    short = lambda s: s if len(s) <= 12 else f"{s[:8]}...({len(s)} bp)"  # noqa: E731
    return f"{contig}:{pos} {short(ref)}>{short(alt)} (ClinVar {cid})"


def summary_markdown(label, table, state, ledger, regressions, meta) -> str:
    keys = {record_key(r) for r in ledger}
    judged = [k for k in keys if k[:2] not in EXCLUDED]
    identical = [k for k in judged if state.get(k, (False,))[0]]
    nondeterministic = sorted(k for k in judged if state.get(k, (False, False))[1])
    deferred = sorted(
        {record_key(r) for r in ledger for i in DEFERRED if i in r["issues"].split(",")}
    )
    lines = [
        f"### ClinVar progress panel: `{label}`",
        "",
        f"vepyr `{meta['vepyr_version']}`, cache `{meta['cache']}`, "
        f"{meta['records']:,} panel records ({len(judged):,} judged, {len(keys) - len(judged)} excluded). "
        f"Annotation {meta['annotate_seconds']:.0f} s, total {meta['total_seconds']:.0f} s.",
        "",
        f"**{len(identical):,} / {len(judged):,} records now identical to VEP 116.0**"
        + (
            f" ({len(nondeterministic)} only modulo VEP's random `CLIN_SIG` order, #238)"
            if nondeterministic
            else ""
        )
        + f"; regressions: **{len(regressions)}**.",
        "",
        "| Issue | Repo | Records fixed / attributed | Entries fixed / attributed | Δ records vs prev | Note |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in table:
        lines.append(
            f"| {row['issue'].replace('vepyr', '')} | {row['repo']} | "
            f"{row['records_now_identical']:,} / {row['records_attributed']:,} | "
            f"{row['entries_now_identical']:,} / {row['entries_attributed']:,} | "
            f"{row['delta_vs_prev'] or '–'} | {row['status']} |"
        )
    lines += ["", "Counts overlap: a record can carry several causes."]
    if deferred:
        lines += [
            "",
            "Known VEP bug (deferred, #201): "
            + "; ".join(fmt_key(k) for k in deferred)
            + ".",
        ]
    if nondeterministic:
        lines += [
            "",
            "VEP-nondeterministic `CLIN_SIG` order (#238): "
            + "; ".join(fmt_key(k) for k in nondeterministic)
            + ".",
        ]
    excluded = sorted(k for k in keys if k[:2] in EXCLUDED)
    if excluded:
        lines += [
            "",
            "Excluded (per-record HGNC_ID differs from the full run): "
            + "; ".join(fmt_key(k) for k in excluded)
            + ".",
        ]
    if regressions:
        lines += [
            "",
            f"<details><summary>{len(regressions)} regression(s)</summary>",
            "",
        ]
        for key, feature, kind, fields, _ in regressions[:50]:
            lines.append(f"- {fmt_key(key)} `{feature}`: {kind} ({fields})")
        if len(regressions) > 50:
            lines.append(
                f"- ... {len(regressions) - 50} more in `regressions_{label}.tsv`"
            )
        lines += ["", "</details>"]
    return "\n".join(lines) + "\n"


def write_tsv(path: Path, header: list[str], rows) -> None:
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--python",
        required=True,
        help="Python interpreter of the vepyr build under test.",
    )
    p.add_argument(
        "--cache",
        required=True,
        help="Parquet cache directory or overlay (…/116_GRCh38_merged).",
    )
    p.add_argument(
        "--label",
        required=True,
        help="Round label, e.g. R0-baseline; names every output.",
    )
    p.add_argument(
        "--prev", type=Path, help="progress_<label>.tsv of the previous round."
    )
    p.add_argument("--panel", type=Path, default=DEFAULT_PANEL)
    p.add_argument(
        "--out", type=Path, help="Output directory (default <panel>/progress)."
    )
    p.add_argument("--fasta", default=str(DEFAULT_FASTA))
    p.add_argument(
        "--contigs",
        nargs="+",
        default=CONTIGS,
        help="Subset of panel contigs (1..22, X, MT).",
    )
    p.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Contigs annotated at once (each with workers=1).",
    )
    p.add_argument(
        "--skip-annotate",
        action="store_true",
        help="Reuse <out>/vepyr_<label>/ outputs.",
    )
    args = p.parse_args(argv)

    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.label):
        p.error("--label may only hold letters, digits, '.', '_' and '-'")
    t0 = time.perf_counter()
    out = args.out or args.panel / "progress"
    vepyr_dir = out / f"vepyr_{args.label}"
    vepyr_dir.mkdir(parents=True, exist_ok=True)

    timings: dict[str, float] = {}
    annotate_seconds = 0.0
    if not args.skip_annotate:
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {
                pool.submit(
                    annotate_contig,
                    args.python,
                    args.panel,
                    args.cache,
                    args.fasta,
                    vepyr_dir / f"chr{c}.vcf",
                    c,
                ): c
                for c in args.contigs
            }
            for future in concurrent.futures.as_completed(futures):
                timings[f"chr{futures[future]}"] = round(future.result(), 2)
                print(
                    f"  annotated chr{futures[future]} in {timings[f'chr{futures[future]}']:.1f} s",
                    flush=True,
                )
        annotate_seconds = time.perf_counter() - start

    ledger = [
        r for r in load_ledger(args.panel / "ledger.tsv") if r["contig"] in args.contigs
    ]
    state = evaluate(args.panel, vepyr_dir, args.contigs)
    prev_rows = None
    prev_entries = None
    if args.prev:
        with open(args.prev) as fh:
            prev_rows = list(csv.DictReader(fh, delimiter="\t"))
        prev_entries = read_prev_entries(args.prev)
    table, entries, regressions = build_progress(ledger, state, prev_rows, prev_entries)

    columns = [
        "issue",
        "repo",
        "records_attributed",
        "records_now_identical",
        "entries_attributed",
        "entries_now_identical",
        "delta_vs_prev",
        "status",
    ]
    write_tsv(
        out / f"progress_{args.label}.tsv",
        columns,
        ([r[c] for c in columns] for r in table),
    )
    write_tsv(
        out / f"entries_{args.label}.tsv",
        [
            "contig",
            "pos",
            "ref",
            "alt",
            "clinvar_id",
            "feature",
            "state",
            "fields_now",
            "issues",
        ],
        ([*k, f, s, fl, i] for k, f, s, fl, i in entries),
    )
    write_tsv(
        out / f"regressions_{args.label}.tsv",
        [
            "contig",
            "pos",
            "ref",
            "alt",
            "clinvar_id",
            "feature",
            "kind",
            "fields",
            "issues",
        ],
        ([*k, f, kind, fl, i] for k, f, kind, fl, i in regressions),
    )
    meta = {
        "label": args.label,
        "vepyr_version": vepyr_version(args.python),
        "python": args.python,
        "cache": args.cache,
        "panel": str(args.panel),
        "prev": str(args.prev) if args.prev else None,
        "contigs": args.contigs,
        "records": len({record_key(r) for r in ledger}),
        "annotate_seconds": round(annotate_seconds, 1),
        "annotate_seconds_per_contig": timings,
        "total_seconds": round(time.perf_counter() - t0, 1),
    }
    (out / f"summary_{args.label}.md").write_text(
        summary_markdown(args.label, table, state, ledger, regressions, meta)
    )
    (out / f"run_{args.label}.json").write_text(json.dumps(meta, indent=1) + "\n")
    print((out / f"summary_{args.label}.md").read_text())
    return 0


if __name__ == "__main__":
    sys.exit(main())
