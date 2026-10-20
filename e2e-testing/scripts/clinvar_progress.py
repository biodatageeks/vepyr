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
  entries_<label>.tsv      per (record, feature): its state and differing fields
                           now, for ledger features, unledgered ones and RECORD
                           rows; read back by --prev
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

``--full-contig`` validates against the full Ensembl VEP 116.0 ClinVar golden
instead of the panel: every ClinVar record of the ``--contigs`` (ALT="."
records dropped, as the golden's input did) is annotated and compared with
the golden record for record. The issue table covers the panel's records of
those contigs, judged in full-contig context, so nothing is excluded. The
regression list also covers every record that was identical before: in the
``--prev`` run (use a full-contig run of the vepyr master build as the
baseline), or else in the triage (any differing record not in the ledger).
Only differing records are kept (``records_<label>.tsv``); the input slice and
the vepyr output are deleted after the comparison unless ``--keep-outputs``.

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
import gzip
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

DATA = Path(os.environ.get("DATA_VEPYR_DIR", Path.home() / "workspace" / "data_vepyr"))
DEFAULT_PANEL = DATA / "clinvar_panel"
DEFAULT_FASTA = DATA / "input" / "Homo_sapiens.GRCh38.dna.primary_assembly.fa"
DEFAULT_CLINVAR = DATA / "input" / "clinvar" / "clinvar_20261011.vcf.gz"
DEFAULT_GOLDEN = (
    DATA
    / "output"
    / "116"
    / "clinvar_20261011_annotated_wgs_everything_hgvs_merged.vcf.gz"
)
CONTIGS = [str(n) for n in range(1, 23)] + ["X", "MT"]

# (contig, pos): per-record annotation gives a different HGNC_ID than the full
# run (e2e/clinvar-parity README), so the panel cannot judge these records.
EXCLUDED = {("20", "63674046"), ("7", "117544096")}
# Issue whose fix is deferred because the difference is a confirmed VEP bug.
DEFERRED = {"vepyr#201": "known VEP bug (deferred)"}
# Ledger pseudo-issues: differences the panel itself causes, not vepyr. VEP
# fills HGNC_ID on some merged RefSeq transcripts differently when a record is
# annotated alone than in the full run that made the golden.
NOTES = {
    **DEFERRED,
    "panel-context:HGNC_ID": "panel artifact: HGNC_ID depends on the input context",
}

# vepyr#238 (merged as f4ab545): the CLIN_SIG rule is shared with the field
# comparison in comparison.compare, so the panel and the HG002 gates cannot
# disagree on what counts as VEP's random %3B order.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from comparison.compare import (  # noqa: E402
    _clin_sig_order_equivalent,
    _pairing_value,
)

# Per-entry key: values beyond (or short of) the header's Format, as
# "<offset>|<surplus values>", so a malformed entry cannot pass as identical.
ARITY = "\0arity"

# Pseudo features that make every entry of a record differ.
RECORD_WIDE = {"RECORD", "CSQ_FORMAT", "CSQ_FIELD_ORDER"}

RecordKey = tuple[str, str, str, str, str]  # contig, pos, ref, alt, clinvar_id


# --------------------------------------------------------------------------
# VCF parsing and comparison
# --------------------------------------------------------------------------


def read_vcf(path: Path) -> tuple[list[str], dict[tuple[str, str, str, str], str]]:
    """Header lines and {(pos, id, ref, alt): line} of a plain-text VCF."""
    header: list[str] = []
    body: dict[tuple[str, str, str, str], str] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("#"):
                header.append(line)
                continue
            cols = line.split("\t", 5)
            key = (cols[1], cols[2], cols[3], cols[4])
            if key in body:
                raise SystemExit(f"{path}: duplicate record {key}")
            body[key] = line.rstrip("\n")
    return header, body


def csq_format(header: list[str]) -> list[str]:
    for line in header:
        if line.startswith("##INFO=<ID=CSQ"):
            match = re.search(r'Format: ([^"]+)"', line)
            if match:
                return match.group(1).split("|")
    raise ValueError("no ##INFO=<ID=CSQ ... Format: ...> header line")


def split_record(line: str, fmt: list[str]):
    """(non-CSQ columns, [((allele, feature), {field: value})]) of one record.

    Entries keep their CSQ order. Several entries can share an (Allele,
    Feature) key; `entry_diffs` pairs those by payload.
    """
    cols = line.split("\t")
    rest, csq = [], []
    for kv in cols[7].split(";"):
        if kv.startswith("CSQ="):
            csq = kv[4:].split(",")
            rest.append("CSQ")  # keeps CSQ's position in the INFO comparison
        else:
            rest.append(kv)
    fi, ai = fmt.index("Feature"), fmt.index("Allele")
    entries = []
    for raw in csq:
        values = raw.split("|")
        entry = dict(zip(fmt, values))
        # A malformed entry may end before Allele or Feature: key it with
        # empty values rather than fail, and let CSQ_ARITY report it.
        ident = values + [""] * (max(fi, ai) + 1 - len(values))
        # zip drops surplus values; keep the count so a malformed entry differs.
        entry[ARITY] = f"{len(values) - len(fmt)}|" + "|".join(values[len(fmt) :])
        entries.append(((ident[ai], ident[fi]), entry))
    non_csq = "\t".join(cols[:7] + [";".join(rest)] + cols[8:])
    return non_csq, entries


def _pair_group(left, right, fields):
    """Pair two lists of (index, entry) sharing one key.

    Equal payloads (CLIN_SIG compared canonically) pair first, so a
    reordering of duplicates is not read as field changes; the rest pair by
    similarity. Returns (pairs, left-only, right-only).
    """

    def payload(entry):
        return tuple(_pairing_value(n, entry.get(n, "")) for n in fields)

    remaining = list(right)
    pairs, left_rest = [], []
    for item in left:
        match = next((r for r in remaining if payload(r[1]) == payload(item[1])), None)
        if match is None:
            left_rest.append(item)
        else:
            remaining.remove(match)
            pairs.append((item, match))

    # The rest pair by similarity: the assignment with the most equal fields
    # over the whole group (ties keep CSQ order), so changed duplicates that
    # also moved are not cross-paired. Groups are tiny; larger ones fall back
    # to a greedy pass.
    def score(a, b):
        return sum(
            _pairing_value(n, a[1].get(n, "")) == _pairing_value(n, b[1].get(n, ""))
            for n in fields
        )

    k = min(len(left_rest), len(remaining))
    chosen_pairs = []
    if k and max(len(left_rest), len(remaining)) <= 6:
        # Permute the larger side, so either side may keep its unmatched ones.
        if len(left_rest) <= len(remaining):
            best = max(
                itertools.permutations(range(len(remaining)), k),
                key=lambda perm: (
                    sum(score(left_rest[i], remaining[p]) for i, p in enumerate(perm)),
                    [-p for p in perm],
                ),
            )
            chosen_pairs = [(left_rest[i], remaining[p]) for i, p in enumerate(best)]
        else:
            best = max(
                itertools.permutations(range(len(left_rest)), k),
                key=lambda perm: (
                    sum(score(left_rest[p], remaining[i]) for i, p in enumerate(perm)),
                    [-p for p in perm],
                ),
            )
            chosen_pairs = [(left_rest[p], remaining[i]) for i, p in enumerate(best)]
    elif k:
        pool = list(remaining)
        for item in left_rest[:k]:
            pick = max(pool, key=lambda r: (score(item, r), -r[0]))
            pool.remove(pick)
            chosen_pairs.append((item, pick))
    pairs += chosen_pairs
    unpaired = [a for a in left_rest if all(a is not x for x, _ in chosen_pairs)]
    remaining = [b for b in remaining if all(b is not y for _, y in chosen_pairs)]
    return pairs, unpaired, remaining


def entry_diffs(
    vep_line: str, vepyr_line: str, vep_fmt: list[str], vepyr_fmt: list[str]
) -> tuple[dict[str, list[str]], bool]:
    """Differing entries of one record, as {feature: [fields]}, and whether a
    CLIN_SIG value matched only after sorting (VEP-nondeterministic).

    Pseudo features: ``INFO`` for the non-CSQ columns, ``ORDER`` when shared
    entries come in another order, ``CSQ_FORMAT`` / ``CSQ_FIELD_ORDER`` when
    the two CSQ headers declare other fields or another field order.
    """
    rest_a, la = split_record(vep_line, vep_fmt)
    rest_b, lb = split_record(vepyr_line, vepyr_fmt)
    out: dict[str, list[str]] = {}
    nondeterministic = False

    def add(feature, fields):
        out.setdefault(feature, [])
        out[feature] += [f for f in fields if f not in out[feature]]

    if rest_a != rest_b:
        add("INFO", ["INFO"])
    # A field one header declares and the other does not is a difference on
    # every record, even where VEP's value is empty; so is another field order.
    schema = sorted(set(vep_fmt) ^ set(vepyr_fmt))
    if schema:
        add("CSQ_FORMAT", schema)
    elif vep_fmt != vepyr_fmt:
        add("CSQ_FIELD_ORDER", ["CSQ_FIELD_ORDER"])
    groups_a: dict = collections.defaultdict(list)
    groups_b: dict = collections.defaultdict(list)
    for n, (key, entry) in enumerate(la):
        groups_a[key].append((n, entry))
    for n, (key, entry) in enumerate(lb):
        groups_b[key].append((n, entry))
    matched = []
    for key in list(groups_a) + [k for k in groups_b if k not in groups_a]:
        feature = key[1]
        pairs, only_a, only_b = _pair_group(
            groups_a.get(key, []), groups_b.get(key, []), vep_fmt
        )
        if only_a:
            add(feature, ["ONLY_IN_VEP"])
        if only_b:
            add(feature, ["ONLY_IN_VEPYR"])
        for (ia, a_entry), (ib, b_entry) in pairs:
            matched.append((ia, ib))
            fields = []
            for name in vep_fmt:
                a, b = a_entry.get(name, ""), b_entry.get(name, "")
                if a == b:
                    continue
                if _clin_sig_order_equivalent(name, b, a):
                    nondeterministic = True
                    continue
                fields.append(name)
            # Surplus or missing values relative to each side's own header.
            if a_entry.get(ARITY) != b_entry.get(ARITY):
                fields.append("CSQ_ARITY")
            # Per-entry detail for a vepyr-only field (CSQ_FORMAT names it per record).
            for name in vepyr_fmt:
                if name not in vep_fmt and b_entry.get(name, ""):
                    fields.append(name)
            if fields:
                add(feature, fields)
    # Independent of field differences: a reordering is a difference too.
    order = [ib for _, ib in sorted(matched)]
    if order != sorted(order):
        add("ORDER", ["CSQ_ORDER"])
    return out, nondeterministic


# --------------------------------------------------------------------------
# ledger
# --------------------------------------------------------------------------


def load_ledger(path: Path) -> list[dict[str, str]]:
    """The ledger rows. Progress is tracked per (record, feature): the ledger
    has one row per differing feature, and a record never carries two CSQ
    entries for one feature (one ALT per ClinVar record, one VEP entry per
    feature and allele). A ledger that breaks this is refused rather than
    miscounted."""
    with open(path, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    seen = set()
    for r in rows:
        key = (r["contig"], r["pos"], r["ref"], r["alt"], r["clinvar_id"], r["feature"])
        if key in seen and r["feature"] not in ("-", "INFO", "ORDER", "RECORD"):
            raise SystemExit(f"{path}: two ledger rows for one entry {key}")
        seen.add(key)
    return rows


def record_key(row: dict[str, str]) -> RecordKey:
    return (row["contig"], row["pos"], row["ref"], row["alt"], row["clinvar_id"])


def normalize_issue(issue: str) -> str:
    """'211', '#211' and 'vepyr#211' name the same issue; other repos keep their prefix."""
    issue = issue.strip()
    if re.fullmatch(r"#?\d+", issue):
        return f"vepyr#{issue.lstrip('#')}"
    return issue


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


ANNOTATE_FULL = (
    "import sys, vepyr; vepyr.annotate(sys.argv[1], sys.argv[2], "
    "reference_fasta=sys.argv[3], output_vcf=sys.argv[4], workers=int(sys.argv[5]), "
    "compression='bgzf', show_progress=False)"
)


def annotate_full_contig(
    python: str,
    clinvar: str,
    cache: str,
    fasta: str,
    out: Path,
    contig: str,
    workers: int,
) -> float:
    """Annotate every ClinVar record of `contig`, as the golden's input held it.

    Same preprocessing as the golden's input (e2e/clinvar-parity README,
    section 2): ALT="." records removed, then `bcftools norm -m -both` with no
    left-alignment. ClinVar 20261011 has no multiallelic record, so the split
    is a no-op there, but it keeps the two inputs on one procedure.
    """
    start = time.perf_counter()
    sliced = out.with_name(f"chr{contig}.in.vcf.gz")
    view = subprocess.Popen(
        ["bcftools", "view", "-e", 'ALT="."', "-r", contig, clinvar, "-Ou"],
        stdout=subprocess.PIPE,
    )
    try:
        norm = subprocess.run(
            ["bcftools", "norm", "-m", "-both", "-Oz", "-o", str(sliced)],
            stdin=view.stdout,
        )
    except BaseException:
        view.kill()
        raise
    finally:
        view.stdout.close()
    if view.wait() != 0 or norm.returncode != 0:
        sliced.unlink(missing_ok=True)
        raise SystemExit(f"slicing chr{contig} from {clinvar} failed")
    # annotate(workers > 1) needs a tabix-indexed input.
    subprocess.run(["tabix", "-f", "-p", "vcf", str(sliced)], check=True)
    try:
        subprocess.run(
            [
                python,
                "-c",
                ANNOTATE_FULL,
                str(sliced),
                cache,
                fasta,
                str(out),
                str(workers),
            ],
            check=True,
        )
    finally:
        sliced.unlink(missing_ok=True)
        Path(str(sliced) + ".tbi").unlink(missing_ok=True)
    return time.perf_counter() - start


def _position_groups(lines):
    """Yield (pos, {(pos, id, ref, alt): line}) per position of a sorted VCF body."""
    group: dict[tuple[str, str, str, str], str] = {}
    current = None
    for line in lines:
        if line.startswith("#"):
            continue
        line = line.rstrip("\n")
        cols = line.split("\t", 5)
        pos = int(cols[1])
        if current is not None and pos != current:
            if pos < current:
                raise SystemExit(f"VCF body is not sorted at position {pos}")
            yield current, group
            group = {}
        current = pos
        key = (cols[1], cols[2], cols[3], cols[4])
        if key in group:
            raise SystemExit(f"duplicate record {key}")
        group[key] = line
    if current is not None:
        yield current, group


def evaluate_full(contig: str, golden: str, vepyr_vcf: Path, keep: set):
    """Compare every record of one contig with the golden, streaming both.

    Returns (state for differing records and for every key in `keep`,
    counts). State has the same shape as `evaluate`.
    """
    header = subprocess.run(
        ["bcftools", "view", "-h", golden], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    gold_fmt = csq_format(header)
    vy_header = []
    with gzip.open(vepyr_vcf, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            vy_header.append(line)
    vy_fmt = csq_format(vy_header)
    stderr = tempfile.TemporaryFile(mode="w+")
    tabix = subprocess.Popen(
        ["tabix", golden, contig], stdout=subprocess.PIPE, stderr=stderr, text=True
    )
    state: dict[RecordKey, tuple[bool, bool, dict[str, list[str]]]] = {}
    counts = collections.Counter()
    try:
        with gzip.open(vepyr_vcf, "rt", encoding="utf-8") as vy_fh:
            gold_groups = _position_groups(tabix.stdout)
            vy_groups = _position_groups(vy_fh)
            g = next(gold_groups, None)
            v = next(vy_groups, None)
            while g is not None or v is not None:
                if v is None or (g is not None and g[0] < v[0]):
                    gold, vy = g[1], {}
                    g = next(gold_groups, None)
                elif g is None or v[0] < g[0]:
                    gold, vy = {}, v[1]
                    v = next(vy_groups, None)
                else:
                    gold, vy = g[1], v[1]
                    g, v = next(gold_groups, None), next(vy_groups, None)
                for k in list(gold) + [k for k in vy if k not in gold]:
                    key = (contig, k[0], k[2], k[3], k[1])
                    counts["records"] += 1
                    if k not in vy:
                        counts["only_in_golden"] += 1
                        state[key] = (False, False, {"RECORD": ["ONLY_IN_VEP"]})
                        continue
                    if k not in gold:
                        counts["only_in_vepyr"] += 1
                        state[key] = (False, False, {"RECORD": ["ONLY_IN_VEPYR"]})
                        continue
                    if gold[k] == vy[k] and gold_fmt == vy_fmt:
                        counts["identical"] += 1
                        if key in keep:
                            state[key] = (True, False, {})
                        continue
                    diffs, nondeterministic = entry_diffs(
                        gold[k], vy[k], gold_fmt, vy_fmt
                    )
                    if diffs:
                        counts["differ"] += 1
                    else:
                        counts["identical"] += 1
                        counts["vep_nondeterministic"] += int(nondeterministic)
                    if diffs or nondeterministic or key in keep:
                        state[key] = (not diffs, nondeterministic and not diffs, diffs)
    except BaseException:
        # Only a failed comparison stops tabix early; on success it has
        # already reached EOF and is waited on below.
        tabix.stdout.close()
        tabix.terminate()
        tabix.wait()
        stderr.close()
        raise
    tabix.stdout.close()
    code = tabix.wait()
    if code != 0:
        stderr.seek(0)
        raise SystemExit(
            f"tabix {golden} {contig} failed ({code}): {stderr.read().strip()}"
        )
    stderr.close()
    return state, counts


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
            if k not in vep:
                raise SystemExit(
                    f"{panel / 'vep116_0' / f'chr{contig}.vcf'} has no line for "
                    f"chr{contig}:{pos} {cid}; rebuild the panel"
                )
            if k not in vy:
                state[key] = (False, False, {"RECORD": ["ONLY_IN_VEP"]})
                continue
            if vep[k] == vy[k] and vep_fmt == vy_fmt:
                # Byte-identical lines mean the same values only when both
                # headers declare the same CSQ field order.
                state[key] = (True, False, {})
                continue
            diffs, nondeterministic = entry_diffs(vep[k], vy[k], vep_fmt, vy_fmt)
            state[key] = (not diffs, nondeterministic and not diffs, diffs)
        # A record vepyr wrote that the panel input does not hold.
        for pos, cid, ref, alt in vy:
            if (pos, cid, ref, alt) not in records:
                state[(contig, pos, ref, alt, cid)] = (
                    False,
                    False,
                    {"RECORD": ["ONLY_IN_VEPYR"]},
                )
    return state


def _file_digest(path, cache_file: Path | None = None) -> dict:
    """Size and full sha256 of a large input (the golden is 5.9 GB).

    The digest is cached by (real path, size, mtime_ns) in `cache_file`, so it
    is computed once per file version: rewriting the file changes its mtime
    and forces a new hash, and a copy elsewhere hashes to the same value.
    The cache trusts size and mtime: a same-size rewrite that restores the
    mtime (cp -p, rsync -t, touch -r) would keep the old digest. It is never
    pruned, and concurrent runs that both miss may drop each other's new
    entry, which only costs a rehash later.
    """
    st = os.stat(path)
    key = f"{os.path.realpath(path)}:{st.st_size}:{st.st_mtime_ns}"
    cache = {}
    if cache_file is not None and cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text(encoding="utf-8"))
        except ValueError:
            cache = {}
    if key not in cache:
        print(
            f"  hashing {path} ({st.st_size / 1e9:.1f} GB, once per file version)",
            file=sys.stderr,
            flush=True,
        )
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 24), b""):
                digest.update(block)
        cache[key] = digest.hexdigest()
        if cache_file is not None:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = cache_file.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(cache, indent=1) + "\n", encoding="utf-8")
            tmp.replace(cache_file)
    return {"size": st.st_size, "sha256": cache[key]}


def input_identity(
    panel: Path, mode: str, clinvar: Path, golden: Path, fasta: Path | None = None
) -> dict:
    """What a run compared against, so reused outputs and --prev can be checked.

    Panel mode: sha256 over ledger.tsv, in/ and vep116_0/ (about 400 MB, ~1 s).
    Full-contig mode: the ledger digest plus the full sha256 of the ClinVar
    release and the golden. Both modes add the reference FASTA when it
    exists. Large-file digests are cached (see `_file_digest`).
    """
    digest = hashlib.sha256()
    files = [panel / "ledger.tsv"]
    if mode == "panel":
        files += sorted((panel / "in").glob("*.vcf")) + sorted(
            (panel / "vep116_0").glob("*.vcf")
        )
    for path in files:
        digest.update(path.name.encode())
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                digest.update(block)
    identity = {"mode": mode, "panel_sha256": digest.hexdigest()}
    cache_file = panel / "build" / "digest_cache.json"
    if fasta is not None and os.path.exists(fasta):
        # The reference changes annotation as much as the inputs do.
        identity["fasta"] = _file_digest(fasta, cache_file)
    if mode == "full-contig":
        for name, path in (("clinvar", clinvar), ("golden", golden)):
            identity[name] = _file_digest(path, cache_file)
    return identity


def _identity_mismatch(recorded, identity, what: str) -> str | None:
    if recorded is None:
        return (
            f"{what} has no input_identity recorded (made before it existed); re-run it"
        )
    if recorded != identity:
        return f"{what} compared against another panel or golden; deltas would be meaningless"
    return None


def check_prev_scope(
    prev_progress: Path,
    contigs: list[str],
    mode: str = "panel",
    identity: dict | None = None,
) -> None:
    """Deltas only mean something over the same contigs and the same mode."""
    label = prev_progress.name[len("progress_") : -len(".tsv")]
    run = prev_progress.with_name(f"run_{label}.json")
    if not run.exists():
        raise SystemExit(f"--prev: {run} is missing, so its contig scope is unknown")
    prev_run = json.loads(run.read_text(encoding="utf-8"))
    if prev_run.get("mode", "panel") != mode:
        raise SystemExit(
            f"--prev is a {prev_run.get('mode', 'panel')} run, this run is {mode}"
        )
    if identity is not None:
        problem = _identity_mismatch(prev_run.get("input_identity"), identity, "--prev")
        if problem:
            raise SystemExit(problem)
    prev_contigs = prev_run.get("contigs") or []
    # A wider --prev is fine (a targeted run against the full R0): deltas are
    # then printed only for issues whose records all lie in this run.
    if not set(contigs) <= set(prev_contigs):
        raise SystemExit(
            f"--prev covers contigs {prev_contigs}, this run {contigs}; "
            "deltas need --prev to cover every contig of this run (same --contigs)"
        )


def read_prev_entries(prev_progress: Path) -> dict[tuple, str] | None:
    """The entries_<label>.tsv written next to a previous progress_<label>.tsv."""
    name = prev_progress.name
    if not name.startswith("progress_"):
        raise SystemExit(f"--prev must be a progress_<label>.tsv, got {name}")
    path = prev_progress.with_name("entries_" + name[len("progress_") :])
    if not path.exists():
        raise SystemExit(f"--prev: {path} is missing")
    with open(path, encoding="utf-8") as fh:
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


def read_prev_fields(prev_progress: Path) -> dict[tuple, set]:
    """Differing fields per entry of the previous run (entries_<label>.tsv)."""
    path = prev_progress.with_name("entries_" + prev_progress.name[len("progress_") :])
    with open(path, encoding="utf-8") as fh:
        return {
            (
                r["contig"],
                r["pos"],
                r["ref"],
                r["alt"],
                r["clinvar_id"],
                r["feature"],
            ): {f for f in r["fields_now"].split(",") if f}
            for r in csv.DictReader(fh, delimiter="\t")
        }


def build_progress(
    ledger,
    state,
    prev_progress_rows=None,
    prev_entries=None,
    excluded=EXCLUDED,
    prev_fields=None,
):
    """Aggregate per issue and collect the regression list."""
    by_record: dict[RecordKey, list[dict[str, str]]] = collections.defaultdict(list)
    for row in ledger:
        by_record[record_key(row)].append(row)

    issues: dict[str, dict] = {}
    entries_out = []
    regressions = []
    for key, rows in by_record.items():
        is_excluded = key[:2] in excluded
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
                # A record-wide difference (missing record, other CSQ schema or
                # field order) leaves no entry of the record identical.
                entry_identical = feature not in diffs and not (
                    RECORD_WIDE & set(diffs)
                )
                fields_now = diffs.get(feature, [])
            state_now = (
                "excluded"
                if is_excluded
                else ("identical" if entry_identical else "differs")
            )
            entries_out.append(
                (key, feature, state_now, ",".join(fields_now), row["issues"])
            )
            if is_excluded:
                continue
            row_issues = [i for i in row["issues"].split(",") if i]
            row_repos = row["repos"].split(",")
            for n, issue in enumerate(row_issues):
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
                # repos is aligned with issues; one issue's repos joined by "+".
                # A ledger built before the columns were aligned has one union
                # per row, and every issue on such a row then gets that union.
                repo = (
                    row_repos[n] if len(row_repos) == len(row_issues) else row["repos"]
                )
                agg["repos"] |= {r for r in re.split(r"[+,]", repo) if r and r != "-"}
                agg["records"].add(key)
                if identical:
                    agg["records_identical"].add(key)
                agg["entries"] += 1
                agg["entries_identical"] += int(entry_identical)
            # fields outside the attributed diff_fields
            attributed_fields = {f for f in row["diff_fields"].split(",") if f}
            if not entry_identical and attributed_fields and feature != "-":
                extra = [f for f in fields_now if f not in attributed_fields]
                # Under --prev, only fields that did not already differ there.
                if prev_fields is not None:
                    extra = [
                        f
                        for f in extra
                        if f not in prev_fields.get((*key, feature), set())
                    ]
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
        if is_excluded:
            continue
        if "RECORD" in diffs:
            # Kept as an entry row, so the next --prev run can tell a record
            # that was already missing from one that just went missing.
            entries_out.append(
                (key, "RECORD", "differs", ",".join(diffs["RECORD"]), "")
            )
        if "RECORD" in diffs and (
            prev_entries is None or prev_entries.get((*key, "RECORD")) != "differs"
        ):
            regressions.append(
                (
                    key,
                    "RECORD",
                    "record missing from one side",
                    ",".join(diffs["RECORD"]),
                    "",
                )
            )
        # entries that differ now but were not differing in the triage baseline.
        # A "-" row is a record-level placeholder, not a claim that every
        # feature differed, so it covers no feature here.
        # With --prev the baseline is the previous run: an entry that already
        # differed there is persistent, not new, and is left to that report.
        for feature, fields in diffs.items():
            if feature not in attributed_features and feature != "RECORD":
                entries_out.append((key, feature, "differs", ",".join(fields), ""))
                if prev_entries is None:
                    kind = "differs now, identical in the triage baseline"
                elif prev_entries.get((*key, feature)) == "differs":
                    continue
                else:
                    kind = "differs now, identical in --prev"
                regressions.append((key, feature, kind, ",".join(fields), ""))
    if prev_entries is not None:
        # One row per regressed entry: an entry already reported against the
        # triage baseline is not reported again against --prev.
        reported = {(r[0], r[1]) for r in regressions}
        # A record-wide difference (missing record, CSQ schema or field order)
        # already has its own row; its entries are not reported one by one.
        one_sided = {k for k, (_, _, d) in state.items() if RECORD_WIDE & set(d)}
        for key, feature, state_now, fields, issues_ in entries_out:
            if (
                state_now == "differs"
                and key not in one_sided
                and prev_entries.get((*key, feature)) == "identical"
                and (key, feature) not in reported
            ):
                regressions.append(
                    (key, feature, "differs now, identical in --prev", fields, issues_)
                )
                reported.add((key, feature))

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
                "status": NOTES.get(issue, ""),
            }
        )
    return table, entries_out, regressions


def fmt_key(key: RecordKey) -> str:
    contig, pos, ref, alt, cid = key
    short = lambda s: s if len(s) <= 12 else f"{s[:8]}...({len(s)} bp)"  # noqa: E731
    return f"{contig}:{pos} {short(ref)}>{short(alt)} (ClinVar {cid})"


def summary_markdown(
    label, table, state, ledger, regressions, meta, excluded=EXCLUDED
) -> str:
    selected = set(meta.get("issues") or [])
    # With --issues the headline counts only those issues' records.
    keys = {
        record_key(r)
        for r in ledger
        if not selected or selected & set(r["issues"].split(","))
    }
    judged = [k for k in keys if k[:2] not in excluded]
    identical = [k for k in judged if state.get(k, (False,))[0]]
    nondeterministic = sorted(k for k in judged if state.get(k, (False, False))[1])
    # The headline counts only judged records; full-contig mode also lists
    # every CLIN_SIG-only match of the contigs, in the ledger or not.
    contig_wide = (
        sorted(k for k, v in state.items() if v[1])
        if meta.get("mode") == "full-contig"
        else nondeterministic
    )
    deferred = sorted(
        {
            record_key(r)
            for r in ledger
            for i in DEFERRED
            if i in r["issues"].split(",") and (not selected or i in selected)
        }
    )
    full = meta.get("mode") == "full-contig"
    lines = [
        f"### ClinVar progress {'(full contigs vs the VEP 116.0 golden)' if full else 'panel'}: `{label}`",
        "",
        f"vepyr `{meta['vepyr_version']}`, cache `{meta['cache']}`, "
        f"{meta['records']:,} {'selected records' if selected else 'ledger records of these contigs' if full else 'panel records'} "
        f"({len(judged):,} judged, {len(keys) - len(judged)} excluded). "
        + (
            f"Annotation {meta['annotate_seconds']:.0f} s (reused from the earlier run), "
            f"this comparison {meta['total_seconds']:.0f} s."
            if meta.get("annotation_reused")
            else f"Annotation {meta['annotate_seconds']:.0f} s, total {meta['total_seconds']:.0f} s."
        ),
        "",
        f"**{len(identical):,} / {len(judged):,} records now identical to VEP 116.0**"
        + (
            f" ({len(nondeterministic)} only modulo VEP's random `CLIN_SIG` order, #238)"
            if nondeterministic
            else ""
        )
        + f"; regressions: **{len(regressions)}**.",
        "",
    ]
    if full:
        lines += [
            "| Contig | ClinVar records | Identical to golden | Differ | Only in golden / vepyr |",
            "|---|---:|---:|---:|---:|",
        ]
        for contig, c in meta["full_contig_counts"].items():
            lines.append(
                f"| chr{contig} | {c.get('records', 0):,} | {c.get('identical', 0):,} | "
                f"{c.get('differ', 0):,} | {c.get('only_in_golden', 0)} / {c.get('only_in_vepyr', 0)} |"
            )
        lines += [""]
    lines += [
        "| Issue | Repo | Records fixed / attributed | Entries fixed / attributed | Records vs prev | Note |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in [r for r in table if r["issue"] not in NOTES]:
        lines.append(
            f"| {row['issue'].replace('vepyr', '')} | {row['repo']} | "
            f"{row['records_now_identical']:,} / {row['records_attributed']:,} | "
            f"{row['entries_now_identical']:,} / {row['entries_attributed']:,} | "
            f"{row['delta_vs_prev'] or '-'} | {row['status']} |"
        )
    lines += ["", "Counts overlap: a record can carry several causes."]
    if meta.get("partial_issues"):
        lines += [
            "",
            "**Partial:** this run covers only some contigs of "
            + ", ".join(i.replace("vepyr", "") for i in meta["partial_issues"])
            + "; their counts are not the whole issue.",
        ]
    not_targets = [r for r in table if r["issue"] in NOTES]
    if not_targets:
        lines += [
            "",
            "Not targets (kept out of the table above; rows stay in the TSV):",
        ]
        for row in not_targets:
            lines.append(
                f"- {row['issue'].replace('vepyr', '')}: {row['status']}; "
                f"{row['records_now_identical']} / {row['records_attributed']} records identical"
            )
    if deferred:
        lines += [
            "",
            "Known VEP bug (deferred, #201): "
            + "; ".join(fmt_key(k) for k in deferred)
            + ".",
        ]
    if contig_wide:
        lines += [
            "",
            "VEP-nondeterministic `CLIN_SIG` order (#238)"
            + (
                f", all {len(contig_wide)} on these contigs"
                if contig_wide != nondeterministic
                else ""
            )
            + ": "
            + "; ".join(fmt_key(k) for k in contig_wide)
            + ".",
        ]
    left_out = sorted(k for k in keys if k[:2] in excluded)
    if left_out:
        lines += [
            "",
            "Excluded (per-record HGNC_ID differs from the full run): "
            + "; ".join(fmt_key(k) for k in left_out)
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
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _annotate_all(args, vepyr_dir: Path) -> tuple[dict[str, float], float]:
    timings: dict[str, float] = {}
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as pool:
        if args.full_contig:
            futures = {
                pool.submit(
                    annotate_full_contig,
                    args.python,
                    str(args.clinvar),
                    args.cache,
                    args.fasta,
                    vepyr_dir / f"chr{c}.vcf.gz",
                    c,
                    args.workers,
                ): c
                for c in args.contigs
            }
        else:
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
            name = f"chr{futures[future]}"
            timings[name] = round(future.result(), 2)
            print(f"  annotated {name} in {timings[name]:.1f} s", flush=True)
    return timings, time.perf_counter() - start


def read_prev_records(prev_progress: Path) -> set[RecordKey]:
    """Differing records of a previous full-contig run (records_<label>.tsv)."""
    label = prev_progress.name[len("progress_") : -len(".tsv")]
    path = prev_progress.with_name(f"records_{label}.tsv")
    if not path.exists():
        raise SystemExit(f"--prev: {path} is missing (not a --full-contig run?)")
    with open(path, encoding="utf-8") as fh:
        return {
            (r["contig"], r["pos"], r["ref"], r["alt"], r["clinvar_id"])
            for r in csv.DictReader(fh, delimiter="\t")
            if r["state"] == "differs"
        }


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
        help="Parquet cache directory or overlay (.../116_GRCh38_merged).",
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
        help="Contigs to run (1..22, X, MT). Default: the whole panel; "
        "required with --full-contig.",
    )
    p.add_argument(
        "--issues",
        nargs="+",
        help="Report only these issues (211, vepyr#211, functions#143). Without "
        "--contigs, runs only the contigs that hold their records.",
    )
    p.add_argument(
        "--full-contig",
        action="store_true",
        help="Annotate every ClinVar record of --contigs and compare with the "
        "full VEP 116.0 golden instead of the panel.",
    )
    p.add_argument("--clinvar", type=Path, default=DEFAULT_CLINVAR)
    p.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN)
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="vepyr workers per contig in --full-contig mode (the panel always uses 1).",
    )
    p.add_argument(
        "--keep-outputs",
        action="store_true",
        help="--full-contig: keep the vepyr output (deleted after the comparison by default).",
    )
    p.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Contigs annotated at once.",
    )
    p.add_argument(
        "--skip-annotate",
        action="store_true",
        help="Reuse <out>/vepyr_<label>/ outputs. With --full-contig they exist "
        "only if that run used --keep-outputs.",
    )
    args = p.parse_args(argv)

    if not re.fullmatch(r"[A-Za-z0-9._-]+", args.label):
        p.error("--label may only hold letters, digits, '.', '_' and '-'")
    if args.full_contig and not args.contigs:
        p.error("--full-contig needs an explicit --contigs list")
    if not args.full_contig and args.workers != 1:
        p.error("the panel runs with workers=1; --workers is for --full-contig")
    wanted_issues = None
    ledger_all = None
    if args.issues:
        wanted_issues = {normalize_issue(i) for i in args.issues}
        ledger_all = load_ledger(args.panel / "ledger.tsv")
        carrying = {
            r["contig"]
            for r in ledger_all
            if wanted_issues & set(r["issues"].split(","))
        }
        unknown_issues = sorted(
            wanted_issues - {i for r in ledger_all for i in r["issues"].split(",")}
        )
        if unknown_issues:
            p.error(f"no ledger rows for issues {unknown_issues}")
        if not args.contigs:
            # Only the contigs that hold a record of these issues.
            args.contigs = [c for c in CONTIGS if c in carrying]
    args.contigs = args.contigs or CONTIGS
    unknown = sorted(set(args.contigs) - set(CONTIGS))
    if unknown:
        p.error(f"unknown contigs {unknown}")
    if len(set(args.contigs)) != len(args.contigs):
        p.error("--contigs repeats a contig")
    mode = "full-contig" if args.full_contig else "panel"
    identity = input_identity(args.panel, mode, args.clinvar, args.golden, args.fasta)
    t0 = time.perf_counter()
    out = args.out or args.panel / "progress"
    vepyr_dir = out / f"vepyr_{args.label}"
    vepyr_dir.mkdir(parents=True, exist_ok=True)

    prev_rows = None
    prev_entries = None
    prev_records = None
    prev_fields = None
    if args.prev:
        # Validate and read before annotating: a bad --prev must not cost a run.
        check_prev_scope(args.prev, args.contigs, mode, identity)
        with open(args.prev, encoding="utf-8") as fh:
            prev_rows = list(csv.DictReader(fh, delimiter="\t"))
        prev_entries = read_prev_entries(args.prev)
        prev_fields = read_prev_fields(args.prev)
        if args.full_contig:
            prev_records = read_prev_records(args.prev)
    if args.skip_annotate and args.full_contig:
        absent = [
            c for c in args.contigs if not (vepyr_dir / f"chr{c}.vcf.gz").exists()
        ]
        if absent:
            raise SystemExit(
                f"--skip-annotate: no vepyr output for contigs {absent} in {vepyr_dir} "
                "(full-contig outputs are kept only with --keep-outputs)"
            )
    if not args.skip_annotate:
        # A run that fails partway must not leave an older run's provenance
        # next to its partial outputs.
        (vepyr_dir / "annotation.json").unlink(missing_ok=True)
        timings, annotate_seconds = _annotate_all(args, vepyr_dir)
        provenance = {
            "vepyr_version": vepyr_version(args.python),
            "python": args.python,
            "cache": args.cache,
            "fasta": args.fasta,
            "mode": mode,
            "workers": args.workers,
            "contigs": args.contigs,
            "input_identity": identity,
            "annotate_seconds": round(annotate_seconds, 1),
            "annotate_seconds_per_contig": timings,
        }
        (vepyr_dir / "annotation.json").write_text(
            json.dumps(provenance, indent=1) + "\n", encoding="utf-8"
        )
    else:
        # The outputs carry their own provenance: report the build and cache
        # that produced them, not the ones named on this command line.
        recorded = vepyr_dir / "annotation.json"
        if not recorded.exists():
            raise SystemExit(
                f"--skip-annotate: {recorded} is missing, so the reused outputs have no provenance"
            )
        provenance = json.loads(recorded.read_text(encoding="utf-8"))
        missing = sorted(set(args.contigs) - set(provenance.get("contigs") or []))
        if missing:
            raise SystemExit(
                f"--skip-annotate: {vepyr_dir} was not annotated for contigs {missing}"
            )
        if provenance.get("mode", "panel") != mode:
            raise SystemExit(
                f"--skip-annotate: {vepyr_dir} holds a {provenance.get('mode', 'panel')} run"
            )
        problem = _identity_mismatch(
            provenance.get("input_identity"), identity, f"--skip-annotate: {vepyr_dir}"
        )
        if problem:
            raise SystemExit(problem + " (or drop --skip-annotate)")
        timings = provenance.get("annotate_seconds_per_contig", {})
        annotate_seconds = provenance.get("annotate_seconds", 0.0)

    if ledger_all is None:
        ledger_all = load_ledger(args.panel / "ledger.tsv")
    ledger = [r for r in ledger_all if r["contig"] in args.contigs]
    full_counts: dict[str, dict] = {}
    if args.full_contig:
        keep = {record_key(r) for r in ledger}
        state = {}
        for c in args.contigs:
            vy = vepyr_dir / f"chr{c}.vcf.gz"
            contig_state, counts = evaluate_full(c, str(args.golden), vy, keep)
            if not counts.get("records"):
                # A contig name the golden and the release do not use (e.g.
                # "chr22" for "22") yields an empty, vacuous comparison.
                raise SystemExit(
                    f"chr{c}: no records in the golden or the vepyr output"
                )
            state.update(contig_state)
            full_counts[c] = dict(counts)
        excluded: set = set()
    else:
        state = evaluate(args.panel, vepyr_dir, args.contigs)
        excluded = EXCLUDED
    ledger_contigs_all = collections.defaultdict(set)
    for r in ledger_all:
        for i in r["issues"].split(","):
            ledger_contigs_all[i].add(r["contig"])
    table, entries, regressions = build_progress(
        ledger, state, prev_rows, prev_entries, excluded, prev_fields
    )
    if prev_rows is not None:
        for row in table:
            if not ledger_contigs_all[row["issue"]] <= set(args.contigs):
                row["delta_vs_prev"] = ""  # this run sees only part of the issue
    partial_issues = sorted(
        (
            i
            for i in {r["issue"] for r in table} | (wanted_issues or set())
            if not ledger_contigs_all[i] <= set(args.contigs)
        ),
        key=issue_order,
    )
    if wanted_issues:
        # The table narrows to the issues asked for. Regressions stay complete:
        # a fix for one issue can break a record of another.
        table = [r for r in table if r["issue"] in wanted_issues]
    ledger_keys = {record_key(r) for r in ledger}
    if not args.full_contig:
        # The panel holds only ledger records, so anything else in the state
        # is a record vepyr wrote that the panel input does not hold.
        # They are kept in entries_<label>.tsv, so a --prev run that already
        # had the same extra record does not report it again.
        for key in sorted(k for k in state if k not in ledger_keys):
            entries.append((key, "RECORD", "differs", "ONLY_IN_VEPYR", ""))
            if (
                prev_entries is not None
                and prev_entries.get((*key, "RECORD")) == "differs"
            ):
                continue
            regressions.append(
                (key, "RECORD", "record only in the vepyr output", "ONLY_IN_VEPYR", "")
            )
    if args.full_contig:
        # Records outside the ledger were identical in the triage; with --prev,
        # every record outside its differing set was identical there.
        for key, (identical, _, diffs) in sorted(state.items()):
            if identical or key in ledger_keys:
                continue
            fields = ",".join(sorted({f for fs in diffs.values() for f in fs}))
            features = ",".join(diffs)
            if prev_records is not None:
                if key not in prev_records:
                    regressions.append(
                        (
                            key,
                            features,
                            "record differs now, identical in --prev",
                            fields,
                            "",
                        )
                    )
            else:
                regressions.append(
                    (
                        key,
                        features,
                        "record differs now, identical in the triage",
                        fields,
                        "",
                    )
                )
        write_tsv(
            out / f"records_{args.label}.tsv",
            [
                "contig",
                "pos",
                "ref",
                "alt",
                "clinvar_id",
                "state",
                "features",
                "fields",
            ],
            (
                [
                    *k,
                    ("identical-vep-nondeterministic" if nd else "identical")
                    if ident
                    else "differs",
                    ",".join(d),
                    ",".join(sorted({f for fs in d.values() for f in fs})),
                ]
                for k, (ident, nd, d) in sorted(state.items())
            ),
        )

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
        "input_identity": identity,
        "mode": mode,
        "vepyr_version": provenance["vepyr_version"],
        "python": provenance["python"],
        "cache": provenance["cache"],
        "annotation_reused": bool(args.skip_annotate),
        "panel": str(args.panel),
        "golden": str(args.golden) if args.full_contig else None,
        "prev": str(args.prev) if args.prev else None,
        "contigs": args.contigs,
        "issues": sorted(wanted_issues) if wanted_issues else None,
        "partial_issues": [
            i for i in partial_issues if not wanted_issues or i in wanted_issues
        ],
        # with --issues: the records of those issues; otherwise every ledger record run
        "records": len(
            {
                record_key(r)
                for r in ledger
                if not wanted_issues or wanted_issues & set(r["issues"].split(","))
            }
        ),
        "full_contig_counts": full_counts,
        "annotate_seconds": round(annotate_seconds, 1),
        "annotate_seconds_per_contig": timings,
        "total_seconds": round(time.perf_counter() - t0, 1),
    }
    (out / f"summary_{args.label}.md").write_text(
        summary_markdown(args.label, table, state, ledger, regressions, meta, excluded),
        encoding="utf-8",
    )
    (out / f"run_{args.label}.json").write_text(
        json.dumps(meta, indent=1) + "\n", encoding="utf-8"
    )
    if args.full_contig and not args.skip_annotate and not args.keep_outputs:
        # Only after every report is written, and only files this run made.
        for c in args.contigs:
            (vepyr_dir / f"chr{c}.vcf.gz").unlink(missing_ok=True)
    print((out / f"summary_{args.label}.md").read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
