"""ClinVar progress panel (e2e-testing/scripts/clinvar_progress.py)."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

SCRIPTS_DIR = Path(__file__).parents[1] / "e2e-testing" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import clinvar_progress as cp  # noqa: E402

FORMAT = "Allele|Consequence|Feature|HGVSp|CLIN_SIG"
HEADER = (
    "##fileformat=VCFv4.1\n"
    f'##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations. Format: {FORMAT}">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)


def rec(contig, pos, cid, csq, info="CLNSIG=Benign"):
    return f"{contig}\t{pos}\t{cid}\tG\tA\t.\t.\t{info};CSQ={csq}\n"


def write(path: Path, *records: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HEADER + "".join(records))


LEDGER_COLUMNS = [
    "contig",
    "pos",
    "ref",
    "alt",
    "clinvar_id",
    "feature",
    "issues",
    "repos",
    "diff_fields",
    "slugs",
    "class",
    "attribution",
    "source",
]


def ledger_row(contig, pos, cid, feature, issues, fields):
    return [
        contig,
        pos,
        "G",
        "A",
        cid,
        feature,
        issues,
        "engine",
        fields,
        "",
        "",
        "entry",
        "test",
    ]


def make_panel(tmp_path: Path) -> Path:
    """chr1: 100 fixed (#211), 200 still differs (#211 + #212 on two entries),
    300 differs only in CLIN_SIG order (#238), 400 regresses on a new entry;
    chr20:63674046 is excluded."""
    panel = tmp_path / "panel"
    vep = [
        rec("1", 100, "a", "A|missense|T1|p.X|"),
        rec("1", 200, "b", "A|inframe_deletion|T1|p.Y|,A|stop_gained|T2|p.Z|"),
        rec("1", 300, "c", "A|missense|T1|p.X|risk_factor&benign%3Brisk_factor"),
        rec("1", 400, "d", "A|missense|T1|p.X|,A|intron|T2||"),
    ]
    triage = [  # what vepyr wrote when the ledger was built
        rec("1", 100, "a", "A|stop_gained|T1|p.X|"),
        rec("1", 200, "b", "A|frameshift|T1|p.Y|,A|stop_gained&pav|T2|p.Z|"),
        rec("1", 300, "c", "A|missense|T1|p.X|risk_factor%3Brisk_factor&benign"),
        rec("1", 400, "d", "A|stop_gained|T1|p.X|,A|intron|T2||"),
    ]
    now = [
        rec("1", 100, "a", "A|missense|T1|p.X|"),
        rec("1", 200, "b", "A|frameshift|T1|p.W|,A|stop_gained&pav|T2|p.Z|"),
        triage[2],
        rec("1", 400, "d", "A|missense|T1|p.X|,A|intron|T2|p.Q|"),
    ]
    write(
        panel / "in" / "chr1.vcf",
        *[rec("1", r.split("\t")[1], r.split("\t")[2], "") for r in vep],
    )
    write(panel / "vep116_0" / "chr1.vcf", *vep)
    write(tmp_path / "vepyr" / "chr1.vcf", *now)
    ex_vep = rec("20", 63674046, "e", "A|missense|T9|p.X|")
    write(panel / "in" / "chr20.vcf", ex_vep)
    write(panel / "vep116_0" / "chr20.vcf", ex_vep)
    write(
        tmp_path / "vepyr" / "chr20.vcf", rec("20", 63674046, "e", "A|missense|T9|p.Y|")
    )
    with open(panel / "ledger.tsv", "w", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(LEDGER_COLUMNS)
        w.writerow(ledger_row("1", "100", "a", "T1", "vepyr#211", "Consequence"))
        w.writerow(ledger_row("1", "200", "b", "T1", "vepyr#211", "Consequence"))
        w.writerow(ledger_row("1", "200", "b", "T2", "vepyr#212", "Consequence"))
        w.writerow(ledger_row("1", "300", "c", "T1", "vepyr#238", "CLIN_SIG"))
        w.writerow(ledger_row("1", "400", "d", "T1", "vepyr#211", "Consequence"))
        w.writerow(ledger_row("20", "63674046", "e", "T9", "vepyr#202", "HGVSp"))
    return panel


def test_progress_counts_fixed_records_entries_and_regressions(tmp_path):
    panel = make_panel(tmp_path)
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    table, entries, regressions = cp.build_progress(ledger, state)
    rows = {r["issue"]: r for r in table}

    assert rows["vepyr#211"]["records_attributed"] == 3
    assert rows["vepyr#211"]["records_now_identical"] == 1  # 100 only; 400 regressed
    assert rows["vepyr#211"]["entries_now_identical"] == 2  # 100/T1 and 400/T1
    assert rows["vepyr#212"]["entries_now_identical"] == 0
    # CLIN_SIG order only: VEP-nondeterministic, counted identical (#238)
    assert rows["vepyr#238"]["records_now_identical"] == 1
    assert state[("1", "300", "G", "A", "c")][:2] == (True, True)
    # the excluded HGNC_ID record never counts
    assert "vepyr#202" not in rows
    kinds = {(k[1], f, kind) for k, f, kind, _, _ in regressions}
    assert (
        "200",
        "T1",
        "new fields on an attributed entry",
    ) in kinds  # HGVSp changed too
    assert ("400", "T2", "differs now, identical in the triage baseline") in kinds
    assert len(regressions) == 2


def test_clin_sig_sort_keeps_real_differences():
    assert cp.sort_clin_sig("b%3Ba&c") == "a&c%3Bb"
    assert cp.sort_clin_sig("a&b") == "a&b"
    fmt = FORMAT.split("|")
    diffs, nondet = cp.entry_diffs(
        rec("1", 1, "x", "A|m|T1||risk_factor%3Bbenign&risk_factor").rstrip("\n"),
        rec("1", 1, "x", "A|m|T1||risk_factor&benign%3Brisk_factor").rstrip("\n"),
        fmt,
        fmt,
    )
    assert diffs == {"T1": ["CLIN_SIG"]} and not nondet


def test_entry_diffs_names_info_order_and_one_sided_entries():
    fmt = FORMAT.split("|")
    a = rec("1", 1, "x", "A|m|T1||,A|i|T2||").rstrip("\n")
    assert cp.entry_diffs(
        a, rec("1", 1, "x", "A|i|T2||,A|m|T1||").rstrip("\n"), fmt, fmt
    )[0] == {"ORDER": ["CSQ_ORDER"]}
    assert cp.entry_diffs(a, rec("1", 1, "x", "A|m|T1||").rstrip("\n"), fmt, fmt)[
        0
    ] == {"T2": ["ONLY_IN_VEP"]}
    other_info = rec("1", 1, "x", "A|m|T1||,A|i|T2||", info="CLNSIG=Pathogenic").rstrip(
        "\n"
    )
    assert cp.entry_diffs(a, other_info, fmt, fmt)[0] == {"INFO": ["INFO"]}


def provenance(directory: Path, version: str = "0.10.1") -> None:
    (directory / "annotation.json").write_text(
        f'{{"vepyr_version": "{version}", "python": "py", "cache": "/recorded", '
        '"contigs": ["1", "20"]}'
    )


def test_main_writes_outputs_and_reads_prev(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_R0").mkdir(parents=True)
    for c in ("1", "20"):
        (out / "vepyr_R0" / f"chr{c}.vcf").write_text(
            (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
        )
    provenance(out / "vepyr_R0")
    args = [
        "--python",
        sys.executable,
        "--cache",
        "/cache",
        "--panel",
        str(panel),
        "--out",
        str(out),
        "--contigs",
        "1",
        "20",
        "--skip-annotate",
    ]
    assert cp.main([*args, "--label", "R0"]) == 0
    progress = {
        r["issue"]: r
        for r in csv.DictReader(open(out / "progress_R0.tsv"), delimiter="\t")
    }
    assert progress["vepyr#211"]["records_now_identical"] == "1"
    summary = (out / "summary_R0.md").read_text()
    assert "| #211 | engine | 1 / 3 |" in summary
    assert "Excluded" in summary and "VEP-nondeterministic" in summary
    # --skip-annotate reports the recorded provenance, not the command line's
    assert "vepyr `0.10.1`, cache `/recorded`" in summary

    # Round 1 fixes chr1:400 and breaks chr1:100.
    (out / "vepyr_R1").mkdir()
    r1 = (out / "vepyr_R0" / "chr1.vcf").read_text()
    r1 = r1.replace(
        "A|missense|T1|p.X|,A|intron|T2|p.Q|", "A|missense|T1|p.X|,A|intron|T2||"
    )
    r1 = r1.replace(
        "1\t100\ta\tG\tA\t.\t.\tCLNSIG=Benign;CSQ=A|missense|",
        "1\t100\ta\tG\tA\t.\t.\tCLNSIG=Benign;CSQ=A|stop_gained|",
    )
    (out / "vepyr_R1" / "chr1.vcf").write_text(r1)
    (out / "vepyr_R1" / "chr20.vcf").write_text(
        (out / "vepyr_R0" / "chr20.vcf").read_text()
    )
    provenance(out / "vepyr_R1", version="0.11.0")
    assert (
        cp.main([*args, "--label", "R1", "--prev", str(out / "progress_R0.tsv")]) == 0
    )
    progress = {
        r["issue"]: r
        for r in csv.DictReader(open(out / "progress_R1.tsv"), delimiter="\t")
    }
    assert progress["vepyr#211"]["records_now_identical"] == "1"
    assert progress["vepyr#211"]["delta_vs_prev"] == "+0"
    regressions = list(csv.DictReader(open(out / "regressions_R1.tsv"), delimiter="\t"))
    assert any(
        r["pos"] == "100" and r["kind"] == "differs now, identical in --prev"
        for r in regressions
    )


def test_prev_with_another_contig_scope_is_rejected(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_R0").mkdir(parents=True)
    for c in ("1", "20"):
        (out / "vepyr_R0" / f"chr{c}.vcf").write_text(
            (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
        )
    provenance(out / "vepyr_R0")
    base = [
        "--python",
        sys.executable,
        "--cache",
        "/c",
        "--panel",
        str(panel),
        "--out",
        str(out),
        "--skip-annotate",
    ]
    assert cp.main([*base, "--label", "R0", "--contigs", "1", "20"]) == 0
    with pytest.raises(SystemExit, match="same --contigs"):
        cp.main(
            [
                *base,
                "--label",
                "R0",
                "--contigs",
                "1",
                "--prev",
                str(out / "progress_R0.tsv"),
            ]
        )


def test_identical_bytes_under_different_csq_formats_are_compared_by_field(tmp_path):
    panel = make_panel(tmp_path)
    swapped = HEADER.replace(
        "Allele|Consequence|Feature|HGVSp|CLIN_SIG",
        "Allele|Consequence|Feature|CLIN_SIG|HGVSp",
    )
    path = tmp_path / "vepyr" / "chr1.vcf"
    vep_body = (panel / "vep116_0" / "chr1.vcf").read_text().split(HEADER)[1]
    path.write_text(swapped + vep_body)
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1"])
    # chr1:100 carries HGVSp p.X and an empty CLIN_SIG: the swap makes both differ
    assert state[("1", "100", "G", "A", "a")][0] is False


def test_new_differences_are_reported_next_to_a_record_level_row(tmp_path):
    panel = make_panel(tmp_path)
    ledger = cp.load_ledger(panel / "ledger.tsv")
    for row in ledger:
        if row["pos"] == "400":
            row["feature"] = "-"
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    _, _, regressions = cp.build_progress(ledger, state)
    assert any(k[1] == "400" and f == "T2" for k, f, *_ in regressions)


@pytest.mark.skipif(
    shutil.which("bgzip") is None or shutil.which("tabix") is None,
    reason="htslib bgzip/tabix not on PATH",
)
def test_full_contig_mode_against_a_golden(tmp_path):
    """--full-contig compares every record of the contig with the golden:
    ledger records are judged in full-contig context (nothing excluded), and a
    record outside the ledger that now differs is a regression."""
    panel = make_panel(tmp_path)
    golden_body = (panel / "vep116_0" / "chr1.vcf").read_text().split(HEADER)[1]
    golden_body += rec("1", 500, "f", "A|intron|T3||")  # identical in the triage
    golden = tmp_path / "golden.vcf"
    golden.write_text(HEADER + golden_body)
    subprocess.run(["bgzip", "-f", str(golden)], check=True)
    subprocess.run(["tabix", "-p", "vcf", str(golden) + ".gz"], check=True)

    out = tmp_path / "out"
    vdir = out / "vepyr_F0"
    vdir.mkdir(parents=True)
    now = (tmp_path / "vepyr" / "chr1.vcf").read_text()
    now += rec("1", 500, "f", "A|intron|T3|p.R|")  # new difference
    plain = vdir / "chr1.vcf"
    plain.write_text(now)
    subprocess.run(["bgzip", "-f", str(plain)], check=True)
    (vdir / "annotation.json").write_text(
        '{"vepyr_version": "master", "python": "py", "cache": "/c", '
        '"mode": "full-contig", "contigs": ["1"]}'
    )
    args = [
        "--python",
        sys.executable,
        "--cache",
        "/c",
        "--panel",
        str(panel),
        "--out",
        str(out),
        "--contigs",
        "1",
        "--full-contig",
        "--golden",
        str(golden) + ".gz",
        "--skip-annotate",
        "--label",
        "F0",
    ]
    assert cp.main(args) == 0
    run = json.loads((out / "run_F0.json").read_text())
    assert run["full_contig_counts"]["1"] == {
        "records": 5,
        "identical": 2,
        "differ": 3,
        "vep_nondeterministic": 1,  # chr1:300, CLIN_SIG order only
    }
    regressions = list(csv.DictReader(open(out / "regressions_F0.tsv"), delimiter="\t"))
    assert any(
        r["pos"] == "500" and "identical in the triage" in r["kind"]
        for r in regressions
    )
    records = list(csv.DictReader(open(out / "records_F0.tsv"), delimiter="\t"))
    assert {r["pos"] for r in records if r["state"] == "differs"} == {
        "200",
        "400",
        "500",
    }
    # reused outputs are never deleted; only a run's own outputs are
    assert (vdir / "chr1.vcf.gz").exists()


def test_deferred_issues_are_listed_apart_from_the_targets():
    table = [
        {
            "issue": "vepyr#211",
            "repo": "engine",
            "records_attributed": 3,
            "records_now_identical": 1,
            "entries_attributed": 3,
            "entries_now_identical": 1,
            "delta_vs_prev": "",
            "status": "",
        },
        {
            "issue": "vepyr#201",
            "repo": "engine",
            "records_attributed": 4,
            "records_now_identical": 0,
            "entries_attributed": 4,
            "entries_now_identical": 0,
            "delta_vs_prev": "",
            "status": cp.NOTES["vepyr#201"],
        },
    ]
    meta = {
        "vepyr_version": "x",
        "cache": "c",
        "records": 0,
        "annotate_seconds": 0,
        "total_seconds": 0,
    }
    md = cp.summary_markdown("L", table, {}, [], [], meta)
    assert "| #211 |" in md and "| #201 |" not in md
    assert "- #201: known VEP bug (deferred); 0 / 4 records identical" in md


def test_a_record_missing_from_vepyr_is_a_regression(tmp_path):
    panel = make_panel(tmp_path)
    path = tmp_path / "vepyr" / "chr1.vcf"
    path.write_text(
        "".join(ln for ln in path.read_text().splitlines(True) if "\t200\t" not in ln)
    )
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    _, _, regressions = cp.build_progress(cp.load_ledger(panel / "ledger.tsv"), state)
    assert any(k[1] == "200" and f == "RECORD" for k, f, *_ in regressions)


def test_a_panel_without_a_vep_line_fails_readably(tmp_path):
    panel = make_panel(tmp_path)
    vep = panel / "vep116_0" / "chr1.vcf"
    vep.write_text(
        "".join(ln for ln in vep.read_text().splitlines(True) if "\t100\t" not in ln)
    )
    with pytest.raises(SystemExit, match="has no line for chr1:100"):
        cp.evaluate(panel, tmp_path / "vepyr", ["1"])


def test_a_bad_prev_is_rejected_before_annotation(tmp_path, monkeypatch):
    panel = make_panel(tmp_path)
    called = []
    monkeypatch.setattr(cp, "_annotate_all", lambda *a: called.append(1))
    with pytest.raises(SystemExit, match="is missing"):
        cp.main(
            [
                "--python",
                sys.executable,
                "--cache",
                "/c",
                "--panel",
                str(panel),
                "--out",
                str(tmp_path / "o"),
                "--label",
                "R1",
                "--prev",
                str(tmp_path / "progress_R0.tsv"),
            ]
        )
    assert called == []
