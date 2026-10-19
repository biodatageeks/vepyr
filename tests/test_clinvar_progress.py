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


def provenance(
    directory: Path,
    version: str = "0.10.1",
    contigs=("1", "20"),
    mode: str = "panel",
    clinvar: Path | None = None,
    golden: Path | None = None,
) -> None:
    """annotation.json as a real run writes it; the panel is <tmp>/panel."""
    panel = directory.parent.parent / "panel"
    identity = cp.input_identity(panel, mode, clinvar, golden)
    (directory / "annotation.json").write_text(
        json.dumps(
            {
                "vepyr_version": version,
                "python": "py",
                "cache": "/recorded",
                "mode": mode,
                "contigs": list(contigs),
                "input_identity": identity,
            }
        )
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
    assert cp.main([*base, "--label", "R0", "--contigs", "1"]) == 0
    with pytest.raises(SystemExit, match="same --contigs"):
        cp.main(
            [
                *base,
                "--label",
                "R0",
                "--contigs",
                "1",
                "20",
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
    clinvar = tmp_path / "clinvar.vcf.gz"
    clinvar.write_bytes(b"")
    provenance(
        vdir, "master", ("1",), "full-contig", clinvar, Path(str(golden) + ".gz")
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
        "--clinvar",
        str(clinvar),
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
    states = {r["pos"]: r["state"] for r in records}
    assert states["300"] == "identical-vep-nondeterministic"
    assert (
        "VEP-nondeterministic `CLIN_SIG` order (#238): 1:300"
        in (out / "summary_F0.md").read_text()
    )
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


@pytest.mark.skipif(
    shutil.which("bcftools") is None or shutil.which("tabix") is None,
    reason="bcftools/tabix not on PATH",
)
def test_full_contig_slice_drops_alt_dot_and_splits_multiallelics(tmp_path):
    """The slice gets the golden input's preprocessing: ALT="." removed and
    bcftools norm -m -both (codex review on #255)."""
    src = tmp_path / "clinvar.vcf"
    src.write_text(
        "##fileformat=VCFv4.1\n##contig=<ID=1>\n##contig=<ID=2>\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "1\t100\ta\tG\tA\t.\t.\t.\n"
        "1\t200\tb\tG\t.\t.\t.\t.\n"
        "1\t300\tc\tG\tA,T\t.\t.\t.\n"
        "2\t100\td\tG\tA\t.\t.\t.\n"
    )
    subprocess.run(["bgzip", "-f", str(src)], check=True)
    subprocess.run(["tabix", "-p", "vcf", str(src) + ".gz"], check=True)
    # Stand-in interpreter: "python -c CODE in cache fasta out workers" copies in to out.
    fake = tmp_path / "fake_python"
    fake.write_text('#!/bin/sh\ncp "$3" "$6"\n')
    fake.chmod(0o755)
    out = tmp_path / "chr1.vcf.gz"
    cp.annotate_full_contig(str(fake), str(src) + ".gz", "/c", "/f", out, "1", 4)
    body = subprocess.run(
        ["bcftools", "view", "-H", str(out)], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    assert [ln.split("\t")[1:5] for ln in body] == [
        ["100", "a", "G", "A"],
        ["300", "c", "G", "A"],
        ["300", "c", "G", "T"],
    ]
    # the slice and its index are gone
    assert (
        sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("chr1.in")) == []
    )


def test_issues_narrows_contigs_and_table(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_T").mkdir(parents=True)
    (out / "vepyr_T" / "chr1.vcf").write_text(
        (tmp_path / "vepyr" / "chr1.vcf").read_text()
    )
    provenance(out / "vepyr_T", "x", ("1",))
    args = [
        "--python",
        sys.executable,
        "--cache",
        "/c",
        "--panel",
        str(panel),
        "--out",
        str(out),
        "--skip-annotate",
        "--label",
        "T",
        "--issues",
        "211",
        "#212",
    ]
    assert cp.main(args) == 0
    run = json.loads((out / "run_T.json").read_text())
    assert run["contigs"] == ["1"]  # chr20 holds only #202
    assert run["issues"] == ["vepyr#211", "vepyr#212"]
    issues = [
        r["issue"] for r in csv.DictReader(open(out / "progress_T.tsv"), delimiter="\t")
    ]
    assert issues == ["vepyr#211", "vepyr#212"]
    with pytest.raises(SystemExit):
        cp.main([*args[:-2], "999"])


def test_a_record_only_in_the_vepyr_output_is_a_regression(tmp_path):
    panel = make_panel(tmp_path)
    path = tmp_path / "vepyr" / "chr1.vcf"
    path.write_text(path.read_text() + rec("1", 900, "z", "A|missense|T1||"))
    out = tmp_path / "out"
    (out / "vepyr_X").mkdir(parents=True)
    for c in ("1", "20"):
        (out / "vepyr_X" / f"chr{c}.vcf").write_text(
            (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
        )
    provenance(out / "vepyr_X")
    assert (
        cp.main(
            [
                "--python",
                sys.executable,
                "--cache",
                "/c",
                "--panel",
                str(panel),
                "--out",
                str(out),
                "--skip-annotate",
                "--label",
                "X",
                "--contigs",
                "1",
                "20",
            ]
        )
        == 0
    )
    regressions = list(csv.DictReader(open(out / "regressions_X.tsv"), delimiter="\t"))
    assert any(
        r["pos"] == "900" and r["kind"] == "record only in the vepyr output"
        for r in regressions
    )


def test_issues_scope_the_headline_and_flag_partial_coverage(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_Y").mkdir(parents=True)
    (out / "vepyr_Y" / "chr1.vcf").write_text(
        (tmp_path / "vepyr" / "chr1.vcf").read_text()
    )
    provenance(out / "vepyr_Y")
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
        "--label",
        "Y",
    ]
    assert cp.main([*base, "--issues", "238"]) == 0
    assert "**1 / 1 records now identical" in (out / "summary_Y.md").read_text()
    # #202's only record is on chr20: an explicit chr1 run covers none of it
    assert cp.main([*base, "--issues", "211", "202", "--contigs", "1"]) == 0
    assert "**Partial:**" in (out / "summary_Y.md").read_text()


def test_a_regressed_entry_is_reported_once_with_prev(tmp_path):
    """chr1:400/T2 is outside the ledger, identical in R0's predecessor and
    differs now: one row, not one per baseline (codex review on #255)."""
    panel = make_panel(tmp_path)
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    prev_entries = {("1", "400", "G", "A", "d", "T2"): "identical"}
    _, _, regressions = cp.build_progress(ledger, state, [], prev_entries)
    assert [(k[1], f) for k, f, *_ in regressions].count(("400", "T2")) == 1


def test_a_missing_record_is_one_regression_even_with_prev(tmp_path):
    panel = make_panel(tmp_path)
    path = tmp_path / "vepyr" / "chr1.vcf"
    path.write_text(
        "".join(ln for ln in path.read_text().splitlines(True) if "\t200\t" not in ln)
    )
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    prev = {("1", "200", "G", "A", "b", f): "identical" for f in ("T1", "T2")}
    _, _, regressions = cp.build_progress(ledger, state, [], prev)
    assert [(k[1], f) for k, f, *_ in regressions if k[1] == "200"] == [
        ("200", "RECORD")
    ]


def test_prev_is_the_baseline_for_entries_outside_the_ledger(tmp_path):
    """chr1:400/T2 differs now and is not in the ledger. Against a --prev in
    which it already differed it is persistent (no row); against one in which
    it was identical it is a --prev regression, not a triage one."""
    panel = make_panel(tmp_path)
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    key = ("1", "400", "G", "A", "d", "T2")
    _, _, persistent = cp.build_progress(ledger, state, [], {key: "differs"})
    assert not [r for r in persistent if r[0][1] == "400" and r[1] == "T2"]
    _, _, new = cp.build_progress(ledger, state, [], {key: "identical"})
    assert [r[2] for r in new if r[0][1] == "400" and r[1] == "T2"] == [
        "differs now, identical in --prev"
    ]


def test_a_persistent_output_only_record_is_not_reported_again(tmp_path):
    panel = make_panel(tmp_path)
    path = tmp_path / "vepyr" / "chr1.vcf"
    path.write_text(path.read_text() + rec("1", 900, "z", "A|missense|T1||"))
    out = tmp_path / "out"
    for label in ("A", "B"):
        (out / f"vepyr_{label}").mkdir(parents=True)
        for c in ("1", "20"):
            (out / f"vepyr_{label}" / f"chr{c}.vcf").write_text(
                (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
            )
        provenance(out / f"vepyr_{label}")
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
        "--contigs",
        "1",
        "20",
    ]
    assert cp.main([*base, "--label", "A"]) == 0
    assert cp.main([*base, "--label", "B", "--prev", str(out / "progress_A.tsv")]) == 0

    def kinds(label):
        rows = csv.DictReader(open(out / f"regressions_{label}.tsv"), delimiter="\t")
        return [r["kind"] for r in rows if r["pos"] == "900"]

    assert kinds("A") == ["record only in the vepyr output"]
    assert kinds("B") == []


def test_entries_sharing_a_feature_keep_every_differing_field():
    fmt = FORMAT.split("|")
    vep = rec("1", 1, "x", "A|m|T1|p.X|,C|m|T1|p.Y|").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|n|T1|p.X|,C|m|T1|p.Z|").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"T1": ["Consequence", "HGVSp"]}


def test_a_persistently_missing_record_is_not_reported_again(tmp_path):
    panel = make_panel(tmp_path)
    path = tmp_path / "vepyr" / "chr1.vcf"
    path.write_text(
        "".join(ln for ln in path.read_text().splitlines(True) if "\t200\t" not in ln)
    )
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    _, entries, first = cp.build_progress(ledger, state)
    assert [(k[1], f) for k, f, *_ in first if k[1] == "200"] == [("200", "RECORD")]
    prev = {(*k, f): st for k, f, st, *_ in entries}
    assert prev[("1", "200", "G", "A", "b", "RECORD")] == "differs"
    _, _, again = cp.build_progress(ledger, state, [], prev)
    assert [r for r in again if r[0][1] == "200"] == []


def test_csq_order_is_reported_next_to_field_differences():
    fmt = FORMAT.split("|")
    vep = rec("1", 1, "x", "A|m|T1|p.X|,A|i|T2||").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|i|T2||,A|n|T1|p.X|").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"T1": ["Consequence"], "ORDER": ["CSQ_ORDER"]}


def test_a_missing_csq_column_is_a_difference_even_when_vep_is_empty():
    fmt = FORMAT.split("|")
    short = [f for f in fmt if f != "CLIN_SIG"]
    vep = rec("1", 1, "x", "A|m|T1|p.X|").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|m|T1|p.X").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, short)
    assert diffs == {"CSQ_FORMAT": ["CLIN_SIG"]}


def test_reordered_duplicate_entries_pair_by_payload():
    """Two entries share (Allele, Feature) and only swap places: an ORDER
    difference, not two field changes (codex review on #255)."""
    fmt = FORMAT.split("|")
    vep = rec("1", 1, "x", "A|m|T1|p.X|,A|n|T1|p.Y|").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|n|T1|p.Y|,A|m|T1|p.X|").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"ORDER": ["CSQ_ORDER"]}


def test_another_csq_field_order_is_a_difference():
    fmt = FORMAT.split("|")
    swapped = ["Allele", "Consequence", "Feature", "CLIN_SIG", "HGVSp"]
    vep = rec("1", 1, "x", "A|m|T1|p.X|").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|m|T1||p.X").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, swapped)
    assert diffs == {"CSQ_FIELD_ORDER": ["CSQ_FIELD_ORDER"]}


def test_repeated_contigs_are_rejected(tmp_path):
    with pytest.raises(SystemExit):
        cp.main(
            [
                "--python",
                sys.executable,
                "--cache",
                "/c",
                "--panel",
                str(tmp_path),
                "--label",
                "D",
                "--contigs",
                "1",
                "1",
            ]
        )


def test_changed_and_reordered_duplicates_pair_by_similarity():
    fmt = FORMAT.split("|")
    vep = rec("1", 1, "x", "A|missense|T1|p.X|,A|stop_gained|T1|p.Y|").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|stop_gained|T1|p.Z|,A|missense|T1|p.W|").rstrip("\n")
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"T1": ["HGVSp"], "ORDER": ["CSQ_ORDER"]}


def test_csq_moved_within_info_is_an_info_difference():
    fmt = FORMAT.split("|")
    vep = "1\t1\tx\tG\tA\t.\t.\tCLNSIG=B;CSQ=A|m|T1||;DP=1"
    vepyr = "1\t1\tx\tG\tA\t.\t.\tCLNSIG=B;DP=1;CSQ=A|m|T1||"
    diffs, _ = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"INFO": ["INFO"]}


def test_reused_outputs_from_another_panel_are_refused(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_P").mkdir(parents=True)
    for c in ("1", "20"):
        (out / "vepyr_P" / f"chr{c}.vcf").write_text(
            (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
        )
    provenance(out / "vepyr_P")
    with open(panel / "ledger.tsv", "a") as fh:  # the panel changes afterwards
        fh.write(
            "\t".join(ledger_row("1", "100", "a", "T9", "vepyr#211", "HGVSp")) + "\n"
        )
    with pytest.raises(SystemExit, match="another panel or golden"):
        cp.main(
            [
                "--python",
                sys.executable,
                "--cache",
                "/c",
                "--panel",
                str(panel),
                "--out",
                str(out),
                "--skip-annotate",
                "--label",
                "P",
                "--contigs",
                "1",
                "20",
            ]
        )


def test_a_schema_mismatch_leaves_no_entry_fixed(tmp_path):
    panel = make_panel(tmp_path)
    ledger = cp.load_ledger(panel / "ledger.tsv")
    state = cp.evaluate(panel, tmp_path / "vepyr", ["1", "20"])
    key = ("1", "100", "G", "A", "a")
    state[key] = (False, False, {"CSQ_FORMAT": ["CLIN_SIG"]})
    table, entries, _ = cp.build_progress(ledger, state)
    rows = {r["issue"]: r for r in table}
    assert rows["vepyr#211"]["entries_now_identical"] == 1  # only 400/T1 now
    # the attributed T1 entry is not fixed, and the schema row itself differs
    assert sorted((e[1], e[2]) for e in entries if e[0] == key) == [
        ("CSQ_FORMAT", "differs"),
        ("T1", "differs"),
    ]


def test_prev_from_another_panel_is_refused(tmp_path):
    panel = make_panel(tmp_path)
    out = tmp_path / "out"
    (out / "vepyr_A").mkdir(parents=True)
    for c in ("1", "20"):
        (out / "vepyr_A" / f"chr{c}.vcf").write_text(
            (tmp_path / "vepyr" / f"chr{c}.vcf").read_text()
        )
    provenance(out / "vepyr_A")
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
        "--contigs",
        "1",
        "20",
    ]
    assert cp.main([*base, "--label", "A"]) == 0
    run = out / "run_A.json"
    data = json.loads(run.read_text())
    data["input_identity"] = None  # a run from before identities existed
    run.write_text(json.dumps(data))
    with pytest.raises(SystemExit, match="no input_identity recorded"):
        cp.main([*base, "--label", "A", "--prev", str(out / "progress_A.tsv")])
    data["input_identity"] = {"mode": "panel", "panel_sha256": "0"}
    run.write_text(json.dumps(data))
    with pytest.raises(SystemExit, match="another panel or golden"):
        cp.main([*base, "--label", "A", "--prev", str(out / "progress_A.tsv")])


def test_similarity_pairing_reads_clin_sig_canonically():
    """Two changed duplicates: the true partner differs only in CLIN_SIG's
    random order plus HGVSp, so it must outscore the other one."""
    fmt = FORMAT.split("|")
    vep = rec("1", 1, "x", "A|m|T1|p.X|b%3Ba,A|m|T1|p.Q|c").rstrip("\n")
    vepyr = rec("1", 1, "x", "A|m|T1|p.R|c,A|m|T1|p.Y|a%3Bb").rstrip("\n")
    diffs, nondet = cp.entry_diffs(vep, vepyr, fmt, fmt)
    assert diffs == {"T1": ["HGVSp"], "ORDER": ["CSQ_ORDER"]} and nondet


def test_a_ledger_with_two_rows_for_one_entry_is_refused(tmp_path):
    panel = make_panel(tmp_path)
    with open(panel / "ledger.tsv", "a") as fh:
        fh.write(
            "\t".join(ledger_row("1", "100", "a", "T1", "vepyr#212", "HGVSp")) + "\n"
        )
    with pytest.raises(SystemExit, match="two ledger rows for one entry"):
        cp.load_ledger(panel / "ledger.tsv")
