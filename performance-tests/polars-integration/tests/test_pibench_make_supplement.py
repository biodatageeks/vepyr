import importlib.util
from pathlib import Path

from pibench.queries import QUERIES

spec = importlib.util.spec_from_file_location(
    "make_supplement",
    Path(__file__).resolve().parents[1] / "scripts/make_supplement.py",
)
ms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ms)


def test_latex_escape_handles_filter_vep_characters():
    assert (
        ms.latex_escape("MAX_AF < 0.01 & x #1 {a}")
        == r"MAX\_AF $<$ 0.01 \& x \#1 \{a\}"
    )


def test_fmt_s_rounds_by_magnitude():
    assert (
        ms.fmt_s(0.123) == "0.12"
        and ms.fmt_s(12.34) == "12.3"
        and ms.fmt_s(1234.6) == "1235"
    )


def test_equivalence_table_has_one_row_per_query():
    tex = ms.equivalence_table(QUERIES)
    assert tex.count(r"\\") >= len(QUERIES) and r"\begin{tabular}" in tex


def test_equivalence_table_uses_filter_vep_expression_not_the_raw_field():
    # R3's filter_vep field is empty on purpose -- the table must call
    # filter_vep_expression(q) to get its region text, not q.filter_vep.
    tex = ms.equivalence_table(QUERIES)
    assert "29603520" in tex and "29698598" in tex


def test_filter_only_table_reports_speedup():
    rows = [
        {
            "query": "Q5",
            "tool": "filter_vep",
            "median_wall_s": 10.0,
            "median_rss_bytes": 1,
            "rows": 7,
        },
        {
            "query": "Q5",
            "tool": "polars",
            "median_wall_s": 2.0,
            "median_rss_bytes": 2 * 2**30,
            "rows": 7,
        },
    ]
    tex = ms.filter_only_table(rows)
    assert "Q5" in tex and "5.0" in tex and "2.00" in tex


def test_filter_only_table_skips_incomplete_pairs():
    rows = [
        {
            "query": "Q5",
            "tool": "filter_vep",
            "median_wall_s": 10.0,
            "median_rss_bytes": 1,
            "rows": 7,
        },
    ]
    tex = ms.filter_only_table(rows)
    assert "Q5" not in tex
    assert r"\begin{tabular}" in tex


def test_filter_only_table_ignores_a_row_missing_the_tool_key():
    rows = [
        {
            "query": "Q5",
            "tool": "filter_vep",
            "median_wall_s": 10.0,
            "median_rss_bytes": 1,
            "rows": 7,
        },
        {"query": "Q5", "median_wall_s": 2.0, "median_rss_bytes": 2 * 2**30, "rows": 7},
    ]
    tex = ms.filter_only_table(rows)
    assert "Q5" not in tex


def test_e2e_table_skips_a_query_with_no_vepyr_result():
    rows = [
        {
            "query": "Q5",
            "tool": "vep",
            "fork": 0,
            "processes": 1,
            "median_wall_s": 10.0,
        },
    ]
    tex = ms.e2e_table(rows)
    assert r"\begin{tabular}" in tex
    assert "Q5" in tex
    assert tex.count("--") == 3


def test_e2e_table_handles_no_rows_at_all():
    tex = ms.e2e_table([])
    assert r"\begin{tabular}" in tex


def test_pushdown_table_reports_speedup():
    rows = [
        {
            "query": "R1",
            "path": "collect",
            "variant": "pushdown",
            "workers": 1,
            "median_wall_s": 1.0,
        },
        {
            "query": "R1",
            "path": "collect",
            "variant": "no_pushdown",
            "workers": 1,
            "median_wall_s": 4.0,
        },
    ]
    tex = ms.pushdown_table(rows)
    assert "R1" in tex and "4.0" in tex


def test_load_reads_every_json_file_in_the_subdir(tmp_path):
    (tmp_path / "A").mkdir()
    (tmp_path / "A" / "Q5_filter_vep.json").write_text(
        '{"query": "Q5", "tool": "filter_vep"}'
    )
    (tmp_path / "A" / "Q5_polars.json").write_text('{"query": "Q5", "tool": "polars"}')
    rows = ms.load(tmp_path, "A")
    assert len(rows) == 2
    assert {r["tool"] for r in rows} == {"filter_vep", "polars"}


def test_latex_escape_renders_comparison_operators_as_math():
    assert ms.latex_escape("AF >= 0.1 and DP <= 5") == r"AF $\geq$ 0.1 and DP $\leq$ 5"
    assert "$>$=" not in ms.latex_escape("CADD_PHRED >= 20")


def test_tabular_long_emits_a_longtable_with_repeated_header():
    tex = ms._tabular("ll", ["A", "B"], [["1", "2"]], long=True)
    assert r"\begin{longtable}" in tex and r"\endhead" in tex
    assert tex.count("A & B") == 2


def _b_rows():
    return [
        {
            "query": "Q5",
            "tool": "vep",
            "fork": 0,
            "processes": 1,
            "annotate_median_wall_s": 90.0,
            "filter": {"median_wall_s": 10.0},
            "median_wall_s": 100.0,
        },
        *[
            {
                "query": "Q5",
                "tool": "vepyr",
                "path": p,
                "workers": 1,
                "processes": 1,
                "median_wall_s": s,
                "median_rss_bytes": 1,
            }
            for p, s in (("collect", 1.0), ("vcf", 4.0), ("parquet", 2.0))
        ],
    ]


def test_e2e_rows_speedup_is_against_the_slowest_vepyr_path():
    (row,) = ms.e2e_rows(_b_rows())
    assert row["vep_annotate_s"] == 90.0 and row["vep_filter_s"] == 10.0
    assert row["vepyr_slowest_s"] == 4.0 and row["speedup_vs_slowest"] == 25.0
    assert row["group"] == "consequence" and row["vepyr_workers"] == 1


def test_e2e_table_can_be_restricted_to_one_process_count():
    rows = _b_rows() + [dict(_b_rows()[0], fork=7, processes=8)]
    assert ms.e2e_table(rows, procs=1).count("Q5") == 1
    assert "Q5" not in ms.e2e_table(rows, procs=4)


def test_e2e_sliced_rows_join_on_query_and_processes():
    e2e = ms.e2e_rows([dict(r, query="R1") for r in _b_rows()])
    sliced = [
        {
            "query": "R1",
            "tool": "vep_sliced",
            "fork": 0,
            "processes": 1,
            "median_wall_s": 8.0,
            "rows": 3,
        }
    ]
    (row,) = ms.e2e_sliced_rows(sliced, e2e)
    assert row["vep_whole_chrom_total_s"] == 100.0 and row["speedup_vs_slowest"] == 2.0
    assert "R1" in ms.sliced_table([row])


def test_summary_table_reports_median_speedup_per_group():
    tex = ms.summary_table(ms.e2e_rows(_b_rows()))
    assert "consequence" in tex and r"25$\times$" in tex and "1 proc." in tex


def test_pushdown_rows_and_table_filter_by_workers():
    rows = [
        {
            "query": "R1",
            "path": "collect",
            "variant": v,
            "workers": w,
            "median_wall_s": s,
        }
        for w in (1, 8)
        for v, s in (("pushdown", 1.0), ("no_pushdown", 4.0))
    ]
    out = ms.pushdown_rows(rows)
    assert len(out) == 2 and out[0]["kind"] == "region" and out[0]["speedup"] == 4.0
    assert ms.pushdown_table(rows, workers=8).count("R1") == 1


def test_plugin_fix_rows_keep_only_configs_in_both_builds():
    new = [
        {
            "query": "P1",
            "path": "collect",
            "variant": "full",
            "workers": 1,
            "median_wall_s": 2.0,
        }
    ]
    old = [dict(new[0], median_wall_s=6.0), dict(new[0], workers=8)]
    (row,) = ms.plugin_fix_rows(old, new)
    assert row["speedup"] == 3.0
    assert "P1" in ms.plugin_fix_table([row])


def test_write_csv_round_trips_and_blanks_missing_values(tmp_path):
    import csv

    dest = tmp_path / "csv" / "x.csv"
    ms.write_csv([{"a": 1, "b": 0.5, "c": None}], dest)
    (row,) = csv.DictReader(dest.open())
    assert row == {"a": "1", "b": "0.5000", "c": ""}
