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
