"""The LazyFrame run-pool bench must refuse runs that did not process the input.

A run that drops records finishes sooner, so without this check an empty or
truncated input, or an engine regression that loses rows, reads as a speed-up.
"""

from __future__ import annotations

import gzip
import importlib.util
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).parents[1] / "performance-tests/vepyr/scripts/lf_run_pool_bench.py"
)


@pytest.fixture(scope="module")
def bench():
    spec = importlib.util.spec_from_file_location("lf_run_pool_bench", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _vcf(path: Path, records: int, compress: bool) -> Path:
    lines = ["##fileformat=VCFv4.2", "#CHROM\tPOS\tID\tREF\tALT"]
    lines += [f"chr1\t{100 + i}\t.\tA\tC" for i in range(records)]
    text = "\n".join(lines) + "\n"
    if compress:
        with gzip.open(path, "wt") as f:
            f.write(text)
    else:
        path.write_text(text)
    return path


def test_count_records_skips_headers(bench, tmp_path):
    assert bench.count_records(str(_vcf(tmp_path / "a.vcf.gz", 5, True))) == 5
    assert bench.count_records(str(_vcf(tmp_path / "a.vcf", 3, False))) == 3


def test_check_rows_accepts_complete_runs(bench):
    bench.check_rows("chr22 lf w8", [{"rows": 7}, {"rows": 7}], 7)


def test_check_rows_rejects_a_run_that_processed_nothing(bench):
    with pytest.raises(SystemExit, match="chr22 vcf w8"):
        bench.check_rows("chr22 vcf w8", [{"rows": 7}, {"rows": 0}], 7)


def test_check_rows_rejects_a_truncated_run(bench):
    with pytest.raises(SystemExit, match="6"):
        bench.check_rows("chr1 raw w4", [{"rows": 6}], 7)


def test_check_rows_rejects_an_empty_input(bench):
    with pytest.raises(SystemExit, match="no records"):
        bench.check_rows("chr1 raw w4", [{"rows": 0}], 0)


def test_wait_for_quiet_refuses_a_host_that_stays_busy(bench):
    # Any real load exceeds a negative threshold, so the host never goes quiet.
    with pytest.raises(SystemExit, match="load"):
        bench.wait_for_quiet(-1.0, timeout_s=0.0)


def test_wait_for_quiet_returns_the_load_on_a_quiet_host(bench):
    assert bench.wait_for_quiet(1e9) >= 0.0


def _row(mode, wall, setup):
    return {
        "input": "chr22",
        "plugins": "none",
        "mode": mode,
        "workers": 8,
        "median_wall_s": wall,
        "min_wall_s": wall,
        "max_wall_s": wall,
        "median_setup_s": setup,
        "median_end_to_end_s": wall + setup,
        "median_rss_gib": 1.0,
        "median_engine_wait_s": 0.0,
        "median_consumer_s": 0.0,
    }


def test_lf_vcf_ratio_counts_lazyframe_setup(bench):
    # output_vcf's wall starts at the annotate() call; the LazyFrame's stream
    # wall starts after annotate() returns, so the gate ratio adds its setup.
    summary = bench.render_summary([_row("lf", 1.0, 0.2), _row("vcf", 1.0, 0.0)])
    assert "lf end-to-end/vcf = 1.20" in summary


def test_end_to_end_median_pairs_setup_with_its_own_run(bench):
    # Warm-up first, then three kept runs. Medians of the parts are 0.5 and
    # 1.0 (sum 1.5); the runs themselves took 1.0, 1.0 and 1.5 end to end.
    runs = [
        {"setup_s": 9.0, "wall_s": 9.0},
        {"setup_s": 0.0, "wall_s": 1.0},
        {"setup_s": 0.5, "wall_s": 0.5},
        {"setup_s": 0.5, "wall_s": 1.0},
    ]
    assert bench.median_end_to_end(runs[1:]) == 1.0


def test_child_env_drops_inherited_engine_tuning(bench):
    inherited = {
        "PATH": "/bin",
        "VEP_STREAM_BUFFER_MB": "256",
        "VEP_PIPELINE_TRACE": "1",
    }
    env = bench.child_env(inherited, ["VEP_STREAM_RUN_BUFFERS=2"])
    # Engine knobs reach a run only when the bench was told about them.
    assert env == {"PATH": "/bin", "VEP_STREAM_RUN_BUFFERS": "2"}


def test_child_env_keeps_an_explicit_override_of_an_inherited_knob(bench):
    env = bench.child_env(
        {"VEP_STREAM_BUFFER_MB": "256"}, ["VEP_STREAM_BUFFER_MB=4096"]
    )
    assert env == {"VEP_STREAM_BUFFER_MB": "4096"}


def _gate_row(inp, plugins, mode, workers, wall, setup=0.0):
    return {
        "input": inp,
        "plugins": plugins,
        "mode": mode,
        "workers": workers,
        "median_wall_s": wall,
        "median_end_to_end_s": wall + setup,
    }


def _passing_rows():
    return [
        _gate_row("chr22", "none", "raw", 4, 1.4),
        _gate_row("chr22", "none", "raw", 8, 1.0),
        _gate_row("chr22", "none", "lf", 8, 1.1, 0.03),
        _gate_row("chr22", "none", "vcf", 8, 1.4),
        _gate_row("chr1", "all", "raw", 4, 13.0),
        _gate_row("chr1", "all", "raw", 8, 9.0),
        _gate_row("chr1", "all", "lf", 8, 30.0),
        _gate_row("chr1", "all", "vcf", 8, 11.0),
    ]


def test_gate_passes_when_raw_scales_and_core_lf_is_close_to_vcf(bench):
    assert bench.gate(_passing_rows(), ratio_plugins={"none"}) == []


def test_gate_fails_when_raw_does_not_improve_from_w4_to_w8(bench):
    rows = [
        r for r in _passing_rows() if not (r["input"] == "chr1" and r["mode"] == "raw")
    ]
    rows += [
        _gate_row("chr1", "all", "raw", 4, 13.0),
        _gate_row("chr1", "all", "raw", 8, 13.5),
    ]
    failures = bench.gate(rows, ratio_plugins={"none"})
    assert len(failures) == 1 and "chr1 all raw" in failures[0]


def test_gate_fails_when_core_lf_is_slower_than_the_ratio_bar(bench):
    rows = [
        r for r in _passing_rows() if not (r["input"] == "chr22" and r["mode"] == "lf")
    ]
    rows.append(_gate_row("chr22", "none", "lf", 8, 1.6, 0.1))
    failures = bench.gate(rows, ratio_plugins={"none"})
    assert len(failures) == 1 and "chr22 none" in failures[0] and "1.21" in failures[0]


def test_gate_checks_the_plugin_ratio_only_when_asked(bench):
    # chr1 all lf/vcf is 2.7: ignored by default, a failure when requested.
    assert bench.gate(_passing_rows(), ratio_plugins={"none"}) == []
    assert len(bench.gate(_passing_rows(), ratio_plugins={"all"})) == 1


def test_gate_fails_on_empty_results(bench):
    assert bench.gate([], ratio_plugins={"none"}) != []


def test_gate_fails_when_a_group_has_no_raw_scaling_pair(bench):
    rows = [
        r
        for r in _passing_rows()
        if not (r["input"] == "chr1" and r["mode"] == "raw" and r["workers"] == 4)
    ]
    failures = bench.gate(rows, ratio_plugins={"none"})
    assert len(failures) == 1 and "chr1 all" in failures[0] and "raw" in failures[0]


def test_gate_fails_when_a_gated_plugin_set_was_not_measured(bench):
    rows = [r for r in _passing_rows() if r["plugins"] != "none"]
    failures = bench.gate(rows, ratio_plugins={"none"})
    assert any("none" in f and "not measured" in f for f in failures)


def test_parse_expect_expands_the_matrix(bench):
    assert bench.parse_expect(["chr22:none,all:raw,lf:4,8"]) == {
        (inp, p, m, w)
        for inp in ["chr22"]
        for p in ["none", "all"]
        for m in ["raw", "lf"]
        for w in [4, 8]
    }


def test_gate_fails_when_an_expected_configuration_is_missing(bench):
    # chr22-only rows cannot pass a gate that expects chr1 too.
    rows = [r for r in _passing_rows() if r["input"] == "chr22"]
    expected = bench.parse_expect(["chr1:all:raw:4,8"])
    failures = bench.gate(rows, ratio_plugins={"none"}, expected=expected)
    assert failures and all("chr1 all raw" in f and "missing" in f for f in failures)


def test_gate_passes_when_every_expected_configuration_is_present(bench):
    expected = bench.parse_expect(["chr22:none:raw,lf,vcf:8", "chr1:all:raw:4,8"])
    expected.discard(("chr22", "none", "raw", 8))
    assert bench.gate(_passing_rows(), ratio_plugins={"none"}, expected=expected) == []


def _with_chr22_plugins():
    return _passing_rows() + [
        _gate_row("chr22", "all", "raw", 4, 4.0),
        _gate_row("chr22", "all", "raw", 8, 2.4),
        _gate_row("chr22", "all", "lf", 8, 20.0),
        _gate_row("chr22", "all", "vcf", 8, 2.6),
    ]


def test_ratio_gate_can_be_scoped_to_one_input(bench):
    # chr1 all (2.7) and chr22 all (7.7) both miss the bar; scoping to chr1
    # reports only chr1, while raw scaling is still checked everywhere.
    scoped = bench.gate(_with_chr22_plugins(), ratio_plugins={"chr1:all"})
    assert len(scoped) == 1 and scoped[0].startswith("chr1 all w8")
    assert len(bench.gate(_with_chr22_plugins(), ratio_plugins={"all"})) == 2


def test_baseline_regressions_flag_a_slower_final(bench):
    base = [
        _gate_row("chr22", "none", "raw", 8, 1.0),
        _gate_row("chr1", "none", "raw", 8, 4.0),
    ]
    final = [
        _gate_row("chr22", "none", "raw", 8, 1.2),
        _gate_row("chr1", "none", "raw", 8, 3.0),
        _gate_row("chr1", "all", "raw", 8, 9.0),
    ]
    found = bench.baseline_regressions(base, final)
    # 1.2 s against 1.0 s is 20% slower; chr1 got faster; chr1 all has no baseline.
    assert len(found) == 1 and "chr22 none raw w8" in found[0]


def test_baseline_regressions_allow_noise_within_the_bar(bench):
    base = [_gate_row("chr22", "none", "raw", 8, 1.0)]
    final = [_gate_row("chr22", "none", "raw", 8, 1.09)]
    assert bench.baseline_regressions(base, final) == []


def test_baseline_regressions_refuse_an_empty_baseline(bench):
    final = [_gate_row("chr22", "none", "raw", 8, 1.0)]
    assert bench.baseline_regressions([], final) != []


def test_baseline_regressions_require_every_expected_configuration(bench):
    base = [_gate_row("chr22", "none", "raw", 8, 1.0)]
    final = [
        _gate_row("chr22", "none", "raw", 8, 1.0),
        _gate_row("chr1", "all", "lf", 8, 9.0),
    ]
    expected = bench.parse_expect(["chr22:none:raw:8", "chr1:all:lf:8"])
    found = bench.baseline_regressions(base, final, expected)
    assert len(found) == 1 and "chr1 all lf w8" in found[0] and "baseline" in found[0]


def test_lf_baseline_comparison_counts_setup(bench):
    # Same stream wall, but setup grew from 0.1 s to 0.5 s: the LazyFrame the
    # user sees is 36% slower, so the lf row must be compared end to end.
    base = [_gate_row("chr22", "none", "lf", 8, 1.0, 0.1)]
    final = [_gate_row("chr22", "none", "lf", 8, 1.0, 0.5)]
    found = bench.baseline_regressions(base, final)
    assert len(found) == 1 and "chr22 none lf w8" in found[0]
