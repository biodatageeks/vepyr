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
