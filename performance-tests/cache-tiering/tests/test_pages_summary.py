import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pages_summary as ps  # noqa: E402

FIX = Path(__file__).resolve().parent / "fixtures"


def test_parse_buffer_rows():
    rows = ps.parse(FIX / "chr22_ext.buf0.txt")
    assert list(rows) == [0]
    r = rows[0]
    assert r["recs"] == 5000
    assert r["cand_pages"] == 964
    assert r["p2_bytes"] == 2798663 and r["p2_ops"] == 8
    assert r["p2_start_pages_in_fetched_full"] == 1217
    assert r["hit_pages"] == 315
    assert r["p3_all_leaf_pages_in_fetched"] == 73330
    assert r["p3_req_ranges"] == 8713


def test_reduction_buffer0():
    out = ps.compare(
        ps.parse(FIX / "chr22_ext.buf0.txt"), ps.parse(FIX / "sorted_ext.buf0.txt")
    )
    b0 = out["buffers"][0]
    assert b0["hit_pages"] == {
        "tiered": 315,
        "sorted": 907,
        "reduction": pytest.approx(1 - 315 / 907),
    }
    assert b0["cand_pages"]["reduction"] == pytest.approx(
        1 - 964 / 907
    )  # negative: more pages decoded
    assert out["range_full_buffers"]["hit_pages"] == [pytest.approx(1 - 315 / 907)] * 2


def test_partial_buffer_excluded_from_range():
    t = {0: {"recs": 5000, "hit_pages": 100}, 1: {"recs": 861, "hit_pages": 10}}
    s = {0: {"recs": 5000, "hit_pages": 200}, 1: {"recs": 861, "hit_pages": 100}}
    out = ps.compare(t, s, metrics=("hit_pages",))
    assert out["range_full_buffers"]["hit_pages"] == [0.5, 0.5]
    assert out["buffers"][1]["hit_pages"]["reduction"] == pytest.approx(0.9)


def test_mismatched_buffers_rejected():
    with pytest.raises(ValueError):
        ps.compare(
            {0: {"recs": 5000, "hit_pages": 1}},
            {0: {"recs": 4999, "hit_pages": 1}},
            metrics=("hit_pages",),
        )
