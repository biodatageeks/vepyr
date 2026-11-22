import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aggregate_wgs as ag  # noqa: E402


def entry(mode, layout, workers, wall, cpu):
    return {
        "mode": mode,
        "layout": layout,
        "workers": workers,
        "n": 3,
        "wall_s": {"median": wall, "min": wall, "max": wall},
        "cpu_s": {"median": cpu, "min": cpu, "max": cpu},
    }


def write_chrom(root: Path, chrom: str, scale: float, invalid: int = 0):
    d = root / chrom
    d.mkdir(parents=True)
    layouts = [
        entry("warm", "tiered", 1, 10 * scale, 15 * scale),
        entry("warm", "sorted", 1, 13 * scale, 21 * scale),
        entry("cold", "tiered", 1, 12 * scale, 15 * scale),
        entry("cold", "sorted", 1, 14 * scale, 21 * scale),
    ]
    (d / "summary.json").write_text(
        json.dumps({"layouts": layouts, "ratios": {}, "invalid_runs": invalid})
    )


def write_pages(root: Path, chrom: str, tiered: int, sorted_: int):
    d = root / chrom
    d.mkdir(parents=True)
    buffers = {
        "0": {
            "recs": 5000,
            "hit_pages": {
                "tiered": tiered,
                "sorted": sorted_,
                "reduction": 1 - tiered / sorted_,
            },
        },
        "1": {"recs": 10, "hit_pages": {"tiered": 1, "sorted": 2, "reduction": 0.5}},
    }
    (d / "pages.json").write_text(json.dumps({"buffers": buffers}))


def test_per_chromosome_rows(tmp_path):
    write_chrom(tmp_path, "chr1", 2.0)
    write_chrom(tmp_path, "chr22", 1.0)
    rows = ag.chromosome_rows(tmp_path, ["chr1", "chr2", "chr22"])
    assert [r["chrom"] for r in rows] == ["chr1", "chr22"]  # chr2 not done yet
    r = rows[1]
    assert r[("warm", 1)]["tiered"]["wall_s"] == 10.0
    assert r[("warm", 1)]["ratio"]["wall_s"] == pytest.approx(1.3)


def test_totals_sum_medians(tmp_path):
    write_chrom(tmp_path, "chr1", 2.0)
    write_chrom(tmp_path, "chr22", 1.0)
    t = ag.totals(ag.chromosome_rows(tmp_path, ["chr1", "chr22"]))
    w = t[("warm", 1)]
    assert w["tiered"]["wall_s"] == pytest.approx(30.0)
    assert w["sorted"]["wall_s"] == pytest.approx(39.0)
    assert w["ratio"]["wall_s"] == pytest.approx(39.0 / 30.0)
    assert w["reduction"]["wall_s"] == pytest.approx(1 - 30.0 / 39.0)
    assert t["chromosomes"] == 2


def test_invalid_runs_counted(tmp_path):
    write_chrom(tmp_path, "chr1", 1.0, invalid=2)
    rows = ag.chromosome_rows(tmp_path, ["chr1"])
    assert rows[0]["invalid_runs"] == 2
    assert ag.totals(rows)["invalid_runs"] == 2


def test_pages_totals(tmp_path):
    write_pages(tmp_path, "chr1", 300, 900)
    write_pages(tmp_path, "chr22", 100, 400)
    p = ag.pages_totals(tmp_path, ["chr1", "chr22", "chr3"])
    assert p["hit_pages"] == {
        "tiered": 402,
        "sorted": 1304,
        "reduction": pytest.approx(1 - 402 / 1304),
    }
    assert p["chromosomes"] == 2
