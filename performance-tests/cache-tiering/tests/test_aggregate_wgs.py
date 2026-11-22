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


def test_original_arm_in_rows_and_totals(tmp_path):
    d = tmp_path / "chr1"
    d.mkdir()
    layouts = [entry("warm", "tiered", 1, 10.0, 15.0), entry("warm", "sorted", 1, 13.0, 21.0),
               entry("warm", "original", 1, 11.0, 16.0)]  # fmt: skip
    (d / "summary.json").write_text(json.dumps({"layouts": layouts, "invalid_runs": 0}))
    rows = ag.chromosome_rows(tmp_path, ["chr1"])
    assert rows[0][("warm", 1)]["ratio_original"]["wall_s"] == pytest.approx(1.1)
    t = ag.totals(rows)[("warm", 1)]
    assert t["original"]["wall_s"] == pytest.approx(11.0)
    assert t["ratio_original"]["wall_s"] == pytest.approx(1.1)


def test_render_shows_original_ratio(tmp_path):
    d = tmp_path / "chr1"
    d.mkdir()
    layouts = [entry("warm", "tiered", 1, 10.0, 15.0), entry("warm", "sorted", 1, 13.0, 21.0),
               entry("warm", "original", 1, 11.0, 16.0)]  # fmt: skip
    (d / "summary.json").write_text(json.dumps({"layouts": layouts, "invalid_runs": 0}))
    rows = ag.chromosome_rows(tmp_path, ["chr1"])
    text = ag.render(rows, ag.totals(rows), None)
    assert "orig 1.10" in text
    assert "original / tiered" in text


FIX = Path(__file__).resolve().parent / "fixtures"


def make_page_dir(root: Path, chrom: str):
    d = root / chrom
    d.mkdir(parents=True)
    (d / "tiered_ext.txt").write_text((FIX / "chr22_ext.buf0.txt").read_text())
    (d / "sorted_ext.txt").write_text((FIX / "sorted_ext.buf0.txt").read_text())
    (d / "original_ext.txt").write_text((FIX / "chr22_ext.buf0.txt").read_text())


def test_page_rows_per_layout(tmp_path):
    make_page_dir(tmp_path, "chr22")
    rows = ag.page_rows(tmp_path, ["chr1", "chr22"], ["tiered", "sorted", "original"])
    assert list(rows) == ["chr22"]
    t, s = rows["chr22"]["tiered"], rows["chr22"]["sorted"]
    assert (t["hit_pages"], s["hit_pages"]) == (315, 907)
    assert (t["find_decoded"], s["find_decoded"]) == (964, 907)
    assert (t["read_decoded"], s["read_decoded"]) == (8713, 25517)
    assert t["decoded"] == 964 + 8713


def test_page_totals_and_render(tmp_path):
    make_page_dir(tmp_path, "chr21")
    make_page_dir(tmp_path, "chr22")
    rows = ag.page_rows(tmp_path, ["chr21", "chr22"], ["tiered", "sorted", "original"])
    tot = ag.page_totals_by_layout(rows)
    assert tot["tiered"]["hit_pages"] == 630
    text = ag.render_pages(rows, tot)
    assert "| chr22 | 315 / 907 / 315 |" in text
    assert "total" in text
