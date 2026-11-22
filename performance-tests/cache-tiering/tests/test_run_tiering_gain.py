import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_tiering_gain as rtg  # noqa: E402


def test_schedule_abba():
    assert rtg.schedule(["tiered", "sorted"], [1], 2) == [
        ("tiered", 1, "warmup"),
        ("sorted", 1, "warmup"),
        ("tiered", 1, "r1"),
        ("sorted", 1, "r1"),
        ("sorted", 1, "r2"),
        ("tiered", 1, "r2"),
    ]


def test_schedule_workers_blocks():
    s = rtg.schedule(["tiered", "sorted"], [1, 8], 1)
    assert [w for _, w, _ in s] == [1, 1, 1, 1, 8, 8, 8, 8]


def row(
    layout,
    workers,
    label,
    wall,
    user=10.0,
    sys_=1.0,
    rss=100,
    md5_ok=True,
    exit_status=0,
):
    return {
        "layout": layout,
        "workers": workers,
        "label": label,
        "wall_s": wall,
        "user_s": user,
        "sys_s": sys_,
        "max_rss_kib": rss,
        "annotation_s": wall - 1,
        "md5_ok": md5_ok,
        "exit_status": exit_status,
    }


def test_summary_medians_and_ratio():
    rows = [
        row("tiered", 1, "r1", 10.0, user=8.0),
        row("tiered", 1, "r2", 12.0, user=9.0),
        row("tiered", 1, "r3", 11.0, user=10.0),
        row("sorted", 1, "r1", 22.0, user=18.0),
        row("sorted", 1, "r2", 20.0, user=16.0),
        row("sorted", 1, "r3", 24.0, user=20.0),
    ]
    s = {(x["layout"], x["workers"]): x for x in rtg.summarize(rows)["layouts"]}
    assert s[("tiered", 1)]["n"] == 3
    assert s[("tiered", 1)]["wall_s"]["median"] == 11.0
    assert (
        s[("tiered", 1)]["wall_s"]["min"] == 10.0
        and s[("tiered", 1)]["wall_s"]["max"] == 12.0
    )
    assert s[("sorted", 1)]["cpu_s"]["median"] == 18.0 + 1.0
    ratio = rtg.summarize(rows)["ratios"][1]
    assert ratio["wall_s"] == pytest.approx(22.0 / 11.0)
    assert ratio["cpu_s"] == pytest.approx(19.0 / 10.0)


def test_warmups_not_in_summary():
    rows = [row("tiered", 1, "warmup", 99.0), row("tiered", 1, "r1", 10.0)]
    s = rtg.summarize(rows)["layouts"]
    assert s[0]["n"] == 1 and s[0]["wall_s"]["median"] == 10.0


def test_invalid_runs_excluded():
    rows = [
        row("tiered", 1, "r1", 10.0),
        row("tiered", 1, "r2", 50.0, md5_ok=False),
        row("tiered", 1, "r3", 60.0, exit_status=1),
    ]
    s = rtg.summarize(rows)["layouts"]
    assert s[0]["n"] == 1 and s[0]["wall_s"]["median"] == 10.0
    assert rtg.summarize(rows)["invalid_runs"] == 2


def test_md5_compared_with_first_valid_run():
    ref = {}
    assert rtg.check_md5(ref, 1, {"chr22": "a"}) is True
    assert rtg.check_md5(ref, 1, {"chr22": "a"}) is True
    assert rtg.check_md5(ref, 1, {"chr22": "b"}) is False
    assert (
        rtg.check_md5(ref, 8, {"chr22": "b"}) is True
    )  # separate reference per worker count


def test_refuses_when_disk_low(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(rtg, "free_disk_gib", lambda path: 10.0)
    with pytest.raises(SystemExit, match="disk"):
        rtg.execute(
            rtg.schedule(["tiered", "sorted"], [1], 1),
            run_one=lambda *a: calls.append(a),
            workdir=tmp_path,
            min_disk_gib=40.0,
        )
    assert calls == []


def test_parse_time_file(tmp_path):
    p = tmp_path / "t.txt"
    p.write_text(
        "\tUser time (seconds): 12.50\n\tSystem time (seconds): 1.25\n"
        "\tElapsed (wall clock) time (h:mm:ss or m:ss): 1:02.50\n"
        "\tMaximum resident set size (kbytes): 4096\n\tExit status: 0\n"
    )
    assert rtg.parse_time_file(p) == {
        "user_s": 12.5,
        "sys_s": 1.25,
        "wall_s": 62.5,
        "max_rss_kib": 4096,
        "exit_status": 0,
    }


def test_purged_runs_reported_separately():
    rows = [
        row("tiered", 1, "r1", 10.0),
        row("tiered", 1, "purged", 30.0),
        row("sorted", 1, "purged", 40.0),
    ]
    s = rtg.summarize(rows)
    assert s["layouts"][0]["n"] == 1
    assert {(c["layout"], c["wall_s"]) for c in s["cold_start"]} == {
        ("tiered", 30.0),
        ("sorted", 40.0),
    }
    assert s["invalid_runs"] == 0


def test_execute_shares_md5_reference(tmp_path, monkeypatch):
    monkeypatch.setattr(rtg, "free_disk_gib", lambda path: 100.0)
    ref = {}

    def one(layout, w, label):
        return {**row(layout, w, label, 1.0), "contig_md5": {"chr22": label}}

    rtg.execute([("tiered", 1, "r1")], one, tmp_path, reference=ref)
    rows = rtg.execute([("sorted", 1, "purged")], one, tmp_path, reference=ref)
    assert rows[0]["md5_ok"] is False
