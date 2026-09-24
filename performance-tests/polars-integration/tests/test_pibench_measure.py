import sys

import pytest

from pibench import measure, parity


def test_run_once_reports_wall_rss_and_exit(tmp_path):
    r = measure.run_once(
        [sys.executable, "-c", "x = bytearray(50_000_000)"], tmp_path / "a"
    )
    assert r["exit"] == 0
    assert r["wall_s"] > 0
    assert r["max_rss_bytes"] > 50_000_000
    assert (tmp_path / "a.stderr.txt").exists()


def test_repeat_discards_warmups_and_takes_the_median(tmp_path):
    out = measure.repeat(
        [sys.executable, "-c", "pass"], tmp_path, warmups=1, repeats=3, max_load=1e9
    )
    assert len(out["warmups"]) == 1 and len(out["runs"]) == 3
    assert out["min_wall_s"] <= out["median_wall_s"] <= out["max_wall_s"]


def test_repeat_raises_on_a_failing_command(tmp_path):
    with pytest.raises(RuntimeError):
        measure.repeat(
            [sys.executable, "-c", "raise SystemExit(3)"],
            tmp_path,
            warmups=0,
            repeats=1,
            max_load=1e9,
        )


def test_summarize_median_of_three():
    runs = [
        {"wall_s": w, "max_rss_bytes": m} for w, m in ((3.0, 30), (1.0, 10), (2.0, 20))
    ]
    s = measure.summarize(runs)
    assert s["median_wall_s"] == 2.0 and s["median_rss_bytes"] == 20


def test_vcf_keys_and_body(tmp_path):
    p = tmp_path / "a.vcf"
    p.write_text(
        "##x\n#CHROM\tPOS\tID\tREF\tALT\nchr22\t10\t.\tA\tG\tq\nchr22\t12\t.\tC\tT\tq\n"
    )
    assert parity.vcf_keys(p) == [("chr22", 10, "A", "G"), ("chr22", 12, "C", "T")]
    assert parity.vcf_body(p) == ["chr22\t10\t.\tA\tG\tq", "chr22\t12\t.\tC\tT\tq"]


def test_compare_reports_the_first_difference():
    c = parity.compare([1, 2, 3], [1, 9, 3])
    assert not c["equal"] and c["first_diff"]["index"] == 1
    assert parity.compare([1], [1])["equal"]
