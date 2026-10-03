import json

from cache_qa import invariants, manifest, profile, report
from cache_qa_synthetic import SyntheticCache


def _inputs(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    return m, invariants.run_all(m), profile.profile_cache(m)


def test_status_aggregation():
    R = invariants.InvariantResult
    assert report.overall_status([R("a", "pass", "")]) == "pass"
    assert report.overall_status([R("a", "pass", ""), R("b", "warn", "")]) == "warn"
    assert report.overall_status([R("a", "warn", ""), R("b", "fail", "")]) == "fail"


def test_report_keys_and_values(tmp_path):
    m, results, prof = _inputs(tmp_path)
    tool = {"vepyr": "0.4.0", "polars": "1.39.3", "schema_version": 1}
    r = report.build_report(m, results, prof, "2026-09-05T12:00:00Z", tool)
    assert set(r) == {
        "plugin",
        "cache_source_version",
        "generated_at",
        "tool",
        "status",
        "invariants",
        "summary",
        "contigs",
        "columns",
    }
    assert r["plugin"] == "demo" and r["status"] == "pass"
    assert r["tool"]["schema_version"] == 1
    assert r["summary"] == {
        "rows": 13,
        "warm": 6,
        "cold": 7,
        "bytes": prof.bytes,
        "contigs": 3,
    }
    assert r["invariants"][0] == {
        "id": "schema",
        "status": "pass",
        "detail": "3 shards match the manifest",
    }
    assert r["contigs"][0]["chrom"] == "chr1"
    assert r["contigs"][0]["warm_share"] == 2 / 6
    col = next(c for c in r["columns"] if c["name"] == "symbol")
    assert col["top_values"][0] == ["GENE1", 6] and col["numeric"] is None


def test_per_contig_only_when_present(tmp_path):
    m, results, prof = _inputs(tmp_path)
    results[1].per_contig = {"chr1": 2}
    r = report.build_report(m, results, prof, "t", report.tool_versions())
    assert r["invariants"][1]["per_contig"] == {"chr1": 2}
    assert "per_contig" not in r["invariants"][0]


def test_write_report_roundtrip(tmp_path):
    m, results, prof = _inputs(tmp_path)
    r = report.build_report(m, results, prof, "t", report.tool_versions())
    out = tmp_path / "qa_profile.json"
    report.write_report(r, out)
    text = out.read_text()
    assert text.endswith("}\n") and json.loads(text) == r


def test_tool_versions_has_polars():
    t = report.tool_versions()
    assert t["schema_version"] == 1 and t["polars"].count(".") >= 1
