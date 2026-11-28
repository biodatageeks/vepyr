from cache_qa import card


def _col(name, role, dtype, **kw):
    base = {
        "name": name,
        "role": role,
        "dtype": dtype,
        "null_share": 0.0,
        "empty_share": None,
        "distinct": None,
        "approx": False,
        "top_values": None,
        "numeric": None,
        "per_contig": {},
    }
    base.update(kw)
    return base


def _report(status="pass"):
    return {
        "plugin": "demo",
        "generated_at": "2026-09-05T12:00:00Z",
        "tool": {"vepyr": "0.4.0", "polars": "1.39.3", "schema_version": 1},
        "status": status,
        "invariants": [
            {"id": "schema", "status": "pass", "detail": "3 shards match the manifest"},
            {
                "id": "duplicates",
                "status": "warn",
                "detail": "412 duplicate keys (assume_unique)",
                "per_contig": {"chr1": 400},
            },
        ],
        "summary": {
            "rows": 4439569,
            "warm": 113018,
            "cold": 4326551,
            "bytes": 85012345,
            "contigs": 2,
        },
        "contigs": [
            {
                "chrom": "chr1",
                "file": "chr1.parquet",
                "rows": 401099,
                "warm": 10445,
                "cold": 390654,
                "warm_share": 0.026,
                "bytes": 7400000,
                "start_min": 1,
                "start_max": 2,
            },
            {
                "chrom": "chrMT",
                "file": "chrMT.parquet",
                "rows": 0,
                "warm": 0,
                "cold": 0,
                "warm_share": 0.0,
                "bytes": 0,
                "start_min": None,
                "start_max": None,
            },
        ],
        "columns": [
            _col("chrom", "key", "String"),
            _col(
                "clnsig",
                "value",
                "String",
                empty_share=0.0012,
                distinct=31,
                top_values=[
                    ["Uncertain_significance", 1900000],
                    ["Likely_benign", 1200000],
                ],
            ),
            _col(
                "am",
                "value",
                "Float32",
                distinct=68000000,
                approx=True,
                numeric={
                    "parsable_share": 1.0,
                    "min": 0.0,
                    "max": 1.0,
                    "mean": 0.3,
                    "p50": 0.121,
                    "p95": 0.912,
                },
            ),
        ],
    }


def test_render_contains_all_three_tables():
    s = card.render_section(_report())
    assert s.startswith("## Quality profile")
    assert "Generated 2026-09-05 by `profile_plugin_cache.py` " in s
    assert "(vepyr 0.4.0, Polars 1.39.3)" in s
    assert "| schema | ✅ pass | 3 shards match the manifest |" in s
    assert "| duplicates | ⚠️ warn | 412 duplicate keys (assume_unique) |" in s
    assert "| chr1 | 401,099 | 10,445 | 390,654 | 2.6% | 7.4 MB |" in s
    assert (
        "| **total** | **4,439,569** | **113,018** | **4,326,551** | **2.5%** "
        "| **85 MB** |"
    ) in s
    assert (
        "| clnsig | value | String | 0.00 | 0.12 | 31 | — "
        "| Uncertain_significance (1.9M), Likely_benign (1.2M) |"
    ) in s
    assert (
        "| am | value | Float32 | 0.00 | — | ~68M | 0.000 / 0.121 / 0.912 / 1.000 | — |"
        in s
    )
    assert "| chrom |" not in s


def test_render_marks_fail():
    r = _report("fail")
    r["invariants"][0]["status"] = "fail"
    assert "| schema | ❌ fail |" in card.render_section(r)


def test_formatters():
    assert card.format_int(4439569) == "4,439,569"
    assert card.format_bytes(7400000) == "7.4 MB"
    assert card.format_bytes(85012345) == "85 MB"
    assert card.format_bytes(2237109762) == "2.2 GB"
    assert card.format_bytes(0) == "0 B"
    assert card.format_pct(0.026) == "2.6%"
    assert card.format_count_short(1900000) == "1.9M"
    assert card.format_count_short(68000000) == "68M"
    assert card.format_count_short(412) == "412"


def test_splice_inserts_before_usage():
    readme = "# T\n\n## Contents\n\nx\n\n## Usage\n\ny\n"
    out = card.splice(readme, "## Quality profile\n\nq\n")
    assert out.index(card.START) < out.index("## Usage")
    assert out.count(card.START) == 1 and out.count(card.END) == 1
    assert "## Contents\n\nx\n" in out and out.endswith("## Usage\n\ny\n")


def test_splice_replaces_between_markers_idempotently():
    readme = "# T\n\n## Usage\n\ny\n"
    once = card.splice(readme, "## Quality profile\n\nv1\n")
    twice = card.splice(once, "## Quality profile\n\nv2\n")
    assert "v1" not in twice and "v2" in twice
    assert twice.count(card.START) == 1
    assert card.splice(twice, "## Quality profile\n\nv2\n") == twice


def test_splice_appends_without_usage():
    out = card.splice("# T\n\nbody\n", "## Quality profile\n\nq\n")
    assert out.startswith("# T\n\nbody\n") and out.rstrip().endswith(card.END)
