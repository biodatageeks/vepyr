import json

import pytest

from comparison import report


def make_chrom_report(chrom, *, time_s=1.0, consequence_mismatches=0):
    return {
        "chrom": chrom,
        "profile": "merged",
        "release": "115",
        "input_variants": 100,
        "annotation": {
            "backend": "parquet",
            "compression": "plain",
            "time_s": time_s,
            "output_variants": 100,
        },
        "comparison": {
            "variants_compared": 100,
            "variants_only_in_vepyr": 0,
            "variants_only_in_vep": 0,
            "csq_entry_count_match": 100,
            "csq_entry_count_mismatch": 0,
            "csq_order_mismatch": 0,
            "csq_order_ignored": 0,
            "field_match_rates": {"Consequence": 99.0, "IMPACT": 100.0},
            "field_mismatch_counts": (
                {"Consequence": consequence_mismatches}
                if consequence_mismatches
                else {}
            ),
            "field_mismatch_examples": (
                {
                    "Consequence": [
                        {
                            "variant": f"{chrom}\t100\tA\tT",
                            "vepyr": "stop_gained",
                            "vep": "frameshift_variant",
                        }
                    ]
                }
                if consequence_mismatches
                else {}
            ),
            "field_order_mismatch_counts": {},
            "field_order_mismatch_examples": {},
            "equality_bucket_counts": {
                "both_empty": 10,
                "both_nonempty_equal": 188,
                "vepyr_empty_only": 1,
                "vep_empty_only": 1,
                "both_nonempty_unequal": consequence_mismatches,
            },
            "mismatch_ledger": {
                "path": f"/reports/{chrom}.jsonl",
                "rows": consequence_mismatches,
                "sha256": f"{int(chrom.removeprefix('chr')):064x}",
            },
        },
    }


def test_report_json_path_includes_the_release():
    path = report.report_json_path("/reports", "chr1", "merged", "115")
    assert path.endswith("fast_chr1_merged_115_report.json")


def test_report_paths_for_two_releases_do_not_collide():
    a = report.report_json_path("/reports", "chr1", "merged", "115")
    b = report.report_json_path("/reports", "chr1", "merged", "116")
    assert a != b


def test_mismatch_ledger_path_includes_release():
    path = report.mismatch_ledger_path("/reports", "chr1", "merged", "116")
    assert path.endswith("fast_chr1_merged_116_mismatches.jsonl")


def test_contig_span_summarises_a_contiguous_range():
    assert report.contig_span(["chr1", "chr2", "chr22"]) == "chr1_chr22"
    assert report.contig_span(["chr7"]) == "chr7"


def test_load_reports_prefers_the_release_qualified_name(tmp_path):
    modern = tmp_path / "fast_chr1_merged_115_report.json"
    modern.write_text(json.dumps(make_chrom_report("chr1")))
    legacy = tmp_path / "fast_chr1_merged_report.json"
    legacy.write_text(json.dumps({"chrom": "legacy"}))
    loaded = report.load_reports(str(tmp_path), ["chr1"], "merged", "115")
    assert len(loaded) == 1
    assert loaded[0]["chrom"] == "chr1"


def test_load_reports_falls_back_to_the_legacy_name(tmp_path, capsys):
    legacy = tmp_path / "fast_chr1_merged_report.json"
    legacy.write_text(json.dumps(make_chrom_report("chr1")))
    loaded = report.load_reports(str(tmp_path), ["chr1"], "merged", "115")
    assert len(loaded) == 1
    assert "legacy" in capsys.readouterr().out.lower()


def test_aggregate_sums_across_chromosomes():
    reports = [
        make_chrom_report("chr1", consequence_mismatches=2),
        make_chrom_report("chr2", consequence_mismatches=3),
    ]
    agg = report.aggregate_mismatches(reports)
    assert agg["total_compared"] == 200
    assert agg["field_mm"]["Consequence"] == 5
    assert len(agg["field_examples"]["Consequence"]) == 2
    assert {e["source_chrom"] for e in agg["field_examples"]["Consequence"]} == {
        "chr1",
        "chr2",
    }
    assert agg["total_ledger_rows"] == 5
    assert agg["equality_buckets"]["both_nonempty_equal"] == 376


def test_classify_routes_stop_gained_missing():
    examples = [
        {"vepyr": "frameshift_variant", "vep": "stop_gained&frameshift_variant"}
    ]
    classes = report.classify_consequence_mismatches(examples)
    assert "stop_gained_missing" in classes


def test_generate_markdown_names_the_release_and_profile():
    reports = [make_chrom_report("chr1")]
    agg = report.aggregate_mismatches(reports)
    md = report.generate_markdown(
        reports,
        agg,
        report.classify_consequence_mismatches([]),
        None,
        {"branch": "main", "vepyr_rev": "abc1234", "bio_functions_rev": "def5678"},
        release="115",
        profile="merged",
    )
    assert "release 115" in md
    assert "profile merged" in md
    assert "## Per-Chromosome Performance" in md


def test_generate_markdown_survives_an_all_reused_run():
    """Every time_s is None when nothing was re-annotated; must not divide by zero."""
    reports = [make_chrom_report("chr1", time_s=None)]
    agg = report.aggregate_mismatches(reports)
    md = report.generate_markdown(
        reports,
        agg,
        {},
        None,
        {},
        release="115",
        profile="merged",
    )
    assert "Per-Chromosome Performance" in md


@pytest.mark.parametrize(
    ("status", "dirty"),
    [
        ("?? .cargo-ok", False),
        ("?? .cargo-ok\n M src/lib.rs", True),
        ("?? .cargo-ok\n?? generated.rs", True),
    ],
)
def test_git_checkout_info_ignores_only_cargo_bookkeeping(monkeypatch, status, dirty):
    def command_output(command, cwd):
        if command == ["git", "rev-parse", "--show-toplevel"]:
            return "/cargo/git/checkouts/example/abc123"
        if command == ["git", "rev-parse", "HEAD"]:
            return "abc123"
        if command == ["git", "status", "--porcelain", "--untracked-files=all"]:
            return status
        raise AssertionError(command)

    monkeypatch.setattr(report, "_command_output", command_output)

    assert report._git_checkout_info("/cargo/git/checkouts/example/abc123") == {
        "repo_root": "/cargo/git/checkouts/example/abc123",
        "revision": "abc123",
        "dirty": dirty,
    }
