"""Exercise stateful grid seams through the indexed-input BGZF file sink.

The engine tests isolate the coding-aware weight model. This test protects the
public file path using twelve default 5000-unit buffers with different coding
work, tied-position seams, multi-ALT order, merged-cache replay and BGZF shard
assembly. It requires a cost-dependent grouping, not one worker per buffer.

The existing MT fixture has no proven HGNC donor/recipient carry seam; dedicated
HGNC replay coverage remains in the engine tests.
"""

from __future__ import annotations

import gzip
import re
import shutil
import subprocess
from itertools import zip_longest
from pathlib import Path

import pytest

from tests.cache_metadata import copy_cache_with_source_metadata

FIXTURE = Path(__file__).parent / "data" / "mt_codon_table_merged"


def test_balanced_grid_preserves_bgzf_body_and_input_order(
    tmp_path, monkeypatch, capfd
):
    import vepyr

    bgzip = shutil.which("bgzip")
    tabix = shutil.which("tabix")
    if not (bgzip and tabix):
        pytest.skip("The sharded file path needs bgzip/tabix for indexed input")

    source_cache = FIXTURE / "published"
    if not source_cache.is_dir():
        pytest.skip("Merged golden test cache not available")
    cache = copy_cache_with_source_metadata(
        source_cache, tmp_path / "merged_cache", "merged", "116"
    )

    # The published merged fixture has two noncoding rRNA transcripts at each
    # of MT:1000 and MT:2000 (no CDS). MT:3337 and MT:3388 are the fixture's real
    # missense variants in the Ensembl/RefSeq MT-ND1/ND1 coding exon, 3307-4262.
    # Read REF from the complete 16,569-base fixture FASTA, rather than inventing
    # a reference allele or relabelling a noncoding transcript as coding.
    with gzip.open(FIXTURE / "reference.fa.gz", "rt") as handle:
        reference = "".join(line.strip() for line in handle if not line.startswith(">"))
    assert len(reference) == 16569

    # Unique IDs make loss, duplication and reordering within a tie observable.
    # 56,004 rows plus four extra ALTs give 56,008 logical units: eleven full
    # buffers and one partial buffer. Every interior boundary is inside a tie.
    # More buffers than workers allow costs to change grouped ownership.
    sites = (
        (1000, "C", "G"),
        (2000, "A", "G"),
        (3337, "A", None),
        (3388, "A", None),
    )
    expected = []
    logical_boundaries = [0]
    units = 0
    plain = tmp_path / "grid.vcf"
    with plain.open("wb") as handle:
        handle.write(
            b"##fileformat=VCFv4.2\n"
            b"##contig=<ID=MT,length=16569>\n"
            b"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        )
        for pos, alt, second_alt in sites:
            ref = reference[pos - 1]
            assert ref not in (alt, second_alt)
            for repeat in range(14001):
                alts = alt
                if second_alt is not None and repeat == 1000:
                    alts = f"{alt},{second_alt}"
                elif second_alt is not None and repeat == 2500:
                    alts = f"{second_alt},{alt}"
                identity = tuple(
                    field.encode("ascii")
                    for field in (
                        "MT",
                        str(pos),
                        f"grid_{len(expected):05d}",
                        ref,
                        alts,
                    )
                )
                expected.append(identity)
                units += alts.count(",") + 1
                # Multi-ALT rows lie away from these exact 5000-unit marks.
                if units % 5000 == 0:
                    logical_boundaries.append(len(expected))
                handle.write(b"\t".join(identity) + b"\t50\tPASS\t.\n")
    logical_boundaries.append(len(expected))
    assert len(expected) == 56004
    assert units == 56008
    assert len(logical_boundaries) == 13

    indexed = tmp_path / "grid.vcf.gz"
    with indexed.open("wb") as handle:
        subprocess.run([bgzip, "-c", str(plain)], stdout=handle, check=True)
    subprocess.run([tabix, "-p", "vcf", str(indexed)], check=True)

    monkeypatch.setenv("VEP_GRID_BALANCE", "1")
    monkeypatch.setenv("VEP_PIPELINE_TRACE", "1")
    outputs = []
    for workers in (1, 8):
        capfd.readouterr()
        output = tmp_path / f"workers{workers}.vcf.gz"
        vepyr.annotate(
            str(indexed),
            str(cache),
            reference_fasta=str(FIXTURE / "reference.fa.gz"),
            expected_cache_version="116",
            output_vcf=str(output),
            workers=workers,
            compression="bgzf",
            preserve_record_layout=True,
            show_progress=False,
            # Intentionally omit buffer_size to exercise the public default.
        )
        trace = capfd.readouterr().err
        if workers == 8:
            balance_lines = [
                line
                for line in trace.splitlines()
                if "stage=grid_balance event=done" in line
            ]
            assert len(balance_lines) == 1, trace
            assert "model=input_transcript_coding_exon" in balance_lines[0]
            balance = dict(re.findall(r"(\w+)=(\d+)\b", balance_lines[0]))
            buffers = int(balance["buffers"])
            assert buffers == len(logical_boundaries) - 1 > workers, balance_lines[0]
            assert int(balance["slices"]) == workers, balance_lines[0]

            slice_lines = [
                line
                for line in trace.splitlines()
                if "stage=grid_balance event=slice" in line
            ]
            slices = sorted(
                (
                    {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)\b", line)}
                    for line in slice_lines
                ),
                key=lambda item: item["worker_id"],
            )
            assert len(slices) == workers, trace
            actual_cuts = [item["emit_start_row"] for item in slices]
            actual_cuts.append(slices[-1]["emit_end_row"])
            assert actual_cuts[0] == 0 and actual_cuts[-1] == len(expected)
            assert all(cut in logical_boundaries for cut in actual_cuts), actual_cuts
            for index, item in enumerate(slices):
                assert item["emit_start_row"] < item["emit_end_row"]
                assert item["emit_end_row"] == actual_cuts[index + 1]
            # Equal-buffer planning rounds k * buffers / workers to the nearest
            # boundary. Coding-heavy slices must produce a different grouping;
            # no extra annotation run or timing threshold is needed to prove it.
            uniform_cuts = [
                logical_boundaries[(k * buffers + workers // 2) // workers]
                for k in range(workers + 1)
            ]
            assert actual_cuts != uniform_cuts, (actual_cuts, uniform_cuts)
            assert actual_cuts[2] > uniform_cuts[2], (actual_cuts, uniform_cuts)

            # Both ends overlap two transcripts, but only the last end lies in
            # CDS. Require greater cost per ALT unit there, without pinning the
            # estimator's coefficients or confusing the partial buffer with a
            # cheaper coding model.
            first, last = slices[0], slices[-1]
            coding_start_row = 2 * 14001
            assert first["emit_end_row"] <= coding_start_row
            assert last["emit_start_row"] >= coding_start_row
            first_units, last_units = (
                sum(
                    identity[4].count(b",") + 1
                    for identity in expected[
                        item["emit_start_row"] : item["emit_end_row"]
                    ]
                )
                for item in (first, last)
            )
            assert first["weight"] * last_units < last["weight"] * first_units

            plan_lines = [
                line
                for line in trace.splitlines()
                if "stage=grid_plans event=done" in line
            ]
            assert len(plan_lines) == 1, trace
            plan = dict(re.findall(r"(\w+)=(\d+)\b", plan_lines[0]))
            assert plan["workers"] == balance["slices"], plan_lines[0]

        with output.open("rb") as handle:
            header = handle.read(18)
        assert header[:4] == b"\x1f\x8b\x08\x04"
        assert header[12:14] == b"BC", "Output must be BGZF"
        outputs.append(output)

    # Compare raw decompressed body bytes without retaining the full annotated
    # outputs. Headers contain run metadata, so they are deliberately excluded.
    saw_refseq = False
    saw_coding = False
    saw_noncoding = False
    with gzip.open(outputs[0], "rb") as serial, gzip.open(outputs[1], "rb") as parallel:
        serial_body = (line for line in serial if not line.startswith(b"#"))
        parallel_body = (line for line in parallel if not line.startswith(b"#"))
        for row, (left, right, identity) in enumerate(
            zip_longest(serial_body, parallel_body, expected), start=1
        ):
            assert left is not None and right is not None and identity is not None, row
            assert left == right, f"VCF body differs at input row {row}"
            assert tuple(left.split(b"\t", 5)[:5]) == identity, row
            assert b"CSQ=" in left, f"Missing annotation at input row {row}"
            saw_refseq |= b"|ND1|protein_coding|" in left
            if identity[1] in (b"3337", b"3388"):
                saw_coding |= b"missense_variant" in left
            if identity[1] in (b"1000", b"2000"):
                saw_noncoding |= b"non_coding_transcript_exon_variant" in left
    assert saw_refseq, "The merged fixture must actually annotate RefSeq transcripts"
    assert saw_coding and saw_noncoding, (
        "Both coding and noncoding sites must be annotated"
    )
