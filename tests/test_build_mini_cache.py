"""Tests for ``scripts/build_mini_cache.py`` — the region mini-cache fixture.

The mini-cache is the vepyr half of the plugin-parity gate: CI cannot ship the
~34 GB production cache, so the gate runs against a region slice. That makes the
slicer load-bearing in a nasty way — **a mini-cache that silently drops a feature
manufactures phantom "core drift"**, and the parity gate's entire job is to tell
core drift apart from plugin bugs. A slicer bug would therefore not fail loudly;
it would quietly invalidate every verdict the gate produces.

So these tests pin the two ways a slice loses data, both on synthetic parquet
tables that mirror the real cache schemas (no 34 GB fixture required):

1. **Interval tables must be overlap-filtered, not containment-filtered.** A
   transcript straddling the region boundary still annotates variants inside the
   region and must survive.
2. **Transcript-owned tables must be filtered by transcript-id membership, not by
   coordinates.** ``exon`` and ``translation_sift`` carry ``start`` / ``end``, but
   their rows are *parts of* a transcript: coordinate-filtering them amputates the
   out-of-region exons of a boundary-crossing transcript, silently changing that
   transcript's structure. ``translation_core`` has no coordinates at all.

Plus the FASTA window, which has the same failure mode in sequence space: a
boundary-crossing transcript's exons are fetched from the FASTA by absolute
coordinate, so the window must carry real bases across the *feature closure*, not
merely the requested region — otherwise those reads silently return ``N``.

The end-to-end proof (annotate a region VCF against the full chr22 cache and
against the mini cache; require byte-identical bodies) lives in
``test_full_vs_mini_annotation_is_identical``, which is skipped unless the built
caches are present, since they are far too large to check in.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from build_mini_cache import (  # noqa: E402
    FilterKind,
    KeptTranscripts,
    Region,
    normalize_chrom,
    overlap_mask,
    write_fasta_window,
)

REGION = Region("chr22", 22_000_000, 23_500_000)


def make_transcript_table() -> pa.Table:
    """Four transcripts: inside, straddling each boundary, and fully outside."""
    return pa.table(
        {
            "chrom": ["22", "22", "22", "22"],
            "start": [22_100_000, 21_900_000, 23_400_000, 24_000_000],
            "end": [22_200_000, 22_050_000, 23_600_000, 24_100_000],
            "stable_id": ["ENST_IN", "ENST_LEFT", "ENST_RIGHT", "ENST_OUT"],
        }
    )


def make_exon_table() -> pa.Table:
    """Exons for the transcripts above, including ones outside the region.

    ``ENST_LEFT``'s first exon sits at 21,900,000 (left of the region) and
    ``ENST_RIGHT``'s last exon at 23,600,000 (right of it). Both belong to
    transcripts that overlap the region and must therefore be kept.
    """
    return pa.table(
        {
            "chrom": ["22"] * 5,
            "start": [22_100_000, 21_900_000, 22_040_000, 23_400_000, 23_600_000],
            "end": [22_100_500, 21_900_500, 22_050_000, 23_400_500, 23_600_500],
            "transcript_id": [
                "ENST_IN",
                "ENST_LEFT",  # OUT of region, but its transcript overlaps
                "ENST_LEFT",
                "ENST_RIGHT",
                "ENST_RIGHT",  # OUT of region, but its transcript overlaps
            ],
        }
    )


class TestOverlapNotContainment:
    """Interval tables keep boundary-crossing features."""

    def test_keeps_transcripts_straddling_both_boundaries(self) -> None:
        table = make_transcript_table()
        kept = table.filter(overlap_mask(table, REGION))
        assert sorted(kept["stable_id"].to_pylist()) == [
            "ENST_IN",
            "ENST_LEFT",
            "ENST_RIGHT",
        ]

    def test_drops_features_fully_outside(self) -> None:
        table = make_transcript_table()
        kept = table.filter(overlap_mask(table, REGION))
        assert "ENST_OUT" not in kept["stable_id"].to_pylist()

    def test_containment_filter_would_lose_real_transcripts(self) -> None:
        """Pin the bug this design avoids: containment drops the straddlers."""
        table = make_transcript_table()
        contained = table.filter(
            pc.and_(
                pc.greater_equal(table["start"], REGION.start),
                pc.less_equal(table["end"], REGION.end),
            )
        )
        assert contained.num_rows == 1  # only ENST_IN
        overlapping = table.filter(overlap_mask(table, REGION))
        assert overlapping.num_rows == 3  # the slicer keeps 2 more

    def test_chrom_prefix_is_normalized(self) -> None:
        """Shards store ``22``; the region says ``chr22``. They must match."""
        assert normalize_chrom("chr22") == normalize_chrom("22") == "22"
        table = make_transcript_table()  # stores bare "22"
        assert table.filter(overlap_mask(table, REGION)).num_rows == 3


class TestAnnotationFlank:
    """Features just OUTSIDE the region still annotate variants inside it.

    Regression test for a bug the full-vs-mini comparison actually caught: with a
    bare (unflanked) overlap filter, ``ENST00000548391`` — which lies 4,723 bp
    before the region start — was dropped, and the two variants at the region's
    left edge silently lost their ``upstream_gene_variant`` CSQ entry. VEP's
    default up/downstream distance is 5000 bp
    (``transcript_distance_config`` → ``.unwrap_or((5000, 5000))``).
    """

    def test_transcript_within_flank_but_outside_region_is_kept(self) -> None:
        upstream = pa.table(
            {
                "chrom": ["22"],
                # Entirely before the region, but only ~4.7 kb before it — exactly
                # the ENST00000548391 case.
                "start": [21_990_000],
                "end": [21_995_277],
                "stable_id": ["ENST_UPSTREAM"],
            }
        )
        assert upstream.filter(overlap_mask(upstream, REGION)).num_rows == 1

    def test_transcript_beyond_the_flank_is_dropped(self) -> None:
        far = pa.table(
            {
                "chrom": ["22"],
                "start": [21_000_000],
                "end": [21_100_000],  # ~900 kb away: far beyond the 5 kb flank
                "stable_id": ["ENST_FAR"],
            }
        )
        assert far.filter(overlap_mask(far, REGION)).num_rows == 0

    def test_unflanked_region_would_drop_it(self) -> None:
        """Pin the bug: flank=0 loses the upstream transcript."""
        unflanked = Region("chr22", 22_000_000, 23_500_000, flank=0)
        upstream = pa.table(
            {
                "chrom": ["22"],
                "start": [21_990_000],
                "end": [21_995_277],
                "stable_id": ["ENST_UPSTREAM"],
            }
        )
        assert upstream.filter(overlap_mask(upstream, unflanked)).num_rows == 0
        assert upstream.filter(overlap_mask(upstream, REGION)).num_rows == 1

    def test_default_flank_matches_the_engine_default(self) -> None:
        from build_mini_cache import DEFAULT_FLANK

        assert DEFAULT_FLANK == 5000
        assert Region("chr22", 22_000_000, 23_500_000).padded == (
            21_995_000,
            23_505_000,
        )


class TestTranscriptMembership:
    """Transcript-owned tables are filtered by id, never by coordinate."""

    def test_keeps_out_of_region_exons_of_kept_transcripts(self) -> None:
        transcripts = make_transcript_table()
        kept_ids = transcripts.filter(overlap_mask(transcripts, REGION))["stable_id"]
        exons = make_exon_table()
        by_membership = exons.filter(
            pc.is_in(exons["transcript_id"], value_set=kept_ids)
        )
        assert by_membership.num_rows == 5  # every exon of the 3 kept transcripts

    def test_coordinate_filtering_exons_would_truncate_transcripts(self) -> None:
        """The phantom-drift bug, pinned: coordinate-filtering amputates exons."""
        exons = make_exon_table()
        by_coordinate = exons.filter(overlap_mask(exons, REGION))
        lost = {
            (tid, start)
            for tid, start in zip(
                exons["transcript_id"].to_pylist(), exons["start"].to_pylist()
            )
        } - {
            (tid, start)
            for tid, start in zip(
                by_coordinate["transcript_id"].to_pylist(),
                by_coordinate["start"].to_pylist(),
            )
        }
        # ENST_LEFT's 21.9 Mb exon and ENST_RIGHT's 23.6 Mb exon would vanish,
        # silently changing the structure of two transcripts we DID keep.
        assert lost == {("ENST_LEFT", 21_900_000), ("ENST_RIGHT", 23_600_000)}

    def test_policy_marks_transcript_owned_tables(self) -> None:
        from build_mini_cache import TABLE_POLICIES

        by_name = {p.name: p for p in TABLE_POLICIES}
        for owned in ("exon", "translation_core"):
            assert by_name[owned].kind is FilterKind.TRANSCRIPT_MEMBERSHIP, (
                f"{owned} must be membership-filtered; coordinate-filtering it "
                "truncates boundary-crossing transcripts"
            )
        # translation_sift has neither coordinates nor a stable id — only the
        # packed `key = (transcript_uid << 32) | position`.
        assert by_name["translation_sift"].kind is FilterKind.TRANSCRIPT_UID_KEY
        for interval in ("variation", "transcript", "regulatory", "motif"):
            assert by_name[interval].kind is FilterKind.INTERVAL


class TestSiftUidKey:
    """``translation_sift`` is sliced through its packed uid key."""

    def test_sift_rows_follow_their_transcript(self) -> None:
        """Only sift rows whose ``key >> 32`` is a kept transcript's uid survive."""
        kept = KeptTranscripts(
            stable_ids=pa.array(["ENST_IN", "ENST_LEFT"]),
            uids=pa.array([7, 9], type=pa.uint32()),
        )
        # uid 7 and 9 are kept; uid 42 belongs to a transcript outside the region.
        keys = [(7 << 32) | 1, (9 << 32) | 5, (42 << 32) | 3, (7 << 32) | 2]
        sift = pa.table({"key": pa.array(keys, type=pa.uint64())})

        uid = pc.shift_right(sift["key"], 32)
        survived = sift.filter(pc.is_in(uid, value_set=kept.uids.cast(uid.type)))

        assert survived["key"].to_pylist() == [
            (7 << 32) | 1,
            (9 << 32) | 5,
            (7 << 32) | 2,
        ]

    def test_uids_are_not_renumbered(self) -> None:
        """Renumbering ``transcript_uid`` would invalidate every sift key.

        The runtime reads ``transcript_uid`` off the transcript row and rebuilds
        ``(uid << 32) | position`` to look SIFT up, so the sliced transcript table
        must keep its ORIGINAL (now sparse) uids.
        """
        transcripts = pa.table(
            {
                "chrom": ["22", "22", "22"],
                "start": [22_100_000, 22_200_000, 24_000_000],
                "end": [22_150_000, 22_250_000, 24_100_000],
                "stable_id": ["A", "B", "OUT"],
                "transcript_uid": pa.array([5564, 4043, 9999], type=pa.uint32()),
            }
        )
        survived = transcripts.filter(overlap_mask(transcripts, REGION))
        # Sparse and unsorted — exactly as they were in the source shard.
        assert survived["transcript_uid"].to_pylist() == [5564, 4043]


class TestFastaWindow:
    """The FASTA window preserves absolute coordinates."""

    @pytest.fixture
    def source_fasta(self, tmp_path: Path) -> Path:
        """A 1000 bp contig of cycling ACGT, with a matching ``.fai``.

        Named bare ``22`` exactly like the real primary-assembly FASTA, while the
        region is ``chr22`` — so the tests also pin that the slicer bridges the
        two spellings without renaming the contig.
        """
        bases = ("ACGT" * 250)[:1000]
        fasta = tmp_path / "ref.fa"
        header = ">22\n"
        lines = [bases[i : i + 60] for i in range(0, len(bases), 60)]
        fasta.write_text(header + "\n".join(lines) + "\n")
        fasta.with_suffix(".fa.fai").write_text(f"22\t1000\t{len(header)}\t60\t61\n")
        return fasta

    def test_source_contig_name_is_preserved(
        self, source_fasta: Path, tmp_path: Path
    ) -> None:
        """The mini FASTA must keep the source spelling (``22``, not ``chr22``).

        Renaming the contig would make the engine resolve names differently
        against the mini FASTA than against the full one — a difference that has
        nothing to do with the slice and would corrupt the parity verdict.
        """
        out = tmp_path / "mini.fa"
        write_fasta_window(
            source_fasta,
            source_fasta.with_suffix(".fa.fai"),
            Region("chr22", 401, 600),
            (401, 600),
            out,
        )
        assert out.read_text().splitlines()[0] == ">22"
        assert out.with_suffix(".fa.fai").read_text().split("\t")[0] == "22"

    def read_seq(self, fasta: Path) -> str:
        return "".join(
            line.strip()
            for line in fasta.read_text().splitlines()
            if not line.startswith(">")
        )

    def test_real_bases_inside_span_n_outside_and_length_preserved(
        self, source_fasta: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "mini.fa"
        region = Region("chr22", 401, 600)
        write_fasta_window(
            source_fasta, source_fasta.with_suffix(".fa.fai"), region, (401, 600), out
        )
        seq = self.read_seq(out)
        original = self.read_seq(source_fasta)

        assert len(seq) == 1000, "contig length must be preserved for coordinates"
        # 1-based [401, 600] -> 0-based [400:600]
        assert seq[400:600] == original[400:600]
        assert set(seq[:400]) == {"N"}
        assert set(seq[600:]) == {"N"}

    def test_fai_is_valid_and_seeks_correctly(
        self, source_fasta: Path, tmp_path: Path
    ) -> None:
        out = tmp_path / "mini.fa"
        write_fasta_window(
            source_fasta,
            source_fasta.with_suffix(".fa.fai"),
            Region("chr22", 401, 600),
            (401, 600),
            out,
        )
        name, length, offset, line_bases, line_width = (
            out.with_suffix(".fa.fai").read_text().strip().split("\t")
        )
        assert (name, int(length), int(line_bases), int(line_width)) == (
            "22",
            1000,
            60,
            61,
        )
        # Seek to 1-based position 401 the way an indexed reader does.
        pos = 401
        byte = int(offset) + (pos - 1) // 60 * 61 + (pos - 1) % 60
        with out.open("rb") as handle:
            handle.seek(byte)
            assert handle.read(1).decode() == self.read_seq(source_fasta)[400]

    def test_window_covers_feature_closure_not_just_region(
        self, source_fasta: Path, tmp_path: Path
    ) -> None:
        """A boundary-crossing transcript's bases must be real, not ``N``.

        The span passed here is the feature closure (300..700), wider than the
        requested region (401..600). Bases at 300 must be real sequence, because
        an exon of a kept transcript lives there.
        """
        out = tmp_path / "mini.fa"
        write_fasta_window(
            source_fasta,
            source_fasta.with_suffix(".fa.fai"),
            Region("chr22", 401, 600),
            (300, 700),
            out,
        )
        seq = self.read_seq(out)
        original = self.read_seq(source_fasta)
        assert seq[299:700] == original[299:700]
        assert seq[298] == "N"


# --------------------------------------------------------------------------
# End-to-end: the mini cache must annotate identically to the full cache.
# --------------------------------------------------------------------------

FULL_CACHE = Path(os.environ.get("VEPYR_FULL_CHR22_CACHE", "/tmp/mini_cache_chr22"))
MINI_CACHE = Path(os.environ.get("VEPYR_MINI_CACHE", "/tmp/mini_cache_region"))
REGION_VCF = Path(
    os.environ.get("VEPYR_REGION_VCF", "/tmp/minicache_work/region_chr22.vcf")
)
FULL_FASTA = Path(
    os.environ.get(
        "VEPYR_FULL_FASTA",
        "/Users/wojtek/Documents/vepyr/_cache_v115/"
        "Homo_sapiens.GRCh38.dna.primary_assembly.fa",
    )
)
ANNOTATE_BIN = Path(
    os.environ.get(
        "VEPYR_ANNOTATE_BIN",
        "/Users/wojtek/Documents/vepyr/datafusion-bio-functions-worktrees/"
        "plugin-engine/target/release/examples/annotate_vcf",
    )
)


def body(vcf: Path) -> list[str]:
    """The VCF's data lines (headers carry cache paths and legitimately differ)."""
    return [ln for ln in vcf.read_text().splitlines() if not ln.startswith("#")]


@pytest.mark.skipif(
    not (
        FULL_CACHE.exists()
        and MINI_CACHE.exists()
        and REGION_VCF.exists()
        and ANNOTATE_BIN.exists()
    ),
    reason="full chr22 + mini caches not built locally (too large to check in)",
)
def test_full_vs_mini_annotation_is_identical(tmp_path: Path) -> None:
    """Annotating the region against the mini cache must equal the full cache.

    This is what makes the fixture trustworthy: if the slice dropped a transcript
    or the FASTA window returned ``N`` where a base belonged, the CSQ bodies
    diverge here rather than silently poisoning the parity gate.
    """
    outputs: list[Path] = []
    for name, cache, fasta in (
        ("full", FULL_CACHE, FULL_FASTA),
        ("mini", MINI_CACHE, MINI_CACHE / FULL_FASTA.name),
    ):
        out = tmp_path / f"{name}.vcf"
        subprocess.run(
            [
                str(ANNOTATE_BIN),
                "--input",
                str(REGION_VCF),
                "--cache",
                str(cache),
                "--out",
                str(out),
                "--fasta",
                str(fasta),
                "--everything",
                "--hgvs",
            ],
            check=True,
            capture_output=True,
        )
        outputs.append(out)

    full_body, mini_body = body(outputs[0]), body(outputs[1])
    assert len(full_body) == len(mini_body), (
        f"record count differs: full={len(full_body)} mini={len(mini_body)}"
    )
    mismatches = [
        (i, f, m) for i, (f, m) in enumerate(zip(full_body, mini_body)) if f != m
    ]
    assert not mismatches, (
        f"{len(mismatches)}/{len(full_body)} records differ between the full and "
        f"mini cache. First: full={mismatches[0][1][:300]!r} "
        f"mini={mismatches[0][2][:300]!r}"
    )
