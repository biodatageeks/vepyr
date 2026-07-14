#!/usr/bin/env python3
"""Slice a chromosome-scoped vepyr Parquet cache down to a small region fixture.

Why this exists
---------------
The parity gate (real VEP + Perl plugin vs. vepyr + built plugin cache) needs a
cache in CI. The production cache is ~34 GB (``variation/chr22.parquet`` alone is
541 MB; the primary-assembly FASTA is 2.9 GB), which is far too large to publish
as a release asset. This script produces a *region mini-cache*: tens of MB,
downloadable in CI, and — critically — annotation-identical to the full cache
over the region it covers.

Build, don't slice-from-the-old-cache
-------------------------------------
This script slices a cache produced by the CURRENT engine builders
(``build_parquet_variation_chrom`` / ``build_parquet_context_chrom`` /
``build_parquet_translation_sift_chrom``). It cannot slice the legacy
``_cache_v115/parquet/115_GRCh38_vep`` tree, which predates the engine's tiering
work: its ``variation`` shard has 76 columns and none of them is ``tier``, while
``plugin_cache::join`` issues ``SELECT chrom, start, allele_string, tier FROM
<variation shard>``. That legacy tree also ships no ``chrom_manifest.json``, so
``PartitionedParquetCache::detect`` does not even recognise it. Feed this script
a freshly built cache root.

The two traps this script exists to avoid
-----------------------------------------
A mini-cache that silently drops a feature manufactures phantom "core drift" and
poisons the parity gate, whose whole job is to separate core drift from plugin
bugs. Two ways that happens, both handled explicitly below:

1. **Not every table is coordinate-sliceable.** ``translation_core`` has *no*
   coordinate columns at all — only ``transcript_id``. ``exon`` has ``start`` /
   ``end``, but its rows belong to transcripts: coordinate-filtering exons would
   amputate the exons of a kept transcript that happen to lie outside the region,
   silently changing that transcript's structure. So the slicer filters those
   tables by **transcript-id membership**, not coordinates. See ``TABLE_POLICIES``.

2. **Interval tables must be filtered by overlap, not containment.** A transcript
   spanning the region boundary must be KEPT. Dropping it would delete real
   annotation; keeping it means the region's *feature closure* extends past the
   requested window — which is why the FASTA window is widened to cover every
   kept feature (see ``feature_closure_span``). Without that widening, sequence
   and HGVS lookups for a boundary-overlapping transcript would silently read
   ``N`` instead of reference bases.

Publication
-----------
The cache root is meant to be published as a **GitHub release asset on the
``vepyr`` repository**, named ``mini-cache-chr22-22000000-23500000.tar.gz``, and
downloaded by the parity-gate CI job. Publishing is outward-facing and is NOT done
by this script: it builds the fixture locally and prints its checksum, and a human
decides whether to upload it.

    tar czf mini-cache-chr22-22000000-23500000.tar.gz -C /tmp mini_cache_region

For chr22:22,000,000-23,500,000 that yields ~63 MB extracted (49 MB of which is
the coordinate-preserving FASTA, almost all ``N`` and thus nearly free to
compress) → **~14 MB** as the release asset, against ~29 GB + a 2.9 GB FASTA for
the production cache.

Region
------
``chr22:22,000,000-23,500,000`` is not arbitrary: it contains
``chr22:22,893,742 C>G``, the locus on which AlphaMissense parity was manually
confirmed, so the regression test built on this fixture is meaningful.

Usage
-----
Build a complete single-chromosome cache with the engine's builders first (this
script slices, it does not build)::

    build_parquet_variation_chrom       --chrom chr22 --cache-source-type ensembl ...
    build_parquet_context_chrom --entity {transcript,exon,regulatory,motif,translation_core} ...
    build_parquet_translation_sift_chrom --chrom chr22 ...

then slice it::

    python scripts/build_mini_cache.py \\
        --source-cache /tmp/mini_cache_chr22 \\
        --fasta /path/to/Homo_sapiens.GRCh38.dna.primary_assembly.fa \\
        --output /tmp/mini_cache_region \\
        --chrom chr22 --start 22000000 --end 23500000

``tests/test_build_mini_cache.py`` proves the slice is faithful by annotating a
region VCF against the full chromosome cache and against the mini cache and
requiring byte-identical bodies.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from datafusion import SessionContext

CHROM_MANIFEST_FILE: Final = "chrom_manifest.json"
"""Per-entity manifest filename the runtime reads (``cache::manifest``)."""

FASTA_LINE_WIDTH: Final = 60
"""Bases per line in the emitted FASTA. The ``.fai`` is generated to match."""

MISSING_IN_SOURCE: Final = -1
"""Row count reported for a table the source cache has no shard for.

Distinct from ``0`` (a shard that exists but retained no rows in the region), so
"the source never had this table" is never confused with "the slice emptied it".
"""


class FilterKind(Enum):
    """How a cache table is reduced to the region."""

    INTERVAL = "interval"
    """Keep rows whose ``[start, end]`` OVERLAPS the region (not containment)."""

    TRANSCRIPT_MEMBERSHIP = "transcript_membership"
    """Keep rows whose ``transcript_id`` (an ENST stable id) belongs to a kept
    transcript.

    Used for tables whose rows are *parts of* a transcript. Coordinate-filtering
    them would truncate transcripts that overlap the region boundary.
    """

    TRANSCRIPT_UID_KEY = "transcript_uid_key"
    """Keep rows whose packed ``key`` belongs to a kept transcript.

    ``translation_sift`` has no ``transcript_id`` and no coordinates: it is a
    point-lookup table keyed by ``key = (transcript_uid << 32) | protein_position``
    (``cache::build::compact_translation_sift_position_schema``). So membership is
    tested on ``key >> 32`` against the kept transcripts' ``transcript_uid``.

    The uids are deliberately NOT renumbered when the transcript table is sliced:
    the runtime reads ``transcript_uid`` straight off the transcript row and
    reconstructs this key for a point lookup, so a sparse surviving subset of the
    original uids resolves correctly, while renumbering would break every key.
    """


@dataclass(frozen=True, slots=True)
class TablePolicy:
    """The declared slicing rule for one cache entity directory."""

    name: str
    kind: FilterKind
    required_columns: tuple[str, ...]
    defines_transcript_set: bool = False
    """True for ``transcript``: its surviving ``stable_id`` values drive the
    membership filter applied to ``exon`` / ``translation_core`` /
    ``translation_sift``."""


TABLE_POLICIES: Final[tuple[TablePolicy, ...]] = (
    # Interval tables — overlap-filtered on their own coordinates.
    TablePolicy("variation", FilterKind.INTERVAL, ("chrom", "start", "end", "tier")),
    TablePolicy(
        "transcript",
        FilterKind.INTERVAL,
        ("chrom", "start", "end", "stable_id", "transcript_uid"),
        defines_transcript_set=True,
    ),
    TablePolicy("regulatory", FilterKind.INTERVAL, ("chrom", "start", "end")),
    TablePolicy("motif", FilterKind.INTERVAL, ("chrom", "start", "end")),
    # Transcript-owned tables. `exon` DOES carry start/end, but slicing it on
    # coordinates would drop the out-of-region exons of a boundary-overlapping
    # transcript and thus silently change its structure. `translation_core` has no
    # coordinate columns at all, and `translation_sift` has neither coordinates nor
    # a stable id — only the packed `key`.
    TablePolicy("exon", FilterKind.TRANSCRIPT_MEMBERSHIP, ("transcript_id",)),
    TablePolicy(
        "translation_core", FilterKind.TRANSCRIPT_MEMBERSHIP, ("transcript_id",)
    ),
    TablePolicy("translation_sift", FilterKind.TRANSCRIPT_UID_KEY, ("key",)),
)

SIFT_UID_SHIFT: Final = 32
"""``translation_sift.key`` packs ``(transcript_uid << 32) | protein_position``."""

DEFAULT_FLANK: Final = 5_000
"""Bases to pad the region by when selecting interval features.

VEP reports ``upstream_gene_variant`` / ``downstream_gene_variant`` for
transcripts within a flank of the variant — the engine's default is
``(5000, 5000)`` (``annotate_provider::transcript_distance_config`` →
``.unwrap_or((5000, 5000))``, settable with ``--distance``).

So a transcript lying entirely OUTSIDE the region can still annotate a variant
INSIDE it. Selecting features by bare region overlap drops those transcripts and
silently deletes real CSQ entries: the full-vs-mini test caught exactly this,
losing the ``upstream_gene_variant`` on ``ENST00000548391`` (4,723 bp before the
region start) for the variants at the region's left edge. Pad by at least the
flank you will annotate with.
"""


@dataclass(frozen=True, slots=True)
class Region:
    """A closed 1-based genomic interval, plus the annotation flank around it.

    ``start``/``end`` are the region the fixture is *for*; ``flank`` widens the
    window used to SELECT interval features, because VEP annotates a variant
    against transcripts up to ``flank`` bases away (upstream/downstream gene
    variants). The two are deliberately distinct: the VCF covers the region, but
    the cache must carry the features the region's variants can reach.
    """

    chrom: str
    start: int
    end: int
    flank: int = DEFAULT_FLANK

    def __post_init__(self) -> None:
        if self.start < 1 or self.end < self.start:
            raise ValueError(f"invalid region {self.chrom}:{self.start}-{self.end}")
        if self.flank < 0:
            raise ValueError(f"flank must be non-negative, got {self.flank}")

    @property
    def padded(self) -> tuple[int, int]:
        """The region widened by ``flank`` on both sides, clamped at 1."""
        return max(1, self.start - self.flank), self.end + self.flank

    @property
    def label(self) -> str:
        """A filename-safe rendering, e.g. ``chr22_22000000_23500000``."""
        return f"{self.chrom}_{self.start}_{self.end}"


def normalize_chrom(value: str) -> str:
    """Strip a ``chr`` prefix so ``chr22`` and ``22`` compare equal.

    Cache shards have historically stored the bare label (``"22"``) while the
    manifest and CLI use ``"chr22"``; comparing without normalising would filter
    every row away and yield a silently empty — but structurally valid — cache.
    """
    return value.removeprefix("chr")


def overlap_mask(table: pa.Table, region: Region) -> pa.ChunkedArray:
    """Boolean mask selecting rows whose interval OVERLAPS the FLANKED region.

    Two things this must get right, both of which silently delete real annotation
    if got wrong:

    * **Overlap, not containment**: a feature starting before ``region.start`` or
      ending after ``region.end`` still annotates variants inside the region.
    * **Flanked, not bare**: a feature lying wholly outside the region but within
      ``region.flank`` of it still produces upstream/downstream consequences for
      variants at the region's edges.
    """
    lo, hi = region.padded
    chrom = table["chrom"]
    stripped = pc.if_else(
        pc.starts_with(chrom, pattern="chr"),
        pc.utf8_slice_codeunits(chrom, start=3),
        chrom,
    )
    same_chrom = pc.equal(stripped, normalize_chrom(region.chrom))
    starts_before_end = pc.less_equal(table["start"], hi)
    ends_after_start = pc.greater_equal(table["end"], lo)
    return pc.and_(same_chrom, pc.and_(starts_before_end, ends_after_start))


def write_manifest(entity_dir: Path, entries: Sequence[dict[str, object]]) -> None:
    """Write an entity's ``chrom_manifest.json`` in the runtime's shape."""
    entity_dir.mkdir(parents=True, exist_ok=True)
    (entity_dir / CHROM_MANIFEST_FILE).write_text(
        json.dumps(list(entries), indent=2) + "\n"
    )


@dataclass(frozen=True, slots=True)
class KeptTranscripts:
    """The transcripts that survived the region slice, in both identifier spaces.

    ``stable_ids`` (ENST…) key ``exon`` / ``translation_core``; ``uids`` (the
    ``transcript_uid`` column) key ``translation_sift`` via its packed ``key``.
    """

    stable_ids: pa.Array
    uids: pa.Array


def slice_table(
    source: Path,
    policy: TablePolicy,
    region: Region,
    kept: KeptTranscripts | None,
) -> pa.Table:
    """Apply ``policy`` to one parquet shard and return the surviving rows."""
    table = pq.read_table(source)
    missing = [c for c in policy.required_columns if c not in table.schema.names]
    if missing:
        raise ValueError(
            f"{source}: table '{policy.name}' is missing required column(s) {missing}; "
            f"present columns: {table.schema.names}"
        )

    if policy.kind is not FilterKind.INTERVAL and kept is None:
        raise RuntimeError(
            f"table '{policy.name}' needs the kept-transcript set, but the "
            "'transcript' table was not sliced first"
        )

    match policy.kind:
        case FilterKind.INTERVAL:
            return table.filter(overlap_mask(table, region))
        case FilterKind.TRANSCRIPT_MEMBERSHIP:
            assert kept is not None
            return table.filter(
                pc.is_in(table["transcript_id"], value_set=kept.stable_ids)
            )
        case FilterKind.TRANSCRIPT_UID_KEY:
            assert kept is not None
            # `key` is uint64 and `transcript_uid` uint32; `is_in` will not compare
            # across widths, so lift the uid set to the key's type.
            uid = pc.shift_right(table["key"], SIFT_UID_SHIFT)
            uids = kept.uids.cast(uid.type)
            return table.filter(pc.is_in(uid, value_set=uids))


def feature_closure_span(kept: pa.Table, region: Region) -> tuple[int, int]:
    """Widen ``region`` to cover every kept interval feature.

    A transcript overlapping the region boundary extends past it, and its exon
    sequences are fetched from the FASTA by absolute coordinate. If the FASTA
    carried real bases only across ``region``, those lookups would read ``N`` and
    manufacture annotation differences. So the FASTA window spans the union of
    the region and every kept feature.
    """
    lo, hi = region.padded
    if kept.num_rows:
        lo = min(lo, pc.min(kept["start"]).as_py())
        hi = max(hi, pc.max(kept["end"]).as_py())
    return lo, hi


def iter_fasta_record(
    fasta: Path, fai: Path, contig: str
) -> tuple[str, int, Iterator[bytes]]:
    """Stream one contig's bases from an indexed FASTA.

    Returns the contig's name **as spelled in the source** (the primary-assembly
    FASTA uses bare ``22`` while the cache and VCF use ``chr22``; the mini FASTA
    must keep the source spelling so the engine's contig resolution takes exactly
    the same path against both FASTAs), its length, and an iterator over raw base
    chunks with newlines stripped.
    """
    wanted = normalize_chrom(contig)
    for line in fai.read_text().splitlines():
        name, length, offset, line_bases, line_width = line.split("\t")
        if normalize_chrom(name) != wanted:
            continue
        length, offset = int(length), int(offset)
        line_bases, line_width = int(line_bases), int(line_width)

        def _chunks() -> Iterator[bytes]:
            with fasta.open("rb") as handle:
                handle.seek(offset)
                remaining = length
                while remaining > 0:
                    take = min(line_bases, remaining)
                    row = handle.read(line_width)
                    yield row[:take]
                    remaining -= take

        return name, length, _chunks()
    raise KeyError(f"contig '{contig}' not found in {fai}")


def write_fasta_window(
    fasta: Path,
    fai: Path,
    region: Region,
    span: tuple[int, int],
    out_fasta: Path,
) -> None:
    """Emit a coordinate-preserving FASTA: real bases across ``span``, ``N`` elsewhere.

    The contig keeps its full length and its original name so that every absolute
    coordinate in the cache still resolves. Only ``span`` carries real sequence;
    the rest is ``N`` and compresses to almost nothing in the release tarball.
    A matching ``.fai`` is written alongside.
    """
    span_lo, span_hi = span
    contig, length, chunks = iter_fasta_record(fasta, fai, region.chrom)
    span_lo = max(1, span_lo)
    span_hi = min(span_hi, length)

    # Assemble the contig in memory: `N` everywhere, real bases across the span.
    # Done with bulk slice assignment rather than per-base Python loops — the
    # contig is ~50 Mb and a byte-at-a-time loop would take minutes.
    sequence = bytearray(b"N" * length)
    pos = 0  # 0-based count of bases consumed from the source so far
    for chunk in chunks:
        chunk_lo, chunk_hi = pos + 1, pos + len(chunk)  # 1-based, inclusive
        lo, hi = max(chunk_lo, span_lo), min(chunk_hi, span_hi)
        if lo <= hi:
            sequence[lo - 1 : hi] = chunk[lo - chunk_lo : hi - chunk_lo + 1]
        pos = chunk_hi
        if pos >= span_hi:
            break

    if pos < min(span_hi, length):
        raise RuntimeError(f"FASTA source ended early at {pos}, span needs {span_hi}")

    out_fasta.parent.mkdir(parents=True, exist_ok=True)
    header = f">{contig}\n".encode()  # source spelling, not `region.chrom`
    offset = len(header)
    with out_fasta.open("wb") as out:
        out.write(header)
        for i in range(0, length, FASTA_LINE_WIDTH):
            out.write(sequence[i : i + FASTA_LINE_WIDTH])
            out.write(b"\n")

    fai_line = (
        f"{contig}\t{length}\t{offset}\t{FASTA_LINE_WIDTH}\t{FASTA_LINE_WIDTH + 1}\n"
    )
    out_fasta.with_suffix(out_fasta.suffix + ".fai").write_text(fai_line)


def write_shard(ctx: SessionContext, table: pa.Table, destination: Path) -> None:
    """Write a sliced shard as Parquet the engine's point-lookup reader can use.

    The reader (``parquet_cache::page_dir::PageDir``) resolves a position to a
    data page through the footer's **ColumnIndex/OffsetIndex**, and errors with
    ``parquet has no column index`` without them. pyarrow's page index is not
    loaded by that reader, so shards must be written by parquet-rs — which is what
    the DataFusion ``COPY … STORED AS PARQUET`` writer used here is. Same reason
    ``tests/cache_metadata.py`` rewrites its fixtures through DataFusion.

    ``skip_arrow_metadata=false`` keeps the Arrow schema metadata
    (``bio.vep.cache_source_type``, ``bio.vep.sift_blob_version``) that the engine
    reads off the shard.

    Row ORDER is preserved by the caller's filter, which matters: ``PageDir``
    derives its sorted runs from the page statistics rather than from the footer's
    ``sorting_columns``, so the variation shard's warm-then-cold (each ascending by
    ``start``) layout survives the slice untouched.
    """
    if destination.exists():
        destination.unlink()

    if table.num_rows == 0:
        # DataFusion's writer cannot handle a zero-batch input, and an empty shard
        # has no pages to index anyway.
        pq.write_table(table, destination)
        return

    ctx.register_record_batches("shard", [table.combine_chunks().to_batches()])
    try:
        ctx.sql(
            f"COPY shard TO '{destination}' STORED AS PARQUET OPTIONS "
            "('statistics_enabled' 'page', 'skip_arrow_metadata' 'false')"
        ).collect()
    finally:
        ctx.deregister_table("shard")


def sha256(path: Path) -> str:
    """Hex SHA-256 of a file, read incrementally."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def build_mini_cache(
    source_cache: Path,
    fasta: Path,
    output: Path,
    region: Region,
) -> dict[str, int]:
    """Slice ``source_cache`` to ``region`` and cut the matching FASTA window.

    Returns a ``{table: surviving_rows}`` summary.
    """
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)

    # `transcript` must be sliced first: it defines the kept-transcript set that
    # the membership-filtered tables depend on.
    ordered = sorted(TABLE_POLICIES, key=lambda p: not p.defines_transcript_set)
    kept_transcripts: KeptTranscripts | None = None
    summary: dict[str, int] = {}
    span = region.padded
    ctx = SessionContext()

    for policy in ordered:
        entity_dir = source_cache / policy.name
        shard = entity_dir / f"{region.chrom}.parquet"
        if not shard.exists():
            # A table the source cache does not have for this chromosome. This is
            # normal, not a defect: `build_parquet_context_chrom` writes no shard
            # (and no manifest) for an entity with zero rows, and the Ensembl v115
            # cache ships NO motif features on any chromosome. The runtime's
            # `context_path` already returns `None` for an absent entity dir, so
            # mirroring the source's table set exactly — rather than fabricating an
            # empty shard — keeps the mini cache faithful. If `transcript` itself
            # were missing we could not build anything, so that one still fails.
            if policy.defines_transcript_set:
                raise FileNotFoundError(
                    f"missing {shard} — the source must be a per-chromosome cache "
                    f"built by the current engine builders for {region.chrom}"
                )
            summary[policy.name] = MISSING_IN_SOURCE
            continue

        kept = slice_table(shard, policy, region, kept_transcripts)

        if policy.defines_transcript_set:
            kept_transcripts = KeptTranscripts(
                stable_ids=kept["stable_id"].combine_chunks(),
                uids=kept["transcript_uid"].combine_chunks(),
            )
            span = feature_closure_span(kept, region)

        out_dir = output / policy.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_shard = out_dir / f"{region.chrom}.parquet"
        write_shard(ctx, kept, out_shard)
        write_manifest(
            out_dir,
            [{"chrom": region.chrom, "dataset": out_shard.name, "rows": kept.num_rows}],
        )
        summary[policy.name] = kept.num_rows

    write_fasta_window(
        fasta,
        fasta.with_suffix(fasta.suffix + ".fai"),
        region,
        span,
        output / fasta.name,
    )
    summary["_fasta_span_start"], summary["_fasta_span_end"] = span
    return summary


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-cache", type=Path, required=True)
    parser.add_argument("--fasta", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--chrom", default="chr22")
    parser.add_argument("--start", type=int, default=22_000_000)
    parser.add_argument("--end", type=int, default=23_500_000)
    parser.add_argument(
        "--flank",
        type=int,
        default=DEFAULT_FLANK,
        help=(
            "bases to pad the region by when selecting features, so transcripts "
            "just outside it still produce upstream/downstream consequences. Must "
            "be >= the --distance the cache will be annotated with (default 5000)."
        ),
    )
    args = parser.parse_args()

    region = Region(args.chrom, args.start, args.end, args.flank)
    summary = build_mini_cache(args.source_cache, args.fasta, args.output, region)

    for table, rows in summary.items():
        print(f"{table:24s} {rows:>12,}")
    print(f"\nmini-cache: {args.output}")
    print(f"fasta sha256: {sha256(args.output / args.fasta.name)}")


if __name__ == "__main__":
    main()
