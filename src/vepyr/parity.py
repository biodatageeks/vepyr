"""Field-by-field CSQ parity comparison between an Ensembl VEP VCF and vepyr's.

This is the single implementation of the comparison that decides whether vepyr
agrees with Ensembl VEP. It has two consumers:

* the WGS benchmark (``e2e-testing/scripts/run_annotation_fast.py``), which
  compares every shared CSQ field over ~4M variants and prints/serialises a
  report — see :func:`compare_vcfs`;
* the plugin-parity gate (``vepyr-plugins``), which compares *only* a ported
  plugin's own CSQ fields and demands 100% — see :func:`compare_csq_fields`.

Terminology: **truth** is the Ensembl VEP output, **test** is the vepyr output.
The comparison is deliberately asymmetric in its reading but symmetric in its
arithmetic: a value vepyr emits where VEP emitted nothing (*over-emission*) is a
mismatch, exactly like a value it failed to emit. A plugin that fires more often
than upstream is not a bonus, it is a bug.

Comparison semantics (unchanged from the original benchmark implementation):

* Variants are merge-joined on ``(CHROM, POS, REF, ALT)``; unmatched keys on
  either side are counted, never compared.
* Within a variant, CSQ entries are paired by ``(Feature, Consequence)`` so that
  field comparison is meaningful regardless of emission order; the number of
  entries is compared strictly.
* The *comma order* of CSQ entries is compared strictly by default and reported
  separately (it is not a field mismatch). See :func:`compare_csq_fields` and
  :data:`VEP_HASH_ORDER_PICK_IGNORE_REASON` for the one case where it is
  deliberately ignored.
* Within a multi-valued field (``a&b``), a pure ordering difference counts as a
  match and is reported separately as an *order-only* difference.
"""

from __future__ import annotations

import gzip
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Final

__all__ = [
    "VEP_HASH_ORDER_PICK_IGNORE_REASON",
    "ComparisonResult",
    "EntryOrderMismatch",
    "FieldMismatch",
    "compare_csq_fields",
    "compare_vcfs",
    "count_data_lines",
    "csq_format_fields",
    "open_text",
]

#: Number of worked examples retained per mismatch class (a report, not a dump).
MAX_EXAMPLES: Final = 10

_CSQ_VALUE_RE: Final = re.compile(r"CSQ=([^;\t]+)")
_CSQ_FORMAT_RE: Final = re.compile(r"Format: ([^\"]+)")
_CSQ_HEADER_PREFIX: Final = "##INFO=<ID=CSQ"

#: The rationale for ``ignore_entry_order``. Recorded on the result (and in the
#: benchmark's JSON report) so that a run which ignored CSQ comma order says so,
#: and says why.
VEP_HASH_ORDER_PICK_IGNORE_REASON: Final = (
    "CSQ entry order is ignored for per_gene and pick_allele_gene because "
    "Ensembl VEP selects the representative consequences, then emits those "
    "winners by iterating Perl hashes (`keys %by_gene`; for pick_allele_gene "
    "also `keys %by_allele`). The comma order of those already-selected CSQ "
    "entries has no biological or interpretation meaning; it is not a severity, "
    "transcript-priority, genomic, MANE, or canonical ranking. The meaningful "
    "checks are the selected CSQ entries, entry counts, and field values."
)

#: A variant identity: ``(chrom, pos, ref, alt)``.
VariantKey = tuple[str, int, str, str]


def open_text(path: str | Path) -> IO[str]:
    """Open a VCF for text reading, transparently handling ``.gz`` (bgzf/gzip)."""
    path = str(path)
    if path.endswith((".gz", ".bgz", ".bgzf")):
        return gzip.open(path, "rt")
    return open(path)


def count_data_lines(path: str | Path) -> int:
    """Count non-header lines in a VCF (plain or ``.gz``)."""
    n = 0
    with open_text(path) as f:
        for line in f:
            if not line.startswith("#"):
                n += 1
    return n


def csq_format_fields(path: str | Path) -> list[str]:
    """Return the CSQ field names declared by a VCF's ``##INFO=<ID=CSQ`` header.

    Returns an empty list if the VCF carries no CSQ header (or no ``Format:``
    spec within it), which in turn makes every field unshared and uncomparable.
    """
    with open_text(path) as f:
        for line in f:
            if line.startswith(_CSQ_HEADER_PREFIX):
                m = _CSQ_FORMAT_RE.search(line)
                return m.group(1).split("|") if m else []
    return []


def _keyed_csq(path: str | Path) -> list[tuple[VariantKey, str]]:
    """Return ``(key, raw_csq)`` pairs sorted by key, ready for a merge-join.

    The record terminator is stripped before the line is split into columns. It
    is not part of any field — a VCF field cannot contain CR or LF, since those
    *are* the record separator — so stripping it cannot eat a legitimate value.
    It matters because in a sites-only (8-column) VCF, INFO is the final column,
    so the terminator sits immediately after the last CSQ field; left in place it
    would be captured as part of that field's value (``[^;\\t]`` matches ``\\n``).
    That is precisely where plugin fields live: they are appended to the end of
    VEP's CSQ ``Format`` string.
    """
    rows: list[tuple[VariantKey, str]] = []
    with open_text(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            cols = line.rstrip("\r\n").split("\t", 9)
            m = _CSQ_VALUE_RE.search(cols[7])
            csq = m.group(1) if m else ""
            key: VariantKey = (cols[0], int(cols[1]), cols[3], cols[4])
            rows.append((key, csq))
    rows.sort()
    return rows


def _parse_entries(raw: str, fields: Sequence[str]) -> list[dict[str, str]]:
    """Split a raw CSQ value into per-entry ``field -> value`` mappings."""
    return [dict(zip(fields, entry.split("|"))) for entry in raw.split(",")]


def _pairing_key(entry: dict[str, str]) -> tuple[str, str]:
    """Sort key used to pair a vepyr CSQ entry with its VEP counterpart."""
    return (entry.get("Feature", ""), entry.get("Consequence", ""))


@dataclass(frozen=True, slots=True)
class FieldMismatch:
    """One worked example of a field-level disagreement.

    A bare count is useless for debugging, so every example carries the variant
    key *and* both values.
    """

    key: str
    """The variant, as ``CHROM\\tPOS\\tREF\\tALT``."""
    truth: str
    """The value Ensembl VEP emitted (``""`` if it emitted none)."""
    test: str
    """The value vepyr emitted (``""`` if it emitted none)."""

    @property
    def is_over_emission(self) -> bool:
        """True if vepyr populated a field that VEP left empty."""
        return self.truth == "" and self.test != ""

    def as_report_dict(self) -> dict[str, str]:
        """Render as the benchmark report's example shape."""
        return {"variant": self.key, "vepyr": self.test, "vep": self.truth}


@dataclass(frozen=True, slots=True)
class EntryOrderMismatch:
    """One worked example of a CSQ *comma order* disagreement.

    The same entries are present on both sides; only their emission order
    differs. This is tracked apart from field mismatches because it is a
    different kind of defect (see :data:`VEP_HASH_ORDER_PICK_IGNORE_REASON`).
    """

    key: str
    """The variant, as ``CHROM\\tPOS\\tREF\\tALT``."""
    truth_features: tuple[str, ...]
    """``Feature`` of each CSQ entry, in the order Ensembl VEP emitted them."""
    test_features: tuple[str, ...]
    """``Feature`` of each CSQ entry, in the order vepyr emitted them."""

    def as_report_dict(self) -> dict[str, str | list[str]]:
        """Render as the benchmark report's example shape."""
        return {
            "variant": self.key,
            "vepyr_order": list(self.test_features),
            "vep_order": list(self.truth_features),
        }


@dataclass(slots=True)
class ComparisonResult:
    """The verdict of a CSQ comparison between a VEP VCF and a vepyr VCF."""

    fields_compared: tuple[str, ...]
    """CSQ fields actually compared, in report order."""
    fields_only_in_test: tuple[str, ...]
    """CSQ fields declared by vepyr's header but not VEP's (never compared)."""
    fields_only_in_truth: tuple[str, ...]
    """CSQ fields declared by VEP's header but not vepyr's (never compared)."""
    fields_missing_from_test: tuple[str, ...]
    """Explicitly requested fields absent from vepyr's CSQ header."""
    fields_missing_from_truth: tuple[str, ...]
    """Explicitly requested fields absent from VEP's CSQ header."""

    keys_compared: int
    """Variants present, and therefore compared, on both sides."""
    keys_only_in_test: int
    """Variants vepyr emitted that VEP did not."""
    keys_only_in_truth: int
    """Variants VEP emitted that vepyr did not."""

    csq_missing_in_test: int
    """Compared variants that VEP annotated but vepyr left with no CSQ at all.

    A total annotation dropout: with no CSQ to parse, such a variant contributes
    no field totals and no entry counts, so it is invisible in every other
    counter. A plugin cache that annotated *nothing* would otherwise show a
    perfectly clean field table. It gets its own bucket for that reason.
    """
    csq_missing_in_truth: int
    """Compared variants that vepyr annotated but VEP left with no CSQ at all."""

    entry_count_match: int
    """Compared variants where both sides emitted the same number of CSQ entries."""
    entry_count_mismatch: int
    """Compared variants where the sides disagreed on the CSQ entry count."""
    entry_order_mismatch: int
    """Compared variants with the same CSQ entries in a different comma order."""
    entry_order_ignored: int
    """Entry-order differences waived by ``ignore_entry_order``."""
    entry_order_mismatch_examples: list[EntryOrderMismatch]
    entry_order_ignored_examples: list[EntryOrderMismatch]
    entry_order_ignore_reason: str | None
    """Why entry order was ignored, or ``None`` if it was compared strictly."""

    field_totals: dict[str, int] = field(default_factory=dict)
    """Per field: CSQ entry pairs compared."""
    field_matches: dict[str, int] = field(default_factory=dict)
    """Per field: equal values (incl. order-only ``a&b`` differences)."""
    field_mismatches: dict[str, int] = field(default_factory=dict)
    """Per field: genuinely different values."""
    field_mismatch_examples: dict[str, list[FieldMismatch]] = field(
        default_factory=dict
    )
    field_order_mismatches: dict[str, int] = field(default_factory=dict)
    """Per field: same multi-values (``a&b``) in a different order — counted as matches."""
    field_order_mismatch_examples: dict[str, list[FieldMismatch]] = field(
        default_factory=dict
    )
    over_emissions: dict[str, int] = field(default_factory=dict)
    """Per field: mismatches where vepyr emitted a value and VEP emitted none.

    A strict subset of :attr:`field_mismatches`: over-emission is a failure, not
    a bonus, and is only broken out to name the failure mode.
    """

    @property
    def field_match_rates(self) -> dict[str, float]:
        """Per field: percentage of compared entry pairs that matched."""
        return {
            f: round(self.field_matches[f] / self.field_totals[f] * 100, 4)
            for f in self.fields_compared
            if self.field_totals[f] > 0
        }

    @property
    def mismatched_fields(self) -> tuple[str, ...]:
        """Compared fields with at least one genuine value mismatch."""
        return tuple(f for f in self.fields_compared if self.field_mismatches[f] > 0)

    @property
    def is_clean(self) -> bool:
        """True if this is a pass: nothing to explain away.

        Clean means every compared field agreed on every compared variant, both
        sides emitted the same variants and the same number of CSQ entries per
        variant in the same (or a deliberately-ignored) order, neither side
        dropped a variant's annotation entirely, and every field that was asked
        for actually existed on both sides.
        """
        return (
            not self.mismatched_fields
            and not self.fields_missing_from_test
            and not self.fields_missing_from_truth
            and self.entry_count_mismatch == 0
            and self.entry_order_mismatch == 0
            and self.keys_only_in_test == 0
            and self.keys_only_in_truth == 0
            and self.csq_missing_in_test == 0
            and self.csq_missing_in_truth == 0
        )

    def as_report_dict(self) -> dict[str, object]:
        """Render the benchmark's JSON report payload.

        The key names and the "only non-zero entries" filtering are load-bearing:
        ``run_annotation_fast_all.py`` and every archived report read this shape.
        """
        return {
            "variants_compared": self.keys_compared,
            "variants_only_in_vepyr": self.keys_only_in_test,
            "csq_order_mismatch": self.entry_order_mismatch,
            "csq_order_mismatch_examples": [
                ex.as_report_dict() for ex in self.entry_order_mismatch_examples
            ],
            "csq_order_ignored": self.entry_order_ignored,
            "csq_order_ignored_examples": [
                ex.as_report_dict() for ex in self.entry_order_ignored_examples
            ],
            "csq_order_ignore_reason": self.entry_order_ignore_reason,
            "variants_only_in_vep": self.keys_only_in_truth,
            "csq_missing_in_vepyr": self.csq_missing_in_test,
            "csq_missing_in_vep": self.csq_missing_in_truth,
            "csq_entry_count_match": self.entry_count_match,
            "csq_entry_count_mismatch": self.entry_count_mismatch,
            "field_match_rates": self.field_match_rates,
            "field_mismatch_counts": {
                f: self.field_mismatches[f]
                for f in self.fields_compared
                if self.field_mismatches[f] > 0
            },
            "field_mismatch_examples": {
                f: [ex.as_report_dict() for ex in self.field_mismatch_examples[f]]
                for f in self.fields_compared
                if self.field_mismatch_examples[f]
            },
            "field_order_mismatch_counts": {
                f: self.field_order_mismatches[f]
                for f in self.fields_compared
                if self.field_order_mismatches[f] > 0
            },
            "field_order_mismatch_examples": {
                f: [ex.as_report_dict() for ex in self.field_order_mismatch_examples[f]]
                for f in self.fields_compared
                if self.field_order_mismatch_examples[f]
            },
        }


def compare_csq_fields(
    truth_vcf: str | Path,
    test_vcf: str | Path,
    fields: Sequence[str] | None = None,
    *,
    ignore_entry_order: bool = False,
    max_examples: int = MAX_EXAMPLES,
) -> ComparisonResult:
    """Compare the CSQ fields of a vepyr VCF against an Ensembl VEP VCF.

    Variants are merge-joined on ``(CHROM, POS, REF, ALT)``. Within a matched
    variant, CSQ entries are paired by ``(Feature, Consequence)`` and compared
    field by field; entry counts are compared strictly.

    Args:
        truth_vcf: Ensembl VEP output (plain or gzipped).
        test_vcf: vepyr output (plain or gzipped).
        fields: CSQ fields to compare. ``None`` compares every field shared by
            both headers (the WGS benchmark). A plugin gate passes just that
            plugin's fields, so that drift in the core fields — a separate
            verdict — does not contaminate this one. Requested fields that are
            absent from either header are not compared; they are reported in
            :attr:`ComparisonResult.fields_missing_from_test` /
            :attr:`~ComparisonResult.fields_missing_from_truth` and make the
            result unclean.
        ignore_entry_order: Waive CSQ *comma order* differences (same entries,
            different order). Set this only for VEP's ``--per_gene`` and
            ``--pick_allele_gene`` outputs; see
            :data:`VEP_HASH_ORDER_PICK_IGNORE_REASON`. Entry counts and every
            field value are still compared strictly.
        max_examples: Worked examples retained per mismatch class per field.

    Returns:
        The :class:`ComparisonResult` verdict.
    """
    truth_fields = csq_format_fields(truth_vcf)
    test_fields = csq_format_fields(test_vcf)
    shared = [f for f in test_fields if f in truth_fields]

    if fields is None:
        compared = list(shared)
        missing_from_test: tuple[str, ...] = ()
        missing_from_truth: tuple[str, ...] = ()
    else:
        shared_set = set(shared)
        compared = [f for f in fields if f in shared_set]
        missing_from_test = tuple(f for f in fields if f not in test_fields)
        missing_from_truth = tuple(f for f in fields if f not in truth_fields)

    result = ComparisonResult(
        fields_compared=tuple(compared),
        fields_only_in_test=tuple(sorted(set(test_fields) - set(truth_fields))),
        fields_only_in_truth=tuple(sorted(set(truth_fields) - set(test_fields))),
        fields_missing_from_test=missing_from_test,
        fields_missing_from_truth=missing_from_truth,
        keys_compared=0,
        keys_only_in_test=0,
        keys_only_in_truth=0,
        csq_missing_in_test=0,
        csq_missing_in_truth=0,
        entry_count_match=0,
        entry_count_mismatch=0,
        entry_order_mismatch=0,
        entry_order_ignored=0,
        entry_order_mismatch_examples=[],
        entry_order_ignored_examples=[],
        entry_order_ignore_reason=(
            VEP_HASH_ORDER_PICK_IGNORE_REASON if ignore_entry_order else None
        ),
        field_totals=dict.fromkeys(compared, 0),
        field_matches=dict.fromkeys(compared, 0),
        field_mismatches=dict.fromkeys(compared, 0),
        field_mismatch_examples={f: [] for f in compared},
        field_order_mismatches=dict.fromkeys(compared, 0),
        field_order_mismatch_examples={f: [] for f in compared},
        over_emissions=dict.fromkeys(compared, 0),
    )

    test_rows = _keyed_csq(test_vcf)
    truth_rows = _keyed_csq(truth_vcf)

    i, j = 0, 0
    while i < len(test_rows) and j < len(truth_rows):
        test_key, test_csq = test_rows[i]
        truth_key, truth_csq = truth_rows[j]

        if test_key < truth_key:
            result.keys_only_in_test += 1
            i += 1
            continue
        elif test_key > truth_key:
            result.keys_only_in_truth += 1
            j += 1
            continue

        result.keys_compared += 1
        key_str = f"{test_key[0]}\t{test_key[1]}\t{test_key[2]}\t{test_key[3]}"

        # A variant one side left entirely unannotated has no CSQ to parse, so it
        # can reach no other counter. Count it here or it vanishes.
        if not test_csq and truth_csq:
            result.csq_missing_in_test += 1
        elif test_csq and not truth_csq:
            result.csq_missing_in_truth += 1

        if test_csq and truth_csq:
            test_parsed = _parse_entries(test_csq, test_fields)
            truth_parsed = _parse_entries(truth_csq, truth_fields)

            # Detect CSQ entry ordering mismatch before sorting for comparison
            test_order = [e.get("Feature", "") for e in test_parsed]
            truth_order = [e.get("Feature", "") for e in truth_parsed]
            if test_order != truth_order and sorted(test_order) == sorted(truth_order):
                example = EntryOrderMismatch(
                    key=key_str,
                    truth_features=tuple(truth_order),
                    test_features=tuple(test_order),
                )
                # Ensembl VEP's --per_gene and --pick_allele_gene paths group
                # transcript alleles in Perl hashes, choose representative
                # consequences, then emit winners with `keys %by_gene` and, for
                # pick_allele_gene, `keys %by_allele`. The comma order of those
                # already-selected CSQ entries has no biological or
                # interpretation meaning: it is not a severity,
                # transcript-priority, genomic, MANE, or canonical ranking.
                # Ignoring only this order therefore does not change
                # interpretation; entry counts and every CSQ field value are
                # still compared strictly.
                if ignore_entry_order:
                    result.entry_order_ignored += 1
                    if len(result.entry_order_ignored_examples) < max_examples:
                        result.entry_order_ignored_examples.append(example)
                else:
                    result.entry_order_mismatch += 1
                    if len(result.entry_order_mismatch_examples) < max_examples:
                        result.entry_order_mismatch_examples.append(example)

            # Sort by Feature for stable pairing (so field comparison is meaningful)
            test_parsed.sort(key=_pairing_key)
            truth_parsed.sort(key=_pairing_key)

            if len(test_parsed) == len(truth_parsed):
                result.entry_count_match += 1
            else:
                result.entry_count_mismatch += 1

            for ei in range(min(len(test_parsed), len(truth_parsed))):
                test_vals = test_parsed[ei]
                truth_vals = truth_parsed[ei]

                for f in compared:
                    result.field_totals[f] += 1
                    tv = test_vals.get(f, "")
                    gv = truth_vals.get(f, "")
                    if tv == gv:
                        result.field_matches[f] += 1
                        continue

                    # Check if it's just an &-ordering difference
                    if "&" in tv or "&" in gv:
                        tv_norm = "&".join(sorted(tv.split("&")))
                        gv_norm = "&".join(sorted(gv.split("&")))
                        if tv_norm == gv_norm:
                            # Same values, different order
                            result.field_matches[f] += 1
                            result.field_order_mismatches[f] += 1
                            if (
                                len(result.field_order_mismatch_examples[f])
                                < max_examples
                            ):
                                result.field_order_mismatch_examples[f].append(
                                    FieldMismatch(key=key_str, truth=gv, test=tv)
                                )
                            continue

                    mismatch = FieldMismatch(key=key_str, truth=gv, test=tv)
                    result.field_mismatches[f] += 1
                    if mismatch.is_over_emission:
                        result.over_emissions[f] += 1
                    if len(result.field_mismatch_examples[f]) < max_examples:
                        result.field_mismatch_examples[f].append(mismatch)

        i += 1
        j += 1

    result.keys_only_in_test += len(test_rows) - i
    result.keys_only_in_truth += len(truth_rows) - j
    return result


def compare_vcfs(
    vepyr_vcf: str | Path,
    vep_vcf: str | Path,
    label: str,
    ignore_csq_order: bool = False,
    *,
    backend: str = "parquet",
) -> dict[str, object]:
    """Field-by-field CSQ comparison between vepyr and VEP output.

    The WGS benchmark's entry point: runs :func:`compare_csq_fields` over every
    shared CSQ field, prints the human-readable report, and returns the JSON
    report payload consumed by ``run_annotation_fast_all.py``.

    Args:
        vepyr_vcf: vepyr's annotated output (the *test* side).
        vep_vcf: the Ensembl VEP reference (the *truth* side).
        label: Report label, typically the chromosome.
        ignore_csq_order: See ``ignore_entry_order`` in :func:`compare_csq_fields`.
        backend: Cache backend name, for the report header only.

    Returns:
        The report payload — see :meth:`ComparisonResult.as_report_dict`.
    """
    print()
    print("=" * 60)
    print(f"Comparing vepyr ({backend}) vs VEP — {label}")
    print("=" * 60)

    n_vepyr = count_data_lines(vepyr_vcf)
    n_vep = count_data_lines(vep_vcf)
    print(f"  vepyr:  {n_vepyr:,} data lines")
    print(f"  VEP:    {n_vep:,} data lines")

    vepyr_fields = set(csq_format_fields(vepyr_vcf))
    vep_fields = set(csq_format_fields(vep_vcf))
    fields_only_vepyr = sorted(vepyr_fields - vep_fields)
    fields_only_vep = sorted(vep_fields - vepyr_fields)
    if fields_only_vepyr:
        print(f"  Fields only in vepyr: {fields_only_vepyr}")
    if fields_only_vep:
        print(f"  Fields only in VEP:   {fields_only_vep}")

    print("  Building sorted key+CSQ lists ...")
    result = compare_csq_fields(
        vep_vcf,
        vepyr_vcf,
        ignore_entry_order=ignore_csq_order,
    )
    shared_fields = result.fields_compared

    # Print results
    print("\n  Results:")
    print(f"    Variants compared:        {result.keys_compared:,}")
    print(f"    Only in vepyr:            {result.keys_only_in_test:,}")
    print(f"    Only in VEP:              {result.keys_only_in_truth:,}")
    print(
        f"    CSQ missing in vepyr:     {result.csq_missing_in_test:,}"
        "  (VEP annotated it, vepyr emitted no CSQ)"
    )
    print(
        f"    CSQ missing in VEP:       {result.csq_missing_in_truth:,}"
        "  (vepyr annotated it, VEP emitted no CSQ)"
    )
    print(f"    CSQ count match:          {result.entry_count_match:,}")
    print(f"    CSQ count mismatch:       {result.entry_count_mismatch:,}")
    print(
        f"    CSQ order mismatch:       {result.entry_order_mismatch:,}  (same entries, wrong order — issue #83)"
    )
    if ignore_csq_order:
        print(
            f"    CSQ order ignored:        {result.entry_order_ignored:,}  "
            "(VEP hash-order only)"
        )

    if result.entry_order_mismatch_examples:
        print("\n  CSQ order mismatch examples:")
        for order_ex in result.entry_order_mismatch_examples:
            print(f"    {order_ex.key}")
            print(f"      vepyr: {', '.join(order_ex.test_features)}")
            print(f"      VEP:   {', '.join(order_ex.truth_features)}")

    print(f"\n  Per-field match rates ({result.keys_compared:,} variants):")
    print(
        f"  {'Field':<30} {'Match%':>8} {'Matches':>10} {'Mismatches':>10} {'OrderOnly':>10} {'Total':>10}"
    )
    print(f"  {'-' * 30} {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")
    for f in shared_fields:
        total = result.field_totals[f]
        matches = result.field_matches[f]
        mismatches = result.field_mismatches[f]
        order_only = result.field_order_mismatches[f]
        rate = (matches / total * 100) if total > 0 else 0
        flag = ""
        if mismatches > 0:
            flag = " <--"
        elif order_only > 0:
            flag = " (order)"
        print(
            f"  {f:<30} {rate:>7.2f}% {matches:>10,} {mismatches:>10,} {order_only:>10,} {total:>10,}{flag}"
        )

    fields_with_order_issues = [
        f for f in shared_fields if result.field_order_mismatches[f] > 0
    ]
    if fields_with_order_issues:
        print("\n  &-order mismatch examples (same values, different order):")
        for f in fields_with_order_issues:
            print(
                f"\n    {f} ({result.field_order_mismatches[f]:,} &-order mismatches):"
            )
            for ex in result.field_order_mismatch_examples[f]:
                print(f"      {ex.key}")
                print(f"        vepyr: {ex.test!r}")
                print(f"        VEP:   {ex.truth!r}")

    fields_with_mismatches = result.mismatched_fields
    if fields_with_mismatches:
        print("\n  Mismatch examples:")
        for f in fields_with_mismatches:
            print(f"\n    {f} ({result.field_mismatches[f]:,} mismatches):")
            for ex in result.field_mismatch_examples[f]:
                print(f"      {ex.key}")
                print(f"        vepyr: {ex.test!r}")
                print(f"        VEP:   {ex.truth!r}")
    else:
        print(f"\n  ALL {len(shared_fields)} shared CSQ fields match at 100%!")

    return result.as_report_dict()
