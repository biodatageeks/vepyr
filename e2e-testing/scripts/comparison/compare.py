"""Lossless field-by-field CSQ comparison against an Ensembl VEP reference.

This module remains pure with respect to vepyr: it accepts paths, emits an
optional JSONL mismatch ledger, and returns JSON-serialisable counters. It does
not import the native extension or infer release identity from directory names.

Byte equality is the only thing counted as a plain match. Two weaker forms of
agreement are absorbed into the match rate but counted separately so they stay
visible: '&'-list order differences (``field_order_mismatch_counts``) and
representation-only differences such as decimal padding or VEP's "." missing
marker (``field_format_mismatch_counts``). Both keep the parity gate honest --
it fails on either total being non-zero -- while keeping a plugin comparison
readable instead of drowned in formatting noise.
"""

import hashlib
import json
import os
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from . import vcfio

VEP_HASH_ORDER_PICK_IGNORE_REASON = (
    "CSQ entry order is ignored for per_gene and pick_allele_gene because "
    "Ensembl VEP selects the representative consequences, then emits those "
    "winners by iterating Perl hashes (`keys %by_gene`; for pick_allele_gene "
    "also `keys %by_allele`). The comma order of those already-selected CSQ "
    "entries has no biological or interpretation meaning; it is not a severity, "
    "transcript-priority, genomic, MANE, or canonical ranking. The meaningful "
    "checks are the selected CSQ entries, entry counts, and field values."
)

_CSQ_RE = re.compile(r"(?:^|;)CSQ=([^;\t]+)")
# A token we will treat as a number rather than as an opaque string. Narrower
# than what Decimal() itself accepts: no whitespace, no 'Infinity'/'NaN', and
# no leading zero on the integer part, so "01" stays distinct from "1" the way
# a zero-padded identifier must.
_NUMERIC_RE = re.compile(
    r"^[+-]?(?:(?:0|[1-9][0-9]*)(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$"
)
_EQUALITY_BUCKETS = (
    "both_empty",
    "both_nonempty_equal",
    "vepyr_empty_only",
    "vep_empty_only",
    "both_nonempty_unequal",
)


class _MismatchLedger:
    """Stream deterministic JSONL while hashing exactly the bytes written."""

    def __init__(self, path):
        self.path = os.fspath(path) if path is not None else None
        self.rows = 0
        self._sha256 = hashlib.sha256()
        self._stream = None
        if self.path is not None:
            parent = os.path.dirname(os.path.abspath(self.path))
            os.makedirs(parent, exist_ok=True)
            self._stream = open(self.path, "wb")

    def emit(self, record):
        payload = (
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            + "\n"
        ).encode("utf-8")
        self.rows += 1
        self._sha256.update(payload)
        if self._stream is not None:
            self._stream.write(payload)

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        return {
            "path": self.path,
            "rows": self.rows,
            "sha256": self._sha256.hexdigest(),
        }


def _get_csq_fields(path):
    with vcfio.open_text(path) as stream:
        for line in stream:
            if line.startswith("##INFO=<ID=CSQ"):
                match = re.search(r'Format: ([^"]+)', line)
                return match.group(1).split("|") if match else []
    return []


def _extract_keyed_csq(path):
    rows = []
    with vcfio.open_text(path) as stream:
        for line in stream:
            if line.startswith("#"):
                continue
            # rstrip first because INFO is the final column in a sites-only VCF.
            columns = line.rstrip("\r\n").split("\t", 9)
            match = _CSQ_RE.search(columns[7])
            csq = match.group(1) if match else ""
            key = (
                vcfio.canonical_contig(columns[0]),
                int(columns[1]),
                columns[3],
                columns[4],
            )
            rows.append((key, csq))
    rows.sort()
    return rows


def _parse_entries(raw, fields):
    if not raw:
        return []
    return [dict(zip(fields, encoded.split("|"))) for encoded in raw.split(",")]


def _entry_identity(entry):
    """Identity strong enough to keep different ALT alleles from cross-pairing."""
    return (
        entry.get("ALLELE_NUM", ""),
        entry.get("Allele", ""),
        entry.get("Feature", ""),
    )


def _entry_order_signature(entry):
    return (*_entry_identity(entry), entry.get("Consequence", ""))


def _entry_payload(entry, fields):
    return tuple(entry.get(field, "") for field in fields)


def _pair_entry_groups(vepyr_entries, vep_entries, shared_fields):
    """Pair by allele identity + Feature, preserving duplicates explicitly.

    Exact duplicate payloads are removed first. Any remaining entries in the
    same identity group are paired deterministically; unmatched tails become
    one-sided ledger rows instead of shifting every later transcript.
    """
    vepyr_groups = defaultdict(list)
    vep_groups = defaultdict(list)
    for entry in vepyr_entries:
        vepyr_groups[_entry_identity(entry)].append(entry)
    for entry in vep_entries:
        vep_groups[_entry_identity(entry)].append(entry)

    paired = []
    only_vepyr = []
    only_vep = []
    for identity in sorted(set(vepyr_groups) | set(vep_groups)):
        left = list(vepyr_groups.get(identity, []))
        right = list(vep_groups.get(identity, []))
        exact_pairs = []

        right_by_payload = defaultdict(list)
        for entry in right:
            right_by_payload[_entry_payload(entry, shared_fields)].append(entry)

        left_remaining = []
        for entry in left:
            payload = _entry_payload(entry, shared_fields)
            matches = right_by_payload.get(payload)
            if matches:
                exact_pairs.append((entry, matches.pop(0)))
            else:
                left_remaining.append(entry)
        right_remaining = [
            entry
            for payload in sorted(right_by_payload)
            for entry in right_by_payload[payload]
        ]

        sort_key = lambda entry: (  # noqa: E731 - local symmetric sort definition
            entry.get("Consequence", ""),
            _entry_payload(entry, shared_fields),
        )
        left_remaining.sort(key=sort_key)
        right_remaining.sort(key=sort_key)
        group_pairs = sorted(
            exact_pairs,
            key=lambda pair: (
                pair[0].get("Consequence", ""),
                _entry_payload(pair[0], shared_fields),
            ),
        )
        group_pairs.extend(zip(left_remaining, right_remaining))

        for ordinal, (left_entry, right_entry) in enumerate(group_pairs, start=1):
            paired.append((identity, ordinal, left_entry, right_entry))
        for ordinal, entry in enumerate(
            left_remaining[len(right_remaining) :],
            start=len(group_pairs) + 1,
        ):
            only_vepyr.append((identity, ordinal, entry))
        for ordinal, entry in enumerate(
            right_remaining[len(left_remaining) :],
            start=len(group_pairs) + 1,
        ):
            only_vep.append((identity, ordinal, entry))

    return paired, only_vepyr, only_vep


def _identity_json(identity, duplicate_ordinal):
    allele_num, allele, feature = identity
    return {
        "allele_num": allele_num,
        "allele": allele,
        "feature": feature,
        "duplicate_ordinal": duplicate_ordinal,
    }


def _equality_bucket(vepyr_value, vep_value):
    if not vepyr_value and not vep_value:
        return "both_empty"
    if not vepyr_value:
        return "vepyr_empty_only"
    if not vep_value:
        return "vep_empty_only"
    if vepyr_value == vep_value:
        return "both_nonempty_equal"
    return "both_nonempty_unequal"


def _tokens_equivalent(vepyr_token, vep_token):
    """True when two unequal single values encode the same datum.

    Exact Decimal equality over canonical numeric literals -- not a tolerance,
    and deliberately not float(). float() would absorb genuine differences:
    it accepts whitespace and '_' separators, collapses integers past 2**53
    ("12345678901234567" == "...568"), turns any large exponent into inf, and
    reads "01" as 1 -- so a zero-padded code or a long numeric identifier
    would be written off as formatting. Decimal over _NUMERIC_RE absorbs
    decimal padding and shortest-round-trip forms and nothing else.
    """
    # One direction only: VEP writes the VCF missing marker where it has no
    # value, vepyr writes the empty string. A "." coming FROM vepyr is an
    # output defect, not a way of representing absence.
    if vepyr_token == "" and vep_token == ".":
        return True
    if not (_NUMERIC_RE.match(vepyr_token) and _NUMERIC_RE.match(vep_token)):
        return False
    try:
        return Decimal(vepyr_token) == Decimal(vep_token)
    except InvalidOperation:
        return False


def _values_equivalent(vepyr_value, vep_value):
    """True when two byte-unequal CSQ values carry the same data.

    Two representations differ here without the data differing:

    - Float formatting. vepyr prints Rust's shortest round-trip form ("0",
      "0.57985"); the plugin sources Ensembl VEP passes through print a fixed
      number of decimals ("0.00", "0.579850"). Same number, different string.
    - Missing-value marker. VEP emits the VCF "." for a field it has no value
      for, where vepyr emits the empty string. Absorbed in that direction
      only -- a "." from vepyr is a defect, not a representation of absence.

    '&'-joined multi-value fields are compared token-wise so one padded float
    does not condemn the whole field. Differing token counts are a real
    difference, never a formatting one.
    """
    vepyr_tokens = vepyr_value.split("&")
    vep_tokens = vep_value.split("&")
    if len(vepyr_tokens) != len(vep_tokens):
        return False
    return all(
        _tokens_equivalent(vepyr_token, vep_token)
        for vepyr_token, vep_token in zip(vepyr_tokens, vep_tokens)
    )


def compare_vcfs(
    vepyr_vcf,
    vep_vcf,
    label,
    ignore_csq_order=False,
    backend="parquet",
    mismatch_ledger_path=None,
):
    """Compare two VCFs and optionally write every strict mismatch to JSONL."""
    ledger = _MismatchLedger(mismatch_ledger_path)
    try:
        return _compare_vcfs(
            vepyr_vcf,
            vep_vcf,
            label,
            ignore_csq_order=ignore_csq_order,
            backend=backend,
            ledger=ledger,
        )
    finally:
        ledger.close()


def _compare_vcfs(
    vepyr_vcf,
    vep_vcf,
    label,
    *,
    ignore_csq_order,
    backend,
    ledger,
):
    print()
    print("=" * 60)
    print(f"Comparing vepyr ({backend}) vs VEP — {label}")
    print("=" * 60)

    n_vepyr = vcfio.count_data_lines(vepyr_vcf)
    n_vep = vcfio.count_data_lines(vep_vcf)
    print(f"  vepyr:  {n_vepyr:,} data lines")
    print(f"  VEP:    {n_vep:,} data lines")

    vepyr_fields = _get_csq_fields(vepyr_vcf)
    vep_fields = _get_csq_fields(vep_vcf)
    shared_fields = [field for field in vepyr_fields if field in vep_fields]
    fields_only_vepyr = sorted(set(vepyr_fields) - set(vep_fields))
    fields_only_vep = sorted(set(vep_fields) - set(vepyr_fields))
    if fields_only_vepyr:
        print(f"  Fields only in vepyr: {fields_only_vepyr}")
    if fields_only_vep:
        print(f"  Fields only in VEP:   {fields_only_vep}")

    for field in fields_only_vepyr:
        ledger.emit({"kind": "csq_field_only_in_vepyr", "field": field})
    for field in fields_only_vep:
        ledger.emit({"kind": "csq_field_only_in_vep", "field": field})

    print("  Building sorted key+CSQ lists ...")
    vepyr_rows = _extract_keyed_csq(vepyr_vcf)
    vep_rows = _extract_keyed_csq(vep_vcf)

    field_matches = {field: 0 for field in shared_fields}
    field_mismatches = {field: 0 for field in shared_fields}
    field_total = {field: 0 for field in shared_fields}
    field_mismatch_examples = {field: [] for field in shared_fields}
    field_order_mismatches = {field: 0 for field in shared_fields}
    field_order_mismatch_examples = {field: [] for field in shared_fields}
    field_format_mismatches = {field: 0 for field in shared_fields}
    field_format_mismatch_examples = {field: [] for field in shared_fields}
    field_equality_counts = {
        field: {bucket: 0 for bucket in _EQUALITY_BUCKETS} for field in shared_fields
    }

    n_compared = 0
    n_missing_in_vep = 0
    n_missing_in_vepyr = 0
    n_csq_count_match = 0
    n_csq_count_mismatch = 0
    n_csq_entries_only_in_vepyr = 0
    n_csq_entries_only_in_vep = 0
    n_csq_order_mismatch = 0
    n_csq_order_ignored = 0
    csq_order_mismatch_examples = []
    csq_order_ignored_examples = []

    i, j = 0, 0
    while i < len(vepyr_rows) or j < len(vep_rows):
        if j >= len(vep_rows) or (
            i < len(vepyr_rows) and vepyr_rows[i][0] < vep_rows[j][0]
        ):
            key, _ = vepyr_rows[i]
            key_str = f"{key[0]}\t{key[1]}\t{key[2]}\t{key[3]}"
            n_missing_in_vep += 1
            ledger.emit({"kind": "variant_only_in_vepyr", "variant": key_str})
            i += 1
            continue
        if i >= len(vepyr_rows) or vepyr_rows[i][0] > vep_rows[j][0]:
            key, _ = vep_rows[j]
            key_str = f"{key[0]}\t{key[1]}\t{key[2]}\t{key[3]}"
            n_missing_in_vepyr += 1
            ledger.emit({"kind": "variant_only_in_vep", "variant": key_str})
            j += 1
            continue

        key, vepyr_csq = vepyr_rows[i]
        _, vep_csq = vep_rows[j]
        key_str = f"{key[0]}\t{key[1]}\t{key[2]}\t{key[3]}"
        n_compared += 1

        vepyr_parsed = _parse_entries(vepyr_csq, vepyr_fields)
        vep_parsed = _parse_entries(vep_csq, vep_fields)
        vepyr_order = [_entry_order_signature(entry) for entry in vepyr_parsed]
        vep_order = [_entry_order_signature(entry) for entry in vep_parsed]
        if vepyr_order != vep_order and sorted(vepyr_order) == sorted(vep_order):
            example = {
                "variant": key_str,
                "vepyr_order": [list(value) for value in vepyr_order],
                "vep_order": [list(value) for value in vep_order],
            }
            if ignore_csq_order:
                n_csq_order_ignored += 1
                if len(csq_order_ignored_examples) < 10:
                    csq_order_ignored_examples.append(example)
            else:
                n_csq_order_mismatch += 1
                ledger.emit({"kind": "csq_order_mismatch", **example})
                if len(csq_order_mismatch_examples) < 10:
                    csq_order_mismatch_examples.append(example)

        if len(vepyr_parsed) == len(vep_parsed):
            n_csq_count_match += 1
        else:
            n_csq_count_mismatch += 1

        pairs, only_vepyr, only_vep = _pair_entry_groups(
            vepyr_parsed, vep_parsed, shared_fields
        )
        n_csq_entries_only_in_vepyr += len(only_vepyr)
        n_csq_entries_only_in_vep += len(only_vep)

        for identity, ordinal, entry in only_vepyr:
            ledger.emit(
                {
                    "kind": "csq_entry_only_in_vepyr",
                    "variant": key_str,
                    **_identity_json(identity, ordinal),
                    "vepyr_entry": entry,
                }
            )
        for identity, ordinal, entry in only_vep:
            ledger.emit(
                {
                    "kind": "csq_entry_only_in_vep",
                    "variant": key_str,
                    **_identity_json(identity, ordinal),
                    "vep_entry": entry,
                }
            )

        for identity, ordinal, vepyr_values, vep_values in pairs:
            identity_fields = _identity_json(identity, ordinal)
            for field in shared_fields:
                field_total[field] += 1
                vepyr_value = vepyr_values.get(field, "")
                vep_value = vep_values.get(field, "")
                bucket = _equality_bucket(vepyr_value, vep_value)
                field_equality_counts[field][bucket] += 1

                if vepyr_value == vep_value:
                    field_matches[field] += 1
                    continue

                if "&" in vepyr_value or "&" in vep_value:
                    vepyr_normalized = "&".join(sorted(vepyr_value.split("&")))
                    vep_normalized = "&".join(sorted(vep_value.split("&")))
                    if vepyr_normalized == vep_normalized:
                        field_matches[field] += 1
                        field_order_mismatches[field] += 1
                        if len(field_order_mismatch_examples[field]) < 10:
                            field_order_mismatch_examples[field].append(
                                {
                                    "variant": key_str,
                                    "vepyr": vepyr_value,
                                    "vep": vep_value,
                                    **identity_fields,
                                }
                            )
                        continue

                if _values_equivalent(vepyr_value, vep_value):
                    field_matches[field] += 1
                    field_format_mismatches[field] += 1
                    if len(field_format_mismatch_examples[field]) < 10:
                        field_format_mismatch_examples[field].append(
                            {
                                "variant": key_str,
                                "vepyr": vepyr_value,
                                "vep": vep_value,
                                **identity_fields,
                            }
                        )
                    continue

                field_mismatches[field] += 1
                record = {
                    "kind": "field_mismatch",
                    "variant": key_str,
                    "field": field,
                    "mismatch_shape": bucket,
                    "vepyr": vepyr_value,
                    "vep": vep_value,
                    **identity_fields,
                }
                ledger.emit(record)
                if len(field_mismatch_examples[field]) < 10:
                    field_mismatch_examples[field].append(
                        {
                            "variant": key_str,
                            "vepyr": vepyr_value,
                            "vep": vep_value,
                            **identity_fields,
                        }
                    )

        i += 1
        j += 1

    ledger_info = ledger.close()

    print("\n  Results:")
    print(f"    Variants compared:        {n_compared:,}")
    print(f"    Only in vepyr:            {n_missing_in_vep:,}")
    print(f"    Only in VEP:              {n_missing_in_vepyr:,}")
    print(f"    CSQ count match:          {n_csq_count_match:,}")
    print(f"    CSQ count mismatch:       {n_csq_count_mismatch:,}")
    print(
        f"    CSQ order mismatch:       {n_csq_order_mismatch:,}  "
        "(same entries, wrong order — issue #83)"
    )
    if ignore_csq_order:
        print(
            f"    CSQ order ignored:        {n_csq_order_ignored:,}  "
            "(VEP hash-order only)"
        )
    if ledger_info["path"] is not None:
        print(
            f"    Mismatch ledger:          {ledger_info['rows']:,} rows "
            f"({ledger_info['sha256'][:12]}…) -> {ledger_info['path']}"
        )

    print(f"\n  Per-field match rates ({n_compared:,} variants):")
    print(
        f"  {'Field':<30} {'Match%':>8} {'Matches':>10} "
        f"{'Mismatches':>10} {'OrderOnly':>10} {'FormatOnly':>10} {'Total':>10}"
    )
    print(
        f"  {'-' * 30} {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}"
    )
    for field in shared_fields:
        total = field_total[field]
        matches = field_matches[field]
        mismatches = field_mismatches[field]
        order_only = field_order_mismatches[field]
        format_only = field_format_mismatches[field]
        rate = (matches / total * 100) if total > 0 else 0
        if mismatches > 0:
            flag = " <--"
        elif order_only > 0:
            flag = " (order)"
        elif format_only > 0:
            flag = " (format)"
        else:
            flag = ""
        print(
            f"  {field:<30} {rate:>7.2f}% {matches:>10,} {mismatches:>10,} "
            f"{order_only:>10,} {format_only:>10,} {total:>10,}{flag}"
        )

    fields_with_mismatches = [
        field for field in shared_fields if field_mismatches[field] > 0
    ]
    fields_absorbed = [
        field
        for field in shared_fields
        if field_order_mismatches[field] or field_format_mismatches[field]
    ]
    if fields_with_mismatches:
        print("\n  Mismatch examples (display capped; JSONL ledger is uncapped):")
        for field in fields_with_mismatches:
            print(f"\n    {field} ({field_mismatches[field]:,} mismatches):")
            for example in field_mismatch_examples[field]:
                print(f"      {example['variant']}")
                print(f"        vepyr: {example['vepyr']!r}")
                print(f"        VEP:   {example['vep']!r}")
    elif fields_absorbed:
        # Never print the all-match banner here: these fields DO differ byte
        # for byte, and the parity gate fails on either count. Saying "100%"
        # is the one sentence an operator reads as parity.
        print(
            f"\n  No field mismatches, but {len(fields_absorbed)} shared CSQ "
            "field(s) differ byte-wise and were absorbed:"
        )
        for field in fields_absorbed:
            print(
                f"    {field:<30} {field_order_mismatches[field]:>10,} order-only "
                f"{field_format_mismatches[field]:>10,} format-only"
            )
        print("  This is not byte parity; the parity gate rejects both counts.")
    else:
        print(f"\n  ALL {len(shared_fields)} shared CSQ fields match at 100%!")

    return {
        "variants_compared": n_compared,
        "variants_only_in_vepyr": n_missing_in_vep,
        "variants_only_in_vep": n_missing_in_vepyr,
        "csq_entry_count_match": n_csq_count_match,
        "csq_entry_count_mismatch": n_csq_count_mismatch,
        "csq_entries_only_in_vepyr": n_csq_entries_only_in_vepyr,
        "csq_entries_only_in_vep": n_csq_entries_only_in_vep,
        "csq_order_mismatch": n_csq_order_mismatch,
        "csq_order_mismatch_examples": csq_order_mismatch_examples,
        "csq_order_ignored": n_csq_order_ignored,
        "csq_order_ignored_examples": csq_order_ignored_examples,
        "csq_order_ignore_reason": (
            VEP_HASH_ORDER_PICK_IGNORE_REASON if ignore_csq_order else None
        ),
        "fields_only_in_vepyr": fields_only_vepyr,
        "fields_only_in_vep": fields_only_vep,
        "field_match_rates": {
            field: round(field_matches[field] / field_total[field] * 100, 4)
            for field in shared_fields
            if field_total[field] > 0
        },
        "field_mismatch_counts": {
            field: field_mismatches[field]
            for field in shared_fields
            if field_mismatches[field] > 0
        },
        "field_mismatch_examples": {
            field: field_mismatch_examples[field]
            for field in shared_fields
            if field_mismatch_examples[field]
        },
        "field_order_mismatch_counts": {
            field: field_order_mismatches[field]
            for field in shared_fields
            if field_order_mismatches[field] > 0
        },
        "field_order_mismatch_examples": {
            field: field_order_mismatch_examples[field]
            for field in shared_fields
            if field_order_mismatch_examples[field]
        },
        "field_format_mismatch_counts": {
            field: field_format_mismatches[field]
            for field in shared_fields
            if field_format_mismatches[field] > 0
        },
        "field_format_mismatch_examples": {
            field: field_format_mismatch_examples[field]
            for field in shared_fields
            if field_format_mismatch_examples[field]
        },
        "field_equality_counts": field_equality_counts,
        "equality_bucket_counts": {
            bucket: sum(counts[bucket] for counts in field_equality_counts.values())
            for bucket in _EQUALITY_BUCKETS
        },
        "mismatch_ledger": ledger_info,
    }
