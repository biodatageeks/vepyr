"""Field-by-field CSQ comparison between a vepyr output and a VEP reference.

Pure with respect to the rest of the harness: it takes two paths and returns a
dict. It does not import vepyr, does not parse argv, and does not format
Markdown, so it unit-tests without a built native extension.
"""

import re

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


def compare_vcfs(vepyr_vcf, vep_vcf, label, ignore_csq_order=False, backend="parquet"):
    """Field-by-field CSQ comparison between vepyr and VEP output."""
    print()
    print("=" * 60)
    print(f"Comparing vepyr ({backend}) vs VEP — {label}")
    print("=" * 60)

    n_vepyr = vcfio.count_data_lines(vepyr_vcf)
    n_vep = vcfio.count_data_lines(vep_vcf)
    print(f"  vepyr:  {n_vepyr:,} data lines")
    print(f"  VEP:    {n_vep:,} data lines")

    # Parse CSQ field names from headers
    csq_re = re.compile(r"CSQ=([^;\t]+)")

    def get_csq_fields(path):
        with vcfio.open_text(path) as f:
            for line in f:
                if line.startswith("##INFO=<ID=CSQ"):
                    m = re.search(r"Format: ([^\"]+)", line)
                    return m.group(1).split("|") if m else []
        return []

    vepyr_fields = get_csq_fields(vepyr_vcf)
    vep_fields = get_csq_fields(vep_vcf)
    shared_fields = [f for f in vepyr_fields if f in vep_fields]

    fields_only_vepyr = sorted(set(vepyr_fields) - set(vep_fields))
    fields_only_vep = sorted(set(vep_fields) - set(vepyr_fields))
    if fields_only_vepyr:
        print(f"  Fields only in vepyr: {fields_only_vepyr}")
    if fields_only_vep:
        print(f"  Fields only in VEP:   {fields_only_vep}")

    # Build sorted key+CSQ for merge-join
    def extract_keyed_csq(path):
        rows = []
        with vcfio.open_text(path) as f:
            for line in f:
                if line.startswith("#"):
                    continue
                # rstrip first: in a sites-only VCF (8 columns) INFO is the last
                # column, so the newline would otherwise be captured inside the
                # final CSQ field value and read as a mismatch. No-op on VEP
                # output, which always carries FORMAT and sample columns.
                cols = line.rstrip("\n").split("\t", 9)
                m = csq_re.search(cols[7])
                csq = m.group(1) if m else ""
                key = (cols[0], int(cols[1]), cols[3], cols[4])
                rows.append((key, csq))
        rows.sort()
        return rows

    print("  Building sorted key+CSQ lists ...")
    vepyr_rows = extract_keyed_csq(vepyr_vcf)
    vep_rows = extract_keyed_csq(vep_vcf)

    # Merge-join
    field_matches = {f: 0 for f in shared_fields}
    field_mismatches = {f: 0 for f in shared_fields}
    field_total = {f: 0 for f in shared_fields}
    field_mismatch_examples = {f: [] for f in shared_fields}

    n_compared = 0
    n_missing_in_vep = 0
    n_missing_in_vepyr = 0
    n_csq_count_match = 0
    n_csq_count_mismatch = 0
    n_csq_order_mismatch = 0
    n_csq_order_ignored = 0
    csq_order_mismatch_examples = []
    csq_order_ignored_examples = []
    field_order_mismatches = {f: 0 for f in shared_fields}
    field_order_mismatch_examples = {f: [] for f in shared_fields}

    i, j = 0, 0
    while i < len(vepyr_rows) and j < len(vep_rows):
        vk, vepyr_csq = vepyr_rows[i]
        gk, vep_csq = vep_rows[j]

        if vk < gk:
            n_missing_in_vep += 1
            i += 1
            continue
        elif vk > gk:
            n_missing_in_vepyr += 1
            j += 1
            continue

        n_compared += 1
        key_str = f"{vk[0]}\t{vk[1]}\t{vk[2]}\t{vk[3]}"

        if vepyr_csq and vep_csq:

            def parse_entries(raw, fields):
                entries = []
                for e in raw.split(","):
                    vals = dict(zip(fields, e.split("|")))
                    entries.append(vals)
                return entries

            def sort_key(d):
                return (d.get("Feature", ""), d.get("Consequence", ""))

            vepyr_parsed = parse_entries(vepyr_csq, vepyr_fields)
            vep_parsed = parse_entries(vep_csq, vep_fields)

            # Detect CSQ entry ordering mismatch before sorting for comparison
            vepyr_order = [d.get("Feature", "") for d in vepyr_parsed]
            vep_order = [d.get("Feature", "") for d in vep_parsed]
            if vepyr_order != vep_order and sorted(vepyr_order) == sorted(vep_order):
                example = {
                    "variant": key_str,
                    "vepyr_order": vepyr_order,
                    "vep_order": vep_order,
                }
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
                if ignore_csq_order:
                    n_csq_order_ignored += 1
                    if len(csq_order_ignored_examples) < 10:
                        csq_order_ignored_examples.append(example)
                else:
                    n_csq_order_mismatch += 1
                    if len(csq_order_mismatch_examples) < 10:
                        csq_order_mismatch_examples.append(example)

            # Sort by Feature for stable pairing (so field comparison is meaningful)
            vepyr_parsed.sort(key=sort_key)
            vep_parsed.sort(key=sort_key)

            if len(vepyr_parsed) == len(vep_parsed):
                n_csq_count_match += 1
            else:
                n_csq_count_mismatch += 1

            for ei in range(min(len(vepyr_parsed), len(vep_parsed))):
                vepyr_vals = vepyr_parsed[ei]
                vep_vals = vep_parsed[ei]

                for f in shared_fields:
                    field_total[f] += 1
                    vv = vepyr_vals.get(f, "")
                    gv = vep_vals.get(f, "")
                    if vv == gv:
                        field_matches[f] += 1
                    else:
                        # Check if it's just an &-ordering difference
                        if "&" in vv or "&" in gv:
                            vv_norm = "&".join(sorted(vv.split("&")))
                            gv_norm = "&".join(sorted(gv.split("&")))
                            if vv_norm == gv_norm:
                                # Same values, different order
                                field_matches[f] += 1
                                field_order_mismatches[f] += 1
                                if len(field_order_mismatch_examples[f]) < 10:
                                    field_order_mismatch_examples[f].append(
                                        {"variant": key_str, "vepyr": vv, "vep": gv}
                                    )
                                continue
                        field_mismatches[f] += 1
                        if len(field_mismatch_examples[f]) < 10:
                            field_mismatch_examples[f].append(
                                {"variant": key_str, "vepyr": vv, "vep": gv}
                            )

        i += 1
        j += 1

    while i < len(vepyr_rows):
        n_missing_in_vep += 1
        i += 1
    while j < len(vep_rows):
        n_missing_in_vepyr += 1
        j += 1

    # Print results
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

    if csq_order_mismatch_examples:
        print("\n  CSQ order mismatch examples:")
        for ex in csq_order_mismatch_examples:
            print(f"    {ex['variant']}")
            print(f"      vepyr: {', '.join(ex['vepyr_order'])}")
            print(f"      VEP:   {', '.join(ex['vep_order'])}")

    print(f"\n  Per-field match rates ({n_compared:,} variants):")
    print(
        f"  {'Field':<30} {'Match%':>8} {'Matches':>10} "
        f"{'Mismatches':>10} {'OrderOnly':>10} {'Total':>10}"
    )
    print(f"  {'-' * 30} {'-' * 8} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}")
    for f in shared_fields:
        total = field_total[f]
        matches = field_matches[f]
        mismatches = field_mismatches[f]
        order_only = field_order_mismatches[f]
        rate = (matches / total * 100) if total > 0 else 0
        flag = ""
        if mismatches > 0:
            flag = " <--"
        elif order_only > 0:
            flag = " (order)"
        print(
            f"  {f:<30} {rate:>7.2f}% {matches:>10,} "
            f"{mismatches:>10,} {order_only:>10,} {total:>10,}{flag}"
        )

    fields_with_order_issues = [
        f for f in shared_fields if field_order_mismatches[f] > 0
    ]
    if fields_with_order_issues:
        print("\n  &-order mismatch examples (same values, different order):")
        for f in fields_with_order_issues:
            print(f"\n    {f} ({field_order_mismatches[f]:,} &-order mismatches):")
            for ex in field_order_mismatch_examples[f]:
                print(f"      {ex['variant']}")
                print(f"        vepyr: {ex['vepyr']!r}")
                print(f"        VEP:   {ex['vep']!r}")

    fields_with_mismatches = [f for f in shared_fields if field_mismatches[f] > 0]
    if fields_with_mismatches:
        print("\n  Mismatch examples:")
        for f in fields_with_mismatches:
            print(f"\n    {f} ({field_mismatches[f]:,} mismatches):")
            for ex in field_mismatch_examples[f]:
                print(f"      {ex['variant']}")
                print(f"        vepyr: {ex['vepyr']!r}")
                print(f"        VEP:   {ex['vep']!r}")
    else:
        print(f"\n  ALL {len(shared_fields)} shared CSQ fields match at 100%!")

    return {
        "variants_compared": n_compared,
        "variants_only_in_vepyr": n_missing_in_vep,
        "csq_order_mismatch": n_csq_order_mismatch,
        "csq_order_mismatch_examples": csq_order_mismatch_examples,
        "csq_order_ignored": n_csq_order_ignored,
        "csq_order_ignored_examples": csq_order_ignored_examples,
        "csq_order_ignore_reason": VEP_HASH_ORDER_PICK_IGNORE_REASON
        if ignore_csq_order
        else None,
        "variants_only_in_vep": n_missing_in_vepyr,
        "csq_entry_count_match": n_csq_count_match,
        "csq_entry_count_mismatch": n_csq_count_mismatch,
        "field_match_rates": {
            f: round(field_matches[f] / field_total[f] * 100, 4)
            for f in shared_fields
            if field_total[f] > 0
        },
        "field_mismatch_counts": {
            f: field_mismatches[f] for f in shared_fields if field_mismatches[f] > 0
        },
        "field_mismatch_examples": {
            f: field_mismatch_examples[f]
            for f in shared_fields
            if field_mismatch_examples[f]
        },
        "field_order_mismatch_counts": {
            f: field_order_mismatches[f]
            for f in shared_fields
            if field_order_mismatches[f] > 0
        },
        "field_order_mismatch_examples": {
            f: field_order_mismatch_examples[f]
            for f in shared_fields
            if field_order_mismatch_examples[f]
        },
    }
