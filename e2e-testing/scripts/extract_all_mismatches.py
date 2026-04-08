#!/usr/bin/env python3
"""Extract ALL mismatches between vepyr and VEP to TSV files.

Produces:
  - mismatches_parquet_vs_vep.tsv  — all field-level mismatches for parquet backend
  - mismatches_fjall_vs_vep.tsv    — all field-level mismatches for fjall backend

Each line: variant_key \t csq_entry_idx \t field \t vepyr_value \t vep_value
"""

import os
import re
import subprocess

# ── Paths ──────────────────────────────────────────────────────────────────
DATA_DIR = "/home/tgambin/workspace/data_vepyr"
VEP_VCF = os.path.join(DATA_DIR, "HG002_annotated_wgs_everything_hgvs.vcf")

WORK_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
REPORT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")

BACKENDS = {
    "parquet": os.path.join(WORK_DIR, "vepyr_parquet.vcf"),
    "fjall": os.path.join(WORK_DIR, "vepyr_fjall.vcf"),
}

csq_re = re.compile(r"CSQ=([^;\t]+)")


def get_csq_fields(vcf_path):
    header = (
        subprocess.check_output(f"grep '^##INFO=<ID=CSQ' '{vcf_path}'", shell=True)
        .decode()
        .strip()
    )
    return re.search(r"Format: ([^\"]+)", header).group(1).split("|")


def extract_keyscsq(vcf_path, out_path):
    """Extract chrom\tpos\tref\talt\tCSQ to sorted lightweight file."""
    if os.path.exists(out_path):
        print(f"  Reusing {out_path}")
        return
    unsorted = out_path + ".unsorted"
    print(f"  Extracting key+CSQ from {os.path.basename(vcf_path)}...")
    with open(vcf_path) as fin, open(unsorted, "w") as fout:
        for line in fin:
            if line.startswith("#"):
                continue
            cols = line.split("\t", 9)
            m = csq_re.search(cols[7])
            csq = m.group(1) if m else ""
            fout.write(f"{cols[0]}\t{cols[1]}\t{cols[3]}\t{cols[4]}\t{csq}\n")
    print("  Sorting...")
    subprocess.run(
        f"sort -S 4G -T /tmp -k1,1V -k2,2n -k3,3 -k4,4 '{unsorted}' > '{out_path}'",
        shell=True,
        check=True,
    )
    os.remove(unsorted)
    sz = os.path.getsize(out_path) / (1024 * 1024)
    print(f"  {os.path.basename(out_path)}: {sz:.0f} MB")


def parse_kc_line(line):
    parts = line.rstrip("\n").split("\t", 4)
    key = (parts[0], int(parts[1]), parts[2], parts[3])
    csq = parts[4] if len(parts) > 4 else ""
    return key, csq


def compare_and_dump(vepyr_kc, vep_kc, vepyr_fields, vep_fields, out_tsv):
    """Stream merge-join and write ALL mismatches to TSV."""
    shared_fields = [f for f in vepyr_fields if f in vep_fields]

    n_compared = 0
    n_mismatches = 0

    with open(vepyr_kc) as fv, open(vep_kc) as fg, open(out_tsv, "w") as fout:
        fout.write("chrom\tpos\tref\talt\tcsq_entry_idx\tfield\tvepyr\tvep\n")

        vepyr_line = fv.readline()
        vep_line = fg.readline()

        while vepyr_line and vep_line:
            vk, vepyr_csq = parse_kc_line(vepyr_line)
            gk, vep_csq = parse_kc_line(vep_line)

            if vk < gk:
                vepyr_line = fv.readline()
                continue
            elif vk > gk:
                vep_line = fg.readline()
                continue

            n_compared += 1

            if vepyr_csq and vep_csq:
                vepyr_entries = sorted(vepyr_csq.split(","))
                vep_entries = sorted(vep_csq.split(","))

                for i in range(min(len(vepyr_entries), len(vep_entries))):
                    vepyr_vals = dict(zip(vepyr_fields, vepyr_entries[i].split("|")))
                    vep_vals = dict(zip(vep_fields, vep_entries[i].split("|")))

                    for f in shared_fields:
                        vepyr_v = vepyr_vals.get(f, "")
                        vep_v = vep_vals.get(f, "")
                        if vepyr_v != vep_v:
                            n_mismatches += 1
                            fout.write(
                                f"{vk[0]}\t{vk[1]}\t{vk[2]}\t{vk[3]}\t{i}\t{f}\t{vepyr_v}\t{vep_v}\n"
                            )

            vepyr_line = fv.readline()
            vep_line = fg.readline()

    print(f"  Variants compared: {n_compared:,}")
    print(f"  Total field-level mismatches: {n_mismatches:,}")
    print(f"  Output: {out_tsv}")


def main():
    vep_fields = get_csq_fields(VEP_VCF)

    # Extract VEP key+CSQ once
    vep_kc = os.path.join(WORK_DIR, "vep_reference.keyscsq")
    extract_keyscsq(VEP_VCF, vep_kc)

    for backend, vcf_path in BACKENDS.items():
        print(f"\n{'=' * 60}")
        print(f"Extracting ALL mismatches: {backend} vs VEP")
        print(f"{'=' * 60}")

        vepyr_fields = get_csq_fields(vcf_path)
        vepyr_kc = os.path.join(WORK_DIR, f"vepyr_{backend}.keyscsq")
        extract_keyscsq(vcf_path, vepyr_kc)

        out_tsv = os.path.join(REPORT_DIR, f"mismatches_{backend}_vs_vep.tsv")
        compare_and_dump(vepyr_kc, vep_kc, vepyr_fields, vep_fields, out_tsv)

        # Cleanup
        os.remove(vepyr_kc)

    # Cleanup VEP reference
    os.remove(vep_kc)


if __name__ == "__main__":
    main()
