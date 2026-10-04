"""Create explicitly synthetic reverse-strand variants in real merged116 context.

Requires the downloaded native merged116 cache, bcftools, bgzip and tabix, plus
Docker with the pinned VEP 116.2 image. DATA_VEPYR_DIR identifies the data root;
BCFTOOLS optionally selects the normalization binary. No stock cache is modified.
"""

import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


HERE = Path(__file__).resolve().parent
IMAGE = "ensemblorg/ensembl-vep@sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da"


def main():
    data = Path(os.environ["DATA_VEPYR_DIR"]).resolve()
    source = data / "homo_sapiens_merged/116_GRCh38"
    native = HERE / "native/homo_sapiens_merged/116_GRCh38"
    native.mkdir(parents=True, exist_ok=True)
    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "-v",
        f"{source}:/source:ro",
        "-v",
        f"{HERE}:/fixture",
        IMAGE,
        "perl",
        "/fixture/slice_native.pl",
        "/source",
        "/fixture/native/homo_sapiens_merged/116_GRCh38",
    ]
    subprocess.run(command, check=True)
    for path in (native / "1").glob("*.storable"):
        with (
            path.open("rb") as input_file,
            path.with_suffix(".gz").open("wb") as output,
        ):
            with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed:
                shutil.copyfileobj(input_file, compressed)
        path.unlink()
    for name in ["info.txt", "chr_synonyms.txt"]:
        shutil.copy2(source / name, native / name)

    columns = next(
        line.split("\t", 1)[1].split(",")
        for line in (source / "info.txt").read_text().splitlines()
        if line.startswith("variation_cols\t")
    )
    # All names/alleles/frequencies below are synthetic. Raw cached alleles are
    # deliberately in their declared strand, including multiallelic AF keys.
    cases = [
        (1, "C/A", "-1", "A:0.2", ".", "."),
        (2, "C/T", "-1", "T:0.7", ".", "."),
        (3, "G/T", "-1", "T:0.9", ".", "."),
        (4, "G/T", "1", "T:0.3", "T:benign", "G"),
        (5, "G/T", ".", "T:0.31", ".", "."),
        (6, "G/T", "0", "T:0.32", ".", "."),
        (7, "C/A/T", "-1", "A:0.42,T:0.81", ".", "."),
        (8, "C/A", "-1", ".", "T:likely_pathogenic", "G"),
        (9, "C/A", "-1", ".", "T:uncertain_significance", "."),
    ]
    rows = []
    definitions = []
    for number, alleles, strand, frequency, clinical, reference in cases:
        definition = dict(
            chr="1",
            variation_name=f"rs900000000{number}",
            start="1000000",
            end="1000000",
            allele_string=alleles,
            strand=strand,
            AF=frequency,
            clin_sig_allele=clinical,
            clin_sig_ref_allele=reference,
        )
        # Isolate clinical terms at different loci: Perl hash iteration makes
        # the ordering of multiple clinical terms nondeterministic in VEP.
        if number in (8, 9):
            definition["start"] = definition["end"] = str(
                1000004 if number == 8 else 1000006
            )
        definitions.append(definition)
    definitions += [
        dict(
            chr="1",
            variation_name="rs9000000010",
            start="1000000",
            end="1000001",
            allele_string="CC/AA",
            strand="-1",
            AF="AA:0.17",
        ),
        dict(
            chr="1",
            variation_name="rs9000000011",
            start="1000001",
            end="1000001",
            allele_string="C/-",
            strand="-1",
            AF="-:0.19",
        ),
    ]
    definitions.append(
        dict(
            chr="1",
            variation_name="rs9000000012",
            start="1000003",
            end="1000003",
            allele_string="C/A",
            strand="-1",
            clin_sig_allele="A:pathogenic",
            clin_sig_ref_allele="C",
        )
    )
    definitions.sort(key=lambda row: (int(row["start"]), int(row["end"])))
    for definition in definitions:
        rows.append("\t".join(definition.get(column, ".") for column in columns))
    plain = native / "1/all_vars.txt"
    plain.write_text("\n".join(rows) + "\n")
    with (native / "1/all_vars.gz").open("wb") as output:
        subprocess.run(["bgzip", "-c", str(plain)], stdout=output, check=True)
    subprocess.run(
        ["tabix", "-f", "-s", "1", "-b", "5", "-e", "6", str(native / "1/all_vars.gz")],
        check=True,
    )
    plain.unlink()

    inputs = {}
    bcftools = os.environ.get("BCFTOOLS", "bcftools")
    for name, pos, ref, alt in [
        ("snv", 1000000, "G", "T"),
        ("mnv", 1000000, "GG", "TT"),
        ("deletion", 1000000, "GG", "G"),
        ("clinical_reverse", 1000003, "G", "T"),
        ("clinical_forward", 1000004, "G", "T"),
        ("clinical_missing", 1000006, "G", "T"),
    ]:
        raw = HERE / f"{name}.raw.vcf"
        raw.write_text(
            "##fileformat=VCFv4.2\n##contig=<ID=1>\n"
            "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
            f"1\t{pos}\t.\t{ref}\t{alt}\t.\t.\t.\n"
        )
        normalized = HERE / f"{name}.vcf"
        norm = [
            bcftools,
            "norm",
            "-m",
            "-both",
            "--no-version",
            "-o",
            str(normalized),
            str(raw),
        ]
        subprocess.run(norm, check=True)
        raw.unlink()
        with (HERE / f"{name}.vcf.gz").open("wb") as output:
            subprocess.run(["bgzip", "-c", str(normalized)], stdout=output, check=True)
        subprocess.run(
            ["tabix", "-f", "-p", "vcf", str(HERE / f"{name}.vcf.gz")], check=True
        )
        inputs[name] = dict(
            normalization=norm,
            input_sha256=hashlib.sha256(normalized.read_bytes()).hexdigest(),
        )
    # Preserve whole-chromosome coordinates; retain real sequence over the
    # entire selected transcript/HGVS context, with N padding elsewhere.
    start, end, length = 993966, 1016540, 248956422
    fasta = data / "input/Homo_sapiens.GRCh38.dna.primary_assembly.fa"
    region = subprocess.check_output(
        ["samtools", "faidx", str(fasta), f"1:{start}-{end}"], text=True
    )
    sequence = "".join(region.splitlines()[1:])
    assert len(sequence) == end - start + 1
    compressed = HERE / "reference.fa.gz"
    with compressed.open("wb") as output:
        process = subprocess.Popen(
            ["bgzip", "-c"], stdin=subprocess.PIPE, stdout=output
        )
        process.stdin.write(b">1\n")
        # Bound memory while keeping a single sequence line, as in .fai.
        count = start - 1
        while count:
            size = min(count, 1024 * 1024)
            process.stdin.write(b"N" * size)
            count -= size
        process.stdin.write(sequence.encode())
        count = length - end
        while count:
            size = min(count, 1024 * 1024)
            process.stdin.write(b"N" * size)
            count -= size
        process.stdin.write(b"\n")
        process.stdin.close()
        assert process.wait() == 0
    subprocess.run(["samtools", "faidx", str(compressed)], check=True)
    (HERE / "fasta-region.json").write_text(
        json.dumps(
            dict(chrom="1", start=start, end=end, chromosome_length=length), indent=2
        )
        + "\n"
    )
    receipt = dict(
        synthetic=True,
        limitation="Capability proof only; neither blocked natural-cache port is qualified by these injected rows.",
        image=IMAGE,
        native_slice_command=command,
        synthetic_variations=definitions,
        native_info_sha256=hashlib.sha256(
            (source / "info.txt").read_bytes()
        ).hexdigest(),
        inputs=inputs,
    )
    (HERE / "provenance.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
