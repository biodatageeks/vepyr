import gzip

import polars as pl

from pibench.csq import csq_fields, derive

HEADER = '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|CANONICAL|MAX_AF|Existing_variation|CADD_PHRED|am_class">\n'


def test_csq_fields_reads_plain_and_gzip(tmp_path):
    plain = tmp_path / "a.vcf"
    plain.write_text("##fileformat=VCFv4.2\n" + HEADER + "#CHROM\tPOS\n")
    gz = tmp_path / "a.vcf.gz"
    with gzip.open(gz, "wt") as fh:
        fh.write(plain.read_text())
    want = [
        "Allele",
        "Consequence",
        "IMPACT",
        "CANONICAL",
        "MAX_AF",
        "Existing_variation",
        "CADD_PHRED",
        "am_class",
    ]
    assert csq_fields(plain) == want == csq_fields(gz)


def test_derive_gives_vepyr_shapes():
    fields = [
        "Allele",
        "Consequence",
        "IMPACT",
        "CANONICAL",
        "MAX_AF",
        "Existing_variation",
        "CADD_PHRED",
        "am_class",
    ]
    df = pl.DataFrame(
        {
            "CSQ": [
                [
                    "T|missense_variant|MODERATE|YES|0.001|rs1&COSV2|25.1|likely_pathogenic",
                    "T|upstream_gene_variant|MODIFIER||0.001|rs1&COSV2|25.1|",
                ],
                ["-|intergenic_variant|MODIFIER|||-||"],
            ]
        }
    )
    exprs = derive(
        fields,
        [
            "IMPACT",
            "CANONICAL",
            "MAX_AF",
            "Existing_variation",
            "CADD_PHRED",
            "am_class",
        ],
    )
    out = df.select(**exprs)
    assert out.schema["IMPACT"] == pl.List(pl.String)
    assert out.schema["MAX_AF"] == pl.Float32
    assert out.schema["Existing_variation"] == pl.List(pl.String)
    assert out.schema["CADD_PHRED"] == pl.String
    r0, r1 = out.to_dicts()
    assert r0["IMPACT"] == ["MODERATE", "MODIFIER"]
    assert r0["CANONICAL"] == ["YES", None]  # '' -> null
    assert r0["am_class"] == ["likely_pathogenic", None]
    assert r0["Existing_variation"] == ["rs1", "COSV2"]
    assert abs(r0["MAX_AF"] - 0.001) < 1e-9 and r0["CADD_PHRED"] == "25.1"
    assert r1["MAX_AF"] is None and r1["Existing_variation"] is None  # '-' -> null


def test_derive_keeps_dash_as_deletion_allele():
    fields = [
        "Allele",
        "Consequence",
        "IMPACT",
        "CANONICAL",
        "MAX_AF",
        "Existing_variation",
        "CADD_PHRED",
        "am_class",
    ]
    df = pl.DataFrame(
        {
            "CSQ": [
                [
                    "-|intergenic_variant|MODIFIER|||-||",
                    "|intergenic_variant|MODIFIER|||-||",
                ]
            ]
        }
    )
    exprs = derive(fields, ["Allele"])
    out = df.select(**exprs)
    (row,) = out.to_dicts()
    assert row["Allele"] == ["-", None]  # '-' stays '-'; '' -> null
