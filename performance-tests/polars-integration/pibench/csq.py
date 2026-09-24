"""Give a VEP-annotated VCF the column shapes of vepyr's LazyFrame.

Only the fields a query reads are derived, and the parse is inside the timed
region, since filter_vep parses CSQ too. Empty and '-' become null, as
filter_vep's parse_line does. Variant-level fields are taken from the first
entry that carries them (co-located and per-variant plugin values repeat on
every entry).
"""

from __future__ import annotations

import gzip
import re
from pathlib import Path
from typing import Iterable

import polars as pl

VARIANT_FLOAT = {"MAX_AF", "gnomADg_AF", "gnomADe_AF", "AF"}
VARIANT_LIST = {"Existing_variation", "CLIN_SIG"}
VARIANT_STRING = {"CADD_PHRED", "ClinVar_CLNSIG"}
_FORMAT = re.compile(r'##INFO=<ID=CSQ,.*Format: ([^"]+)"')


def csq_fields(vcf: Path) -> list[str]:
    opener = gzip.open if str(vcf).endswith((".gz", ".bgz")) else open
    with opener(vcf, "rt") as fh:
        for line in fh:
            if not line.startswith("#"):
                break
            m = _FORMAT.match(line)
            if m:
                return m.group(1).split("|")
    raise ValueError(f"no CSQ Format in {vcf}")


def _clean(x: pl.Expr) -> pl.Expr:
    return pl.when(x.is_in(["", "-"])).then(None).otherwise(x)


def derive(fields: list[str], needed: Iterable[str]) -> dict[str, pl.Expr]:
    idx = {f: i for i, f in enumerate(fields)}
    out: dict[str, pl.Expr] = {}
    for name in needed:
        if name in ("chrom", "start", "ref", "alt"):
            continue
        per_entry = pl.col("CSQ").list.eval(
            _clean(pl.element().str.split("|").list.get(idx[name], null_on_oob=True))
        )
        first = per_entry.list.drop_nulls().list.first()
        if name in VARIANT_FLOAT:
            out[name] = first.cast(pl.Float32, strict=False)
        elif name in VARIANT_LIST:
            out[name] = first.str.split("&")
        elif name in VARIANT_STRING:
            out[name] = first
        else:
            out[name] = per_entry
    return out
