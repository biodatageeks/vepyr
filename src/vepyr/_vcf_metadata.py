"""polars-bio frame metadata, so ``polars_bio.sink_vcf`` can write the frame.

The only module that imports polars-bio. Without it the frame is returned as is.
"""

from __future__ import annotations

import pyarrow as pa

_CSQ_DESCRIPTION = "Consequence annotations from Ensembl VEP. Format: "


def build_header(
    schema: pa.Schema,
    carried: dict[str, tuple[str, str]],
    csq_fields: list[str] | None,
    extract,
) -> dict | None:
    """VCF header metadata in polars-bio's shape, keyed by VCF id."""
    vcf = extract(schema).get("format_specific", {}).get("vcf")
    if not vcf:
        return None
    ids = {name: vcf_id for name, (_, vcf_id) in carried.items()}
    info = {
        ids.get(name, name): definition
        for name, definition in (vcf.get("info_fields") or {}).items()
        # The input's own CSQ is replaced by this run's, as Ensembl VEP does.
        if ids.get(name, name) != "CSQ"
    }
    if csq_fields is not None:
        info["CSQ"] = {
            "number": ".",
            "type": "String",
            "description": _CSQ_DESCRIPTION + "|".join(csq_fields),
        }
    return {
        "info_fields": info,
        "format_fields": {
            ids.get(name, name): definition
            for name, definition in (vcf.get("format_fields") or {}).items()
        },
        "sample_names": vcf.get("sample_names"),
        "version": vcf.get("version"),
        "contigs": vcf.get("contigs"),
        "filters": vcf.get("filters"),
        "alt_definitions": vcf.get("alt_definitions"),
    }


def attach(lf, vcf_path: str, schema: pa.Schema, carried, csq_fields) -> None:
    """Set the metadata ``polars_bio.sink_vcf`` reads; a no-op without polars-bio."""
    try:
        import polars_bio as pb
        from polars_bio._metadata import set_coordinate_system
        from polars_bio.metadata_extractors import extract_all_schema_metadata
    except ImportError:
        return
    header = build_header(schema, carried, csq_fields, extract_all_schema_metadata)
    if header is None:
        return
    pb.set_source_metadata(lf, format="vcf", path=vcf_path, header=header)
    set_coordinate_system(lf, zero_based=False)
