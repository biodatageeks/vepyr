"""polars-bio frame metadata, so ``polars_bio.sink_vcf`` can write the frame.

The only module that imports polars-bio. Without it the frame is returned as is.
"""

from __future__ import annotations

import pyarrow as pa


def build_header(
    schema: pa.Schema,
    carried: dict[str, tuple[str, str]],
    csq_description: str | None,
    extract,
) -> dict | None:
    """VCF header metadata in polars-bio's shape, keyed by VCF id.

    ``csq_description`` is the engine's own ``CSQ`` description, or ``None``
    when this run writes no CSQ. It is never built here: the field list follows
    the flags, the cache source type, the pick options and the plugin
    manifests, so only the engine can say what it is.
    """
    vcf = extract(schema).get("format_specific", {}).get("vcf")
    if not vcf:
        return None
    ids = {name: vcf_id for name, (_, vcf_id) in carried.items()}
    info = {
        ids.get(name, name): definition
        for name, definition in (vcf.get("info_fields") or {}).items()
        # The input's own CSQ is replaced by this run's, as Ensembl VEP does --
        # but only when this run has one. With `skip_csq=True` nothing replaces
        # it, so dropping the declaration here would write the input's CSQ
        # column under no definition at all, losing the annotation it came with.
        if csq_description is None or ids.get(name, name) != "CSQ"
    }
    if csq_description is not None:
        info["CSQ"] = {
            "number": ".",
            "type": "String",
            "description": csq_description,
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
        # The input's header as text: polars-bio writes it back line for line and
        # re-declares only what changed (CSQ). Without it the header is rebuilt
        # from the typed keys above, which cannot hold ##fileDate, tool
        # provenance, the PASS filter or contig attributes beyond ID and length.
        "raw_lines": vcf.get("raw_lines"),
        # Whether `_vcf_info_keys` / `_vcf_format_keys` in this frame are the
        # record layout. polars-bio goes by this, never by the column names.
        "record_layout": bool(vcf.get("record_layout")),
    }


def attach(
    lf, vcf_path: str, schema: pa.Schema, carried, writes_csq: bool, provenance=None
) -> None:
    """Set the metadata ``polars_bio.sink_vcf`` reads; a no-op without polars-bio.

    ``provenance`` maps the input's raw header lines to what an annotated VCF
    carries: those lines with this run's provenance merged in, and the ``CSQ``
    description to declare beside them, both built by the engine so they are
    exactly what ``output_vcf`` writes. ``writes_csq`` says whether this run
    has a CSQ of its own to declare; the engine hands over a description
    either way.
    """
    try:
        import polars_bio as pb
        from polars_bio._metadata import set_coordinate_system
        from polars_bio.metadata_extractors import extract_all_schema_metadata
    except ImportError:
        return
    # The lines and the description come from one engine call, and the header
    # needs the description as it is built, so the call comes first.
    lines = csq_description = None
    if provenance is not None:
        source = (
            extract_all_schema_metadata(schema).get("format_specific", {}).get("vcf")
        )
        if source and source.get("raw_lines"):
            lines, csq_description = provenance(source["raw_lines"])
    header = build_header(
        schema,
        carried,
        csq_description if writes_csq else None,
        extract_all_schema_metadata,
    )
    if header is None:
        return
    if lines is not None:
        header["raw_lines"] = lines
    pb.set_source_metadata(lf, format="vcf", path=vcf_path, header=header)
    set_coordinate_system(lf, zero_based=False)
