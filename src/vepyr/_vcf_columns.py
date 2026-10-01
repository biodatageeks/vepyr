"""Input VCF columns on the annotation frame: selection, ids and pruning."""

from __future__ import annotations

import pyarrow as pa

_FIELD_TYPE = b"bio.vcf.field.field_type"
_FORMAT_ID = b"bio.vcf.field.format_id"
# Set by the engine on an input column it renamed because an annotation column
# owns the name (INFO/AF arrives as INFO_AF).
_SOURCE_NAME = b"bio.vep.source_field_name"
# Set by the reader on the two record-layout columns it carries. The name
# cannot stand in for it: a VCF may declare its own INFO field called
# `_vcf_info_keys`, which arrives as an ordinary column.
_RECORD_LAYOUT = b"bio.vcf.record_layout"


# The reader's own columns. An INFO field declaring one of these ids arrives
# beside the column it is named for, and the annotation query fails on the
# duplicate name, so such a field cannot be carried at all.
CORE_COLUMNS = frozenset(
    {"chrom", "start", "end", "id", "ref", "alt", "qual", "filter"}
)


def validate_selection(
    kind: str, requested: list[str] | None, available: list[str]
) -> None:
    """Reject ids the header does not declare; the reader would panic on them."""
    if not requested:
        return
    unknown = [name for name in requested if name not in available]
    if unknown:
        raise ValueError(
            f"{kind} names fields the VCF header does not declare: "
            + ", ".join(repr(name) for name in unknown)
            + ". Available: "
            + ", ".join(available)
        )


def carried_columns(schema: pa.Schema) -> dict[str, tuple[str, str]]:
    """Map each carried input column to its kind and VCF id."""
    carried: dict[str, tuple[str, str]] = {}
    for field in schema:
        # The nested FORMAT container of a multi-sample input. A VCF may also
        # declare an INFO field called `genotypes`; that one is an ordinary
        # column, so the type decides, not the name.
        if field.name == "genotypes" and pa.types.is_struct(field.type):
            carried[field.name] = ("FORMAT", "*")
            continue
        metadata = field.metadata or {}
        kind = metadata.get(_FIELD_TYPE, b"").decode()
        if kind == "FORMAT":
            # The reader's id for a FORMAT column, which differs from its name
            # when the reader or the engine had to rename it.
            fallback = metadata.get(_SOURCE_NAME, field.name.encode())
            carried[field.name] = (kind, metadata.get(_FORMAT_ID, fallback).decode())
        elif kind == "INFO":
            source = metadata.get(_SOURCE_NAME, field.name.encode())
            carried[field.name] = (kind, source.decode())
    return carried


def record_layout_carried(schema: pa.Schema) -> bool:
    """Whether this schema holds the reader's record-layout columns.

    `preserve_record_layout="auto"` is a request: it gives way for a BCF input
    or a file declaring a field with either name. This is the outcome.
    """
    return any((field.metadata or {}).get(_RECORD_LAYOUT) for field in schema)


def fields_for_query(
    carried: dict[str, tuple[str, str]],
    needed: set[str] | None,
    info: list[str] | None,
    fmt: list[str] | None,
) -> tuple[list[str] | None, list[str] | None]:
    """The INFO and FORMAT fields one collect has to read.

    Without a projection the user's selection stands. With one, only the carried
    columns the query names are read; nested genotypes cannot be split per field,
    so they are read as selected or not at all.
    """
    if needed is None:
        return info, fmt
    read_info = [
        vcf_id
        for name, (kind, vcf_id) in carried.items()
        if kind == "INFO" and name in needed
    ]
    if "genotypes" in carried:
        read_fmt = fmt if "genotypes" in needed else []
    else:
        read_fmt = [
            vcf_id
            for name, (kind, vcf_id) in carried.items()
            if kind == "FORMAT" and name in needed
        ]
    return read_info, read_fmt
