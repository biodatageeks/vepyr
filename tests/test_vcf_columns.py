"""INFO/FORMAT columns on the LazyFrame, their pruning, and sink_vcf metadata."""

from __future__ import annotations

import json
import os
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata

TESTS_DIR = Path(__file__).parent
GOLDEN_DIR = TESTS_DIR / "data" / "golden"
CACHE_DIR = str(GOLDEN_DIR / "cache")
INPUT_VCF = str(GOLDEN_DIR / "input.vcf.gz")
REFERENCE_FASTA = str(GOLDEN_DIR / "reference.fa")

GIAB_INFO = [
    "DPSum",
    "platforms",
    "platformnames",
    "platformbias",
    "datasets",
    "datasetnames",
    "datasetsmissingcall",
    "callsets",
    "callsetnames",
    "varType",
    "filt",
    "callable",
    "difficultregion",
    "arbitrated",
    "callsetwiththisuniqgenopassing",
    "callsetwithotheruniqgenopassing",
]
GIAB_FORMAT = ["DP", "GQ", "ADALL", "AD", "GT", "PS"]
CORE = ["chrom", "start", "end", "id", "ref", "alt", "qual", "filter"]


@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    if not os.path.isdir(CACHE_DIR):
        pytest.skip("Golden test cache not available")
    target = tmp_path_factory.mktemp("cache")
    return str(copy_cache_with_source_metadata(CACHE_DIR, target, "ensembl", "115"))


# --- native: header fields and the field selection ---------------------------


def test_vcf_fields_lists_header_ids_in_header_order():
    from vepyr._core import vcf_fields

    info, fmt = vcf_fields(INPUT_VCF)
    assert info == GIAB_INFO
    assert fmt == GIAB_FORMAT


def test_vcf_fields_missing_file_raises():
    from vepyr._core import vcf_fields

    with pytest.raises(RuntimeError):
        vcf_fields("/nonexistent/input.vcf.gz")


def _probe(cache_dir, **opts):
    from vepyr._core import create_annotator

    return create_annotator(INPUT_VCF, cache_dir, json.dumps(opts), True, 0).schema


def test_absent_keys_carry_every_field(cache_dir):
    names = _probe(cache_dir).names
    assert names[:8] == CORE
    assert names[8 : 8 + len(GIAB_INFO)] == GIAB_INFO
    carried_format = names[8 + len(GIAB_INFO) : 8 + len(GIAB_INFO) + len(GIAB_FORMAT)]
    assert carried_format == GIAB_FORMAT


def test_empty_lists_carry_nothing(cache_dir):
    names = _probe(cache_dir, vcf_info_fields=[], vcf_format_fields=[]).names
    assert names[:9] == [*CORE, "most_severe_consequence"]


def test_a_selection_carries_only_what_it_names(cache_dir):
    names = _probe(cache_dir, vcf_info_fields=["DPSum"], vcf_format_fields=["GT"]).names
    assert names[:11] == [*CORE, "DPSum", "GT", "most_severe_consequence"]


def test_the_engine_carries_the_vcf_header(cache_dir):
    """Prerequisite from the collision fix: schema-level VCF metadata survives."""
    metadata = _probe(cache_dir).metadata or {}
    assert any(key.startswith(b"bio.vcf") for key in metadata)


def test_emitted_batches_carry_the_same_schema_as_the_plan(cache_dir):
    from vepyr._core import create_annotator

    annotator = create_annotator(INPUT_VCF, cache_dir, json.dumps({}), True, None)
    batch = next(iter(annotator))
    assert batch.schema.metadata == annotator.schema.metadata
    assert batch.schema.names == annotator.schema.names


# --- _vcf_columns: ids, validation and pruning --------------------------------


def _field(name, kind, fmt_id=None, source=None, dtype=pa.int32()):
    meta = {"bio.vcf.field.field_type": kind}
    if fmt_id:
        meta["bio.vcf.field.format_id"] = fmt_id
    if source:
        meta["bio.vep.source_field_name"] = source
    return pa.field(name, dtype, metadata=meta)


CARRIED_SCHEMA = pa.schema(
    [
        pa.field("chrom", pa.string()),
        _field("DP", "INFO"),
        _field("INFO_AF", "INFO", source="AF", dtype=pa.float32()),
        _field("GT", "FORMAT", "GT", dtype=pa.string()),
        _field("fmt_DP", "FORMAT", "DP"),
        pa.field("CSQ", pa.string()),
        pa.field("most_severe_consequence", pa.string()),
        pa.field("AF", pa.float32()),
    ]
)


def test_carried_columns_map_names_to_vcf_ids():
    from vepyr._vcf_columns import carried_columns

    assert carried_columns(CARRIED_SCHEMA) == {
        "DP": ("INFO", "DP"),
        "INFO_AF": ("INFO", "AF"),
        "GT": ("FORMAT", "GT"),
        "fmt_DP": ("FORMAT", "DP"),
    }


def test_a_field_really_named_with_the_prefix_keeps_its_id():
    from vepyr._vcf_columns import carried_columns

    schema = pa.schema([_field("INFO_X", "INFO"), pa.field("X", pa.string())])
    assert carried_columns(schema) == {"INFO_X": ("INFO", "INFO_X")}


def test_the_layout_columns_are_recognised_by_metadata_not_by_name():
    """A VCF may declare its own INFO field named `_vcf_info_keys`; it arrives
    as an ordinary column, so only the reader's metadata marks a real carry."""
    from vepyr._vcf_columns import record_layout_carried

    carried = pa.schema(
        [
            pa.field("chrom", pa.string()),
            pa.field(
                "_vcf_info_keys",
                pa.string(),
                metadata={"bio.vcf.record_layout": "info_keys"},
            ),
        ]
    )
    assert record_layout_carried(carried)
    assert not record_layout_carried(pa.schema([pa.field("chrom", pa.string())]))
    # Same name, but the file's own field.
    assert not record_layout_carried(pa.schema([_field("_vcf_info_keys", "INFO")]))


def test_a_query_reading_no_carried_column_reads_no_fields():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, {"chrom", "AF"}, None, None) == ([], [])


def test_a_query_reads_only_the_fields_it_names():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, {"INFO_AF", "fmt_DP"}, None, None) == (
        ["AF"],
        ["DP"],
    )


def test_no_projection_keeps_the_user_selection():
    from vepyr._vcf_columns import carried_columns, fields_for_query

    carried = carried_columns(CARRIED_SCHEMA)
    assert fields_for_query(carried, None, ["DP"], None) == (["DP"], None)


def test_multi_sample_genotypes_keep_the_user_format_selection():
    from vepyr._vcf_columns import fields_for_query

    carried = {"genotypes": ("FORMAT", "*")}
    assert fields_for_query(carried, {"genotypes"}, None, ["GT"]) == ([], ["GT"])
    assert fields_for_query(carried, {"chrom"}, None, ["GT"]) == ([], [])


def test_unknown_id_is_a_value_error_listing_what_exists():
    from vepyr._vcf_columns import validate_selection

    with pytest.raises(ValueError, match=r"info_fields.*'NOPE'.*DP, AF"):
        validate_selection("info_fields", ["DP", "NOPE"], ["DP", "AF"])
    validate_selection("info_fields", None, ["DP"])
    validate_selection("info_fields", [], ["DP"])


# --- annotate(): the selection and per-collect pruning -------------------------


class _FakeAnnotator:
    schema = pa.schema(
        [
            pa.field("chrom", pa.string()),
            pa.field("start", pa.uint32()),
            pa.field("end", pa.uint32()),
            _field("DP", "INFO"),
            _field("GT", "FORMAT", "GT", dtype=pa.string()),
            pa.field("most_severe_consequence", pa.string()),
            pa.field("SYMBOL", pa.list_(pa.string())),
        ]
    )

    def __iter__(self):
        return iter(())


@pytest.fixture
def fake_engine(monkeypatch):
    """Capture the options every annotator creation receives."""
    import vepyr

    seen: list[dict] = []

    def fake_create(vcf_path, cache_dir, options_json, skip_csq=True, limit=None):
        seen.append(json.loads(options_json))
        return _FakeAnnotator()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create)
    monkeypatch.setattr(vepyr, "_vcf_fields", lambda path: (["DP", "AF"], ["GT", "DP"]))
    monkeypatch.setattr(vepyr, "_vcf_contigs", lambda path: ["chr1"])
    return seen


def _annotate(**kwargs):
    import vepyr

    return vepyr.annotate(INPUT_VCF, CACHE_DIR, show_progress=False, **kwargs)


def _selection(opts):
    return opts.get("vcf_info_fields"), opts.get("vcf_format_fields")


def test_default_sends_no_selection_so_every_field_is_carried(fake_engine):
    _annotate().collect()
    # seen[0] is the schema probe, seen[1] the collect.
    assert [_selection(opts) for opts in fake_engine] == [(None, None), (None, None)]


def test_selection_reaches_the_probe_and_an_unprojected_collect(fake_engine):
    _annotate(info_fields=["DP"], format_fields=[]).collect()
    assert [_selection(opts) for opts in fake_engine] == [(["DP"], []), (["DP"], [])]


def test_a_projection_without_input_columns_reads_none(fake_engine):
    _annotate().select("chrom", "SYMBOL").collect()
    assert _selection(fake_engine[1]) == ([], [])


def test_a_filtered_input_column_is_read_even_when_not_selected(fake_engine):
    _annotate().filter(pl.col("DP") > 10).select("chrom", "SYMBOL").collect()
    assert _selection(fake_engine[1]) == (["DP"], [])


def test_unknown_field_raises_before_any_annotator_is_created(fake_engine):
    with pytest.raises(ValueError, match="format_fields"):
        _annotate(format_fields=["NOPE"])
    assert fake_engine == []


def test_output_vcf_ignores_the_selection(monkeypatch, tmp_path):
    import vepyr

    captured = {}

    def fake_annotate_vcf(vcf, cache_dir, output, options_json, *rest):
        captured.update(json.loads(options_json))
        return 0

    monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
    vepyr.annotate(
        INPUT_VCF,
        CACHE_DIR,
        output_vcf=str(tmp_path / "o.vcf"),
        info_fields=["DP"],
        show_progress=False,
    )
    assert "vcf_info_fields" not in captured
    assert "vcf_format_fields" not in captured


# --- the real frame ------------------------------------------------------------


def _real(cache_dir, **kwargs):
    import vepyr

    return vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        reference_fasta=REFERENCE_FASTA,
        show_progress=False,
        **kwargs,
    )


def test_carried_columns_hold_the_inputs_values(cache_dir):
    df = (
        _real(cache_dir)
        .select("chrom", "start", "ref", "alt", "DPSum", "GT", "AD", "SYMBOL")
        .collect()
    )
    assert df.height == 100
    assert df["GT"].null_count() == 0
    assert df.schema["AD"] == pl.List(pl.Int32)


def test_pruned_and_unpruned_queries_agree(cache_dir):
    lf = _real(cache_dir)
    columns = ["chrom", "start", "SYMBOL", "Consequence"]
    assert lf.select(columns).collect().equals(lf.collect().select(columns))


def test_empty_selection_is_the_0_7_frame(cache_dir):
    names = list(_real(cache_dir, info_fields=[], format_fields=[]).collect_schema())
    assert names[:9] == [*CORE, "most_severe_consequence"]


# --- plugin columns that share an input field's id ------------------------------


def test_input_field_named_like_a_plugin_column_is_renamed():
    from vepyr import _rename_shadowed_input_columns

    schema = {
        "chrom": pl.String,
        "CADD_PHRED": pl.Float32,
        "GT": pl.String,
        "SYMBOL": pl.String,
    }
    carried = {"CADD_PHRED": ("INFO", "CADD_PHRED"), "GT": ("FORMAT", "GT")}
    renamed, mapping = _rename_shadowed_input_columns(schema, carried, ["CADD_PHRED"])
    assert list(renamed) == ["chrom", "INFO_CADD_PHRED", "GT", "SYMBOL"]
    assert mapping == {"CADD_PHRED": "INFO_CADD_PHRED"}


def test_annotation_column_named_like_a_plugin_field_still_raises():
    from vepyr import _rename_shadowed_input_columns

    with pytest.raises(ValueError, match="conflicts with an existing DataFrame column"):
        _rename_shadowed_input_columns({"SYMBOL": pl.String}, {}, ["SYMBOL"])


# --- frame metadata for polars_bio.sink_vcf --------------------------------------


def _fake_extract(schema):
    return {
        "format_specific": {
            "vcf": {
                "info_fields": {
                    "DP": {"number": "1", "type": "Integer", "description": "d"},
                    "INFO_AF": {
                        "number": "A",
                        "type": "Float",
                        "description": "cohort",
                    },
                    "INFO_CSQ": {
                        "number": ".",
                        "type": "String",
                        "description": "old Format: X",
                    },
                },
                "format_fields": {
                    "GT": {"number": "1", "type": "String", "description": "g"}
                },
                "sample_names": ["S1"],
                "version": "VCFv4.2",
                "contigs": [{"id": "chr1", "length": 10}],
                "filters": [],
                "alt_definitions": [],
            }
        }
    }


def test_header_is_keyed_by_vcf_id_and_csq_is_replaced():
    from vepyr._vcf_metadata import build_header

    carried = {
        "DP": ("INFO", "DP"),
        "INFO_AF": ("INFO", "AF"),
        "INFO_CSQ": ("INFO", "CSQ"),
        "GT": ("FORMAT", "GT"),
    }
    header = build_header(
        CARRIED_SCHEMA, carried, ["Allele", "Consequence"], _fake_extract
    )
    assert set(header["info_fields"]) == {"DP", "AF", "CSQ"}
    assert header["info_fields"]["AF"]["description"] == "cohort"
    assert header["info_fields"]["CSQ"] == {
        "number": ".",
        "type": "String",
        "description": (
            "Consequence annotations from Ensembl VEP. Format: Allele|Consequence"
        ),
    }
    assert header["sample_names"] == ["S1"]


def test_without_a_csq_column_no_csq_is_declared():
    from vepyr._vcf_metadata import build_header

    header = build_header(CARRIED_SCHEMA, {"DP": ("INFO", "DP")}, None, _fake_extract)
    assert "CSQ" not in header["info_fields"]


def test_a_schema_the_extractor_does_not_recognise_gives_no_header():
    from vepyr._vcf_metadata import build_header

    unrecognised = lambda schema: {"format_specific": {}}  # noqa: E731
    assert build_header(CARRIED_SCHEMA, {}, None, unrecognised) is None


def test_the_provenance_records_the_worker_count_the_frame_is_collected_with(
    cache_dir,
):
    """The header describes this run, and the frame is collected with the
    caller's workers, so the provenance cannot build its config with a fixed
    one."""
    from vepyr._core import annotation_header_lines

    def recorded(**opts):
        lines = annotation_header_lines(
            INPUT_VCF,
            cache_dir,
            json.dumps({"everything": True, **opts}),
            ["##fileformat=VCFv4.2"],
        )
        line = next(
            line
            for line in lines
            if line.startswith("##datafusion-bio-function-vep-command-line=")
        )
        return json.loads(line.split("='", 1)[1][:-1])["options"]["workers"]

    assert recorded() == 1
    assert recorded(workers=4) == 4


def test_csq_field_names_skip_cache_only_columns_and_append_plugins():
    from vepyr import _csq_field_names

    names = [
        "chrom",
        "CSQ",
        "most_severe_consequence",
        "Allele",
        "Consequence",
        "dbsnp_ids",
    ]
    assert _csq_field_names(names, None, ["CADD_PHRED"]) == [
        "Allele",
        "Consequence",
        "CADD_PHRED",
    ]
    assert _csq_field_names(names, ["Consequence"], ["CADD_PHRED"]) == [
        "Consequence",
        "CADD_PHRED",
    ]


# --- sink_vcf agrees with output_vcf ---------------------------------------------


def _records(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        if line.startswith("#"):
            continue
        f = line.split("\t")
        fmt = {k: v for k, v in zip(f[8].split(":"), f[9].split(":")) if v != "."}
        out[(f[0], f[1], f[3], f[4])] = (
            f[2],
            f[5],
            f[6],
            frozenset(f[7].split(";")),
            frozenset(fmt.items()),
        )
    return out


def _header_lines(path, prefix):
    return [
        line for line in Path(path).read_text().splitlines() if line.startswith(prefix)
    ]


# Flag sets both output paths honour. The co-located flags (check_existing, af*,
# max_af, pubmed) are left out on purpose: without `everything` the VCF output
# path does not forward them to the engine, so the two paths differ there for a
# reason that has nothing to do with sink_vcf.
#
# Without `everything` the engine's CSQ layout is shorter than the typed-column
# list the header is derived from here, so the declared `Format:` is wrong. The
# engine has to expose its own Format string on the CSQ field (plan, Task 5);
# until it does that case is expected to fail, strictly, so it cannot be
# forgotten once fixed. A flagless frame is not comparable at all: it runs
# `everything` when it has a FASTA, and a flagless output_vcf does not.
@pytest.mark.parametrize(
    "flags",
    [
        {"everything": True},
        pytest.param(
            {"hgvs": True},
            marks=pytest.mark.xfail(
                strict=True,
                reason="CSQ Format: is derived from typed columns; wrong without everything",
            ),
        ),
    ],
)
def test_sink_vcf_matches_output_vcf_field_for_field(cache_dir, tmp_path, flags):
    import vepyr

    pb = pytest.importorskip("polars_bio")
    kwargs = dict(reference_fasta=REFERENCE_FASTA, show_progress=False, **flags)

    reference = tmp_path / "reference.vcf"
    vepyr.annotate(
        INPUT_VCF, cache_dir, output_vcf=str(reference), compression="plain", **kwargs
    )

    sunk = tmp_path / "sunk.vcf"
    lf = vepyr.annotate(INPUT_VCF, cache_dir, skip_csq=False, **kwargs)
    pb.sink_vcf(lf.filter(pl.col("IMPACT").list.contains("MODIFIER")), str(sunk))

    got, want = _records(sunk), _records(reference)
    assert got and set(got) <= set(want)
    assert all(got[key] == want[key] for key in got)
    # The same CSQ layout declared, the input's header kept, GT first.
    assert _header_lines(sunk, "##INFO=<ID=CSQ") == _header_lines(
        reference, "##INFO=<ID=CSQ"
    )
    first = next(
        line for line in sunk.read_text().splitlines() if not line.startswith("#")
    )
    assert first.split("\t")[8].startswith("GT")


def test_sink_vcf_keeps_the_input_header_and_can_reproduce_record_lines(
    cache_dir, tmp_path
):
    """sink_vcf writes what output_vcf writes. Needs a polars-bio with raw-header
    passthrough and the record layout carry (biodatageeks/polars-bio#469)."""
    import inspect

    import vepyr

    pb = pytest.importorskip("polars_bio")
    if "preserve_record_layout" not in inspect.signature(pb.scan_vcf).parameters:
        pytest.skip("polars-bio without the record layout carry")
    kwargs = dict(reference_fasta=REFERENCE_FASTA, show_progress=False, everything=True)

    reference = tmp_path / "reference.vcf"
    vepyr.annotate(
        INPUT_VCF, cache_dir, output_vcf=str(reference), compression="plain", **kwargs
    )
    want = {
        tuple(line.split("\t")[i] for i in (0, 1, 3, 4)): line
        for line in reference.read_text().splitlines()
        if not line.startswith("#")
    }

    sunk = tmp_path / "sunk.vcf"
    lf = vepyr.annotate(
        INPUT_VCF, cache_dir, skip_csq=False, preserve_record_layout=True, **kwargs
    )
    assert {"_vcf_info_keys", "_vcf_format_keys"} <= set(lf.collect_schema().names())
    pb.sink_vcf(lf.filter(pl.col("IMPACT").list.contains("MODIFIER")), str(sunk))

    lines = sunk.read_text().splitlines()
    got = [line for line in lines if not line.startswith("#")]
    assert got
    # Byte for byte the lines output_vcf writes, which are Ensembl VEP's.
    assert all(
        line == want[tuple(line.split("\t")[i] for i in (0, 1, 3, 4))] for line in got
    )
    # The header is output_vcf's, line for line and in order: the input's own
    # lines, then vepyr's provenance, then CSQ. One line may differ, and only by
    # what this path does not have: an output file and its compression.
    import json

    got_header = [line for line in lines if line.startswith("#")]
    want_header = [
        line for line in reference.read_text().splitlines() if line.startswith("#")
    ]
    assert len(got_header) == len(want_header)
    differing = [(g, w) for g, w in zip(got_header, want_header) if g != w]
    assert len(differing) == 1
    sunk_line, reference_line = differing[0]
    assert sunk_line.startswith("##datafusion-bio-function-vep-command-line='")
    sunk_json, reference_json = (
        json.loads(line.split("='", 1)[1][:-1]) for line in (sunk_line, reference_line)
    )
    assert "output" not in sunk_json and "compression" not in sunk_json
    assert sunk_json == {
        key: value
        for key, value in reference_json.items()
        if key not in ("output", "compression")
    }

    # On by default; False leaves the two columns out of the caller's frame.
    default = vepyr.annotate(INPUT_VCF, cache_dir, skip_csq=False, **kwargs)
    assert {"_vcf_info_keys", "_vcf_format_keys"} <= set(
        default.collect_schema().names()
    )
    plain = vepyr.annotate(
        INPUT_VCF, cache_dir, skip_csq=False, preserve_record_layout=False, **kwargs
    )
    assert not [c for c in plain.collect_schema().names() if c.startswith("_vcf_")]


def test_the_default_layout_carry_gives_way_to_an_input_that_cannot_have_it(
    cache_dir, tmp_path
):
    """A file may declare a field named like a layout column. The default then
    annotates without the carry; asking for it explicitly is an error."""
    import vepyr

    src = tmp_path / "reserved.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        '##INFO=<ID=_vcf_info_keys,Number=1,Type=String,Description="Real">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t602113\t.\tT\tTGCCCA\t50\tPASS\t_vcf_info_keys=mine\n"
    )
    lf = vepyr.annotate(str(src), cache_dir, show_progress=False)
    names = lf.collect_schema().names()
    assert "_vcf_format_keys" not in names
    assert names.count("_vcf_info_keys") == 1  # the file's own field, as data

    with pytest.raises(ValueError, match="preserve_record_layout"):
        vepyr.annotate(
            str(src), cache_dir, preserve_record_layout=True, show_progress=False
        ).collect()


def test_an_input_field_named_genotypes_is_carried_as_data(cache_dir, tmp_path):
    """A multi-sample input nests its FORMAT fields under one `genotypes`
    struct, but a file may also declare an INFO field with that name. The type
    decides: by name alone the header listing skipped the field and the column
    was classified as the FORMAT container, so a projection naming it dropped
    it."""
    from vepyr._core import vcf_fields

    import vepyr

    src = tmp_path / "genotypes.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        '##INFO=<ID=genotypes,Number=1,Type=String,Description="Not the container">\n'
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t602113\t.\tT\tTGCCCA\t50\tPASS\tgenotypes=mine;DP=7\n"
    )
    # Declared, so it can be named in info_fields and validated.
    assert vcf_fields(str(src)) == (["genotypes", "DP"], [])

    lf = vepyr.annotate(str(src), cache_dir, show_progress=False)
    assert "genotypes" in lf.collect_schema().names()
    # The projection is what regressed: read as FORMAT, the column never
    # arrived.
    assert lf.select("genotypes").collect()["genotypes"].to_list() == ["mine"]


def test_the_provenance_records_the_layout_that_was_carried_not_the_one_asked_for(
    cache_dir, tmp_path, monkeypatch
):
    """`auto` gives way for an input that cannot carry the record layout. The
    provenance has to describe the run, so it takes the outcome from the probe's
    schema rather than leaving the engine's default to speak for it."""
    import vepyr

    pytest.importorskip("polars_bio")  # without it no provenance is built

    seen = []
    real = vepyr._annotation_header_lines

    def capture(vcf, cache, options_json, raw_lines):
        seen.append(json.loads(options_json))
        return real(vcf, cache, options_json, raw_lines)

    monkeypatch.setattr(vepyr, "_annotation_header_lines", capture)

    vepyr.annotate(INPUT_VCF, cache_dir, show_progress=False)
    assert seen[-1]["preserve_record_layout"] is True

    # A file declaring a layout column: the carry gives way, and so must the
    # recorded option.
    src = tmp_path / "reserved.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        '##INFO=<ID=_vcf_info_keys,Number=1,Type=String,Description="Real">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t602113\t.\tT\tTGCCCA\t50\tPASS\t_vcf_info_keys=mine\n"
    )
    vepyr.annotate(str(src), cache_dir, show_progress=False)
    assert seen[-1]["preserve_record_layout"] is False

    # Opting out entirely, and the empty selection that has no layout to keep.
    vepyr.annotate(
        INPUT_VCF, cache_dir, preserve_record_layout=False, show_progress=False
    )
    assert seen[-1]["preserve_record_layout"] is False
    vepyr.annotate(
        INPUT_VCF, cache_dir, info_fields=[], format_fields=[], show_progress=False
    )
    assert seen[-1]["preserve_record_layout"] is False


def test_a_shadow_rename_onto_a_taken_name_is_a_clear_error():
    from vepyr import _rename_shadowed_input_columns

    schema = {
        "chrom": pl.String,
        "CADD_PHRED": pl.Float32,
        "INFO_CADD_PHRED": pl.String,
    }
    carried = {
        "CADD_PHRED": ("INFO", "CADD_PHRED"),
        "INFO_CADD_PHRED": ("INFO", "INFO_CADD_PHRED"),
    }
    with pytest.raises(ValueError, match="INFO_CADD_PHRED"):
        _rename_shadowed_input_columns(schema, carried, ["CADD_PHRED"])


def test_a_plugin_named_like_an_already_renamed_input_column_is_an_error():
    from vepyr import _rename_shadowed_input_columns

    schema = {"chrom": pl.String, "INFO_AF": pl.Float32}
    carried = {"INFO_AF": ("INFO", "AF")}
    with pytest.raises(ValueError, match="INFO_AF"):
        _rename_shadowed_input_columns(schema, carried, ["INFO_AF"])
