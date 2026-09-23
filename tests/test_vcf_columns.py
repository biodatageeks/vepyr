"""INFO/FORMAT columns on the LazyFrame, their pruning, and sink_vcf metadata."""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from tests.cache_metadata import copy_cache_with_source_metadata
from vepyr._core import vcf_fields

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

    info, fmt, nests = vcf_fields(INPUT_VCF)
    assert info == GIAB_INFO
    assert fmt == GIAB_FORMAT
    assert nests is False  # the golden input has one sample


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
    monkeypatch.setattr(
        vepyr, "_vcf_fields", lambda path: (["DP", "AF"], ["GT", "DP"], False)
    )
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
        CARRIED_SCHEMA,
        carried,
        "Consequence annotations from Ensembl VEP. Format: Allele|Consequence",
        _fake_extract,
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
        lines, _csq = annotation_header_lines(
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


def test_the_provenance_records_the_colocated_switches(cache_dir):
    """The LazyFrame passes `af` and friends to the engine on collect, so the
    header has to say so. `everything` implies them and records itself, and a
    run that sets none records what it always did."""
    from vepyr._core import annotation_header_lines

    def recorded(**opts):
        lines, _csq = annotation_header_lines(
            INPUT_VCF, cache_dir, json.dumps(opts), ["##fileformat=VCFv4.2"]
        )
        line = next(
            line
            for line in lines
            if line.startswith("##datafusion-bio-function-vep-command-line=")
        )
        return json.loads(line.split("='", 1)[1][:-1])["options"]

    asked = recorded(af=True, pubmed=True)
    assert asked["af"] is True and asked["pubmed"] is True
    assert "check_existing" not in asked  # only what was asked for
    assert recorded(check_existing=True)["check_existing"] is True
    # Unchanged for the two cases every existing header comes from.
    assert recorded(everything=True) == {
        **recorded(),
        "everything": True,
    }
    assert not set(recorded()) & {"af", "pubmed", "check_existing", "max_af"}


def test_the_engine_hands_over_the_csq_layout(cache_dir):
    """The `Format:` list is the engine's, not a list derived here: it follows
    the flags, the cache source type, the pick options and the plugin
    manifests. vepyr used to approximate it from the frame's typed columns,
    which was only right under `everything`."""
    from vepyr._core import annotation_header_lines

    def description(**opts):
        _lines, csq = annotation_header_lines(
            INPUT_VCF, cache_dir, json.dumps(opts), ["##fileformat=VCFv4.2"]
        )
        return csq

    full = description(everything=True)
    partial = description(hgvs=True)
    assert full.startswith("Consequence annotations from Ensembl VEP. Format: ")
    # The two layouts differ, which is exactly what a caller cannot derive.
    assert partial != full
    assert partial.count("|") < full.count("|")


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
# `hgvs` alone was a strict xfail while the `Format:` list was derived from the
# frame's typed columns, which is only the engine's layout under `everything`.
# The engine hands the description over now, so a partial flag set declares
# exactly what it writes. A flagless frame is still not comparable: it runs
# `everything` when it has a FASTA, and a flagless output_vcf does not.
@pytest.mark.parametrize("flags", [{"everything": True}, {"hgvs": True}])
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
    assert vcf_fields(str(src)) == (["genotypes", "DP"], [], False)

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


ANNOTATED_INPUT = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=chr1>\n"
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations from '
    'Ensembl VEP. Format: Allele|Consequence">\n'
    '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    "chr1\t602113\t.\tT\tTGCCCA\t50\tPASS\tCSQ=STALE|stale_consequence;DP=7\n"
    "chr1\t604358\t.\tG\tC\t50\tPASS\tCSQ=STALE2|also_stale;DP=9\n"
)


def _csq_of(path):
    """INFO/CSQ per data line, in file order."""
    out = []
    for line in Path(path).read_text().splitlines():
        if line.startswith("#"):
            continue
        info = line.split("\t")[7]
        out.append(next((f[4:] for f in info.split(";") if f.startswith("CSQ=")), None))
    return out


def test_this_runs_csq_replaces_the_inputs_own(cache_dir, tmp_path):
    """Re-annotating an annotated VCF must write the new consequences. The
    engine renames the input's CSQ to `INFO_CSQ`, and polars-bio maps a column
    to a VCF id by that prefix, so a carried one is written as CSQ and shadows
    the annotation. Dropping its header entry is not enough."""
    import vepyr

    pb = pytest.importorskip("polars_bio")
    src = tmp_path / "annotated.vcf"
    src.write_text(ANNOTATED_INPUT)
    kwargs = dict(reference_fasta=REFERENCE_FASTA, show_progress=False, everything=True)

    reference = tmp_path / "reference.vcf"
    vepyr.annotate(
        str(src), cache_dir, output_vcf=str(reference), compression="plain", **kwargs
    )

    lf = vepyr.annotate(str(src), cache_dir, skip_csq=False, **kwargs)
    # The stale column is gone from the frame, not merely from the header.
    assert [n for n in lf.collect_schema().names() if "CSQ" in n.upper()] == ["CSQ"]
    sunk = tmp_path / "sunk.vcf"
    pb.sink_vcf(lf, str(sunk))

    got = _csq_of(sunk)
    assert got == _csq_of(reference)  # exactly what output_vcf writes
    assert all(value and not value.startswith("STALE") for value in got)
    # One CSQ definition, this run's, and no leftover for the column that went:
    # the stale field's id mapping has to survive for the header to exclude it.
    assert len(_header_lines(sunk, "##INFO=<ID=CSQ,")) == 1
    assert not _header_lines(sunk, "##INFO=<ID=INFO_CSQ")


def test_dropping_the_stale_csq_does_not_read_as_a_user_projection(cache_dir, tmp_path):
    """`fields=` fixes the layout, so projecting the frame on top of it is
    refused. vepyr's own drop of the input's stale CSQ is not such a projection
    and must not be counted as one: a plain collect() makes no selection."""
    import vepyr

    src = tmp_path / "annotated.vcf"
    src.write_text(ANNOTATED_INPUT)
    kwargs = dict(
        reference_fasta=REFERENCE_FASTA,
        show_progress=False,
        everything=True,
        skip_csq=False,
        fields=["Consequence", "IMPACT"],
    )

    lf = vepyr.annotate(str(src), cache_dir, **kwargs)
    assert lf.collect().height == 2
    # A projection the caller really did make is still refused.
    with pytest.raises(Exception, match="fields="):
        vepyr.annotate(str(src), cache_dir, **kwargs).select("chrom").collect()


def test_without_a_csq_of_its_own_the_inputs_csq_is_written_back(cache_dir, tmp_path):
    """`skip_csq=True` generates no CSQ, so there is nothing to replace the
    input's with. Dropping its declaration would write the column under no
    definition, losing the annotation the file came with."""
    import vepyr

    pb = pytest.importorskip("polars_bio")
    src = tmp_path / "annotated.vcf"
    src.write_text(ANNOTATED_INPUT)

    lf = vepyr.annotate(
        str(src),
        cache_dir,
        reference_fasta=REFERENCE_FASTA,
        show_progress=False,
        everything=True,
    )
    sunk = tmp_path / "passthrough.vcf"
    pb.sink_vcf(lf, str(sunk))

    assert _csq_of(sunk) == ["STALE|stale_consequence", "STALE2|also_stale"]
    assert _header_lines(sunk, "##INFO=<ID=CSQ")  # still declared


def test_an_excluded_input_field_does_not_lend_its_id_to_an_annotation_column(
    cache_dir, tmp_path
):
    """An input may declare a field an annotation column is also named for, and
    then not carry it. Its id must not become a writer target: the engine's own
    `AF` would be written under a header line describing the input's cohort
    frequency."""
    import vepyr

    pb = pytest.importorskip("polars_bio")
    src = tmp_path / "hasaf.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        '##INFO=<ID=AF,Number=A,Type=Float,Description="Cohort frequency, this study">\n'
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t604358\t.\tG\tC\t50\tPASS\tAF=0.123;DP=9\n"
    )
    lf = vepyr.annotate(
        str(src),
        cache_dir,
        skip_csq=False,
        info_fields=[],
        format_fields=[],
        reference_fasta=REFERENCE_FASTA,
        show_progress=False,
        everything=True,
    )
    assert "AF" in lf.collect_schema().names()  # the engine's, not the input's
    sunk = tmp_path / "sunk.vcf"
    pb.sink_vcf(lf, str(sunk))

    record = next(
        line for line in sunk.read_text().splitlines() if not line.startswith("#")
    )
    keys = [field.split("=")[0] for field in record.split("\t")[7].split(";")]
    assert keys == ["CSQ"]  # no AF carrying VEP's value under the input's id


def test_an_info_field_named_like_a_core_column_is_not_carried(cache_dir, tmp_path):
    """A VCF may declare an INFO field called `id` or `filter`. Carrying it
    would put it beside the reader's own column of that name and the query
    fails on the duplicate, so the default carry gives way for it and the file
    still annotates. Asking for it by name is an error."""
    import vepyr

    src = tmp_path / "coreid.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "##contig=<ID=chr1>\n"
        '##INFO=<ID=id,Number=1,Type=String,Description="An INFO field named id">\n'
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "chr1\t604358\t.\tG\tC\t50\tPASS\tid=abc;DP=9\n"
    )
    with pytest.warns(UserWarning, match="named like"):
        frame = vepyr.annotate(str(src), cache_dir, show_progress=False).collect()
    assert frame.height == 1
    assert frame["DP"].to_list() == [9]  # everything else is still carried
    assert frame["id"].to_list() == [""]  # the reader's column, not the field

    with pytest.raises(ValueError, match="cannot carry"):
        vepyr.annotate(str(src), cache_dir, info_fields=["id"], show_progress=False)


@pytest.mark.parametrize(
    "kind,ident",
    [
        # An annotation column's name: the engine renames the input's field.
        ("INFO", "Consequence"),
        ("INFO", "AF"),
        ("INFO", "most_severe_consequence"),
        # Cache-only annotation columns, which have no CSQ sub-field.
        ("INFO", "dbsnp_ids"),
        ("INFO", "clin_sig_allele"),
        # This run replaces the input's CSQ.
        ("INFO", "CSQ"),
        # The nested FORMAT container's name, and the record-layout columns.
        ("INFO", "genotypes"),
        ("INFO", "_vcf_info_keys"),
        # The reader's own columns: not carried at all.
        ("INFO", "start"),
        ("INFO", "filter"),
        ("INFO", "qual"),
        # FORMAT ids collide against the same names.
        ("FORMAT", "AF"),
        ("FORMAT", "CSQ"),
        ("FORMAT", "genotypes"),
        ("FORMAT", "Consequence"),
        ("FORMAT", "start"),
    ],
)
@pytest.mark.parametrize("samples", [1, 2], ids=["one_sample", "two_samples"])
def test_an_input_field_may_be_named_like_any_column_the_frame_has(
    cache_dir, tmp_path, kind, ident, samples
):
    """Every way an input's own field id can collide with a frame column.

    Each has its own resolution -- rename, replace, give way -- and the point
    here is only that none of them stops the file being annotated. Built as a
    matrix because the individual cases arrived one review round at a time.

    Crossed with the sample count because the frame's own columns depend on it:
    more than one sample puts the FORMAT fields in a nested `genotypes` struct,
    and a single-sample file has no such column. A one-sample-only matrix
    passed `INFO/genotypes` for the wrong reason.
    """
    import vepyr

    names = [f"S{n + 1}" for n in range(samples)]
    header = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"
    src = tmp_path / f"{kind}_{ident}_{samples}.vcf"
    if kind == "INFO":
        columns = ("\tFORMAT\t" + "\t".join(names)) if names else ""
        calls = ("\tGT\t" + "\t".join("0/1" for _ in names)) if names else ""
        src.write_text(
            "##fileformat=VCFv4.2\n##contig=<ID=chr1>\n"
            f'##INFO=<ID={ident},Number=1,Type=String,Description="x">\n'
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
            f"{header}{columns}\n"
            f"chr1\t604358\t.\tG\tC\t50\tPASS\t{ident}=1;DP=9{calls}\n"
        )
    else:
        calls = "\t".join("0/1:1" for _ in names)
        src.write_text(
            "##fileformat=VCFv4.2\n##contig=<ID=chr1>\n"
            f'##FORMAT=<ID={ident},Number=1,Type=String,Description="x">\n'
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
            f"{header}\tFORMAT\t" + "\t".join(names) + "\n"
            f"chr1\t604358\t.\tG\tC\t50\tPASS\t.\tGT:{ident}\t{calls}\n"
        )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # a field that cannot be carried says so
        frame = vepyr.annotate(
            str(src),
            cache_dir,
            skip_csq=False,
            reference_fasta=REFERENCE_FASTA,
            show_progress=False,
            everything=True,
        ).collect()
    assert frame.height == 1


def test_a_multi_sample_input_reserves_genotypes_for_its_samples(cache_dir, tmp_path):
    """With more than one sample the FORMAT fields arrive in one nested
    `genotypes` struct, so an INFO field of that name has nowhere to go and is
    not carried. With one sample there is no struct and it stays data -- the
    two cases differ, which is why the name alone cannot decide."""
    import vepyr

    def vcf(path, samples):
        columns = "\t".join(samples)
        calls = "\t".join("0/1" for _ in samples)
        path.write_text(
            "##fileformat=VCFv4.2\n##contig=<ID=chr1>\n"
            '##INFO=<ID=genotypes,Number=1,Type=String,Description="x">\n'
            '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
            '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
            f"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t{columns}\n"
            f"chr1\t604358\t.\tG\tC\t50\tPASS\tgenotypes=abc;DP=9\tGT\t{calls}\n"
        )
        return str(path)

    many = vcf(tmp_path / "two.vcf", ["S1", "S2"])
    assert vcf_fields(many)[2] is True  # the reader nests them
    with pytest.warns(UserWarning, match="genotypes"):
        frame = vepyr.annotate(many, cache_dir, show_progress=False).collect()
    assert frame["DP"].to_list() == [9]  # the rest of the header still carried
    assert frame.schema["genotypes"] == pl.Struct({"GT": pl.List(pl.String)})

    one = vcf(tmp_path / "one.vcf", ["S1"])
    assert vcf_fields(one)[2] is False
    frame = vepyr.annotate(one, cache_dir, show_progress=False).collect()
    assert frame["genotypes"].to_list() == ["abc"]  # the input's field, as data


def test_a_format_selection_alone_still_knows_about_the_nesting(cache_dir, tmp_path):
    """The nesting is read once, whichever selection asked for the header. A
    FORMAT-only selection takes the other branch, and the reserved set has to
    be decided there too."""
    import vepyr

    src = tmp_path / "two.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n##contig=<ID=chr1>\n"
        '##INFO=<ID=genotypes,Number=1,Type=String,Description="x">\n'
        '##INFO=<ID=DP,Number=1,Type=Integer,Description="d">\n'
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="gt">\n'
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tS1\tS2\n"
        "chr1\t604358\t.\tG\tC\t50\tPASS\tgenotypes=abc;DP=9\tGT\t0/1\t1/1\n"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert (
            vepyr.annotate(
                str(src), cache_dir, format_fields=["GT"], show_progress=False
            )
            .collect()
            .height
            == 1
        )
        assert (
            vepyr.annotate(str(src), cache_dir, format_fields=[], show_progress=False)
            .collect()
            .height
            == 1
        )

    # Explicitly asking for the INFO field while the container is there is the
    # error; with no FORMAT field selected there is no container and it is not.
    with pytest.raises(ValueError, match="cannot carry"):
        vepyr.annotate(
            str(src), cache_dir, info_fields=["genotypes"], show_progress=False
        )
    frame = vepyr.annotate(
        str(src),
        cache_dir,
        info_fields=["genotypes"],
        format_fields=[],
        show_progress=False,
    ).collect()
    assert frame["genotypes"].to_list() == ["abc"]


@pytest.mark.parametrize(
    "selection",
    [
        {},
        {"info_fields": ["DPSum"]},
        {"info_fields": []},
        {"format_fields": ["GT"]},
        {"format_fields": []},
        {"info_fields": [], "format_fields": ["GT"]},
        {"info_fields": ["DPSum"], "format_fields": []},
        {"info_fields": [], "format_fields": []},
        {"info_fields": ["DPSum"], "format_fields": ["GT"]},
    ],
    ids=lambda s: "_".join(f"{k}={v}" for k, v in sorted(s.items())) or "neither",
)
def test_every_shape_of_selection_reaches_a_frame(cache_dir, selection):
    """Each of `info_fields` and `format_fields` is absent, empty or a list,
    and every pairing has to annotate.

    A selection naming only FORMAT fields took a branch nothing exercised
    through `annotate()` -- the neighbouring tests drive `create_annotator`
    directly -- and reached the carry guard with its state unset.
    """
    import vepyr

    frame = vepyr.annotate(
        INPUT_VCF, cache_dir, show_progress=False, **selection
    ).collect()
    assert frame.height == 100


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


def test_a_rename_target_that_is_another_plugins_field_is_an_error():
    """The plugin columns are added after the shadow rename, so a plugin field
    named like a rename target would overwrite the column the rename just made
    and the input's values would go without a word."""
    from vepyr import _rename_shadowed_input_columns

    schema = {"chrom": pl.String, "X": pl.Float32}
    carried = {"X": ("INFO", "X")}
    with pytest.raises(ValueError, match="INFO_X"):
        _rename_shadowed_input_columns(schema, carried, ["X", "INFO_X"])


def test_building_the_frame_survives_flags_a_sink_could_not_get(cache_dir, tmp_path):
    """polars-bio wants the header lines when the metadata is attached, so the
    provenance is built while the frame is. Deriving the flags a sink would need
    can raise -- a plugin match template needing a FASTA there is none of -- and
    that must not decide whether annotate() succeeds: a caller who projects the
    plugin columns away has a working query, and had one before polars-bio was
    installed."""
    import vepyr

    pytest.importorskip("polars_bio")  # without it no provenance is built at all
    from tests.test_build_plugin_cache import _init_full_repo

    repo = _init_full_repo(tmp_path)
    source = tmp_path / "demo.tsv"
    source.write_text("1\t604358\tG\tC\t0.5\n")
    plugin_root = tmp_path / "pc"
    vepyr.build_plugin_cache(
        "demo",
        "v0.1.0",
        source_path=str(source),
        cache_dir=cache_dir,
        plugin_cache_root=str(plugin_root),
        plugins_repo=str(repo),
        chroms=["1"],
    )
    # Keyed on HGVSc, which needs the reference_fasta this run does not pass.
    manifest = plugin_root / "plugin" / "demo" / "manifest.json"
    spec = json.loads(manifest.read_text())
    spec["match_columns"] = [{"column": "hgvsc", "template": "{HGVSc}"}]
    manifest.write_text(json.dumps(spec))

    lf = vepyr.annotate(
        INPUT_VCF,
        cache_dir,
        plugin_cache_root=str(plugin_root),
        plugins=["demo"],
        show_progress=False,
    )
    assert lf.select("chrom").collect().height > 0


def test_a_plugin_named_like_an_already_renamed_input_column_is_an_error():
    from vepyr import _rename_shadowed_input_columns

    schema = {"chrom": pl.String, "INFO_AF": pl.Float32}
    carried = {"INFO_AF": ("INFO", "AF")}
    with pytest.raises(ValueError, match="INFO_AF"):
        _rename_shadowed_input_columns(schema, carried, ["INFO_AF"])
