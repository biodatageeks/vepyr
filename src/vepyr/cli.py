"""Command-line interface for vepyr.

A thin VCF-in / VCF-out shell over :func:`vepyr.annotate`. Flag names follow
Ensembl VEP's own spelling so that ``ext.args`` strings written for the
``ensemblvep/vep`` nf-core module carry over unchanged.

Annotation always runs ``--everything``, the configuration the golden parity
suite validates, so ``--fasta`` is required and ``--everything`` / ``--hgvsc``
are accepted only so existing argument strings keep working. Every other
:func:`vepyr.annotate` parameter that applies to VCF output has a flag; only
the ``LazyFrame``-side parameters (``skip_csq``, ``info_fields``,
``format_fields``) and the ``on_batch_written`` callback stay on the Python API.
"""

from __future__ import annotations

import argparse
import sys
from importlib.metadata import version as _package_version

_EPILOG = """\
examples:
  vepyr annotate -i in.vcf.gz -o out.vcf.gz --dir_cache CACHE \\
      --fasta GRCh38.fa --fork 8
"""


# VEP's consequence-selection flags, each a boolean annotate() keyword of the
# same name.
_PICK_FLAGS = (
    ("--pick", "One selected consequence per variant."),
    ("--pick_allele", "One selected consequence per allele."),
    ("--per_gene", "One selected consequence per gene."),
    ("--pick_allele_gene", "One selected consequence per allele and gene."),
    ("--flag_pick", "Keep all consequences; flag one per variant with PICK=1."),
    ("--flag_pick_allele", "As --flag_pick, one per allele."),
    ("--flag_pick_allele_gene", "As --flag_pick, one per allele and gene."),
)

# --compress_output takes VEP's names; annotate() calls block-gzip "bgzf".
_COMPRESSION = {"bgzip": "bgzf", "gzip": "gzip"}


def _distance(value: str) -> int | tuple[int, int]:
    """Parse VEP's ``--distance N`` or ``--distance N,M``."""
    parts = value.split(",")
    try:
        numbers = [int(part) for part in parts]
    except ValueError:
        numbers = []
    if len(numbers) not in (1, 2) or any(n < 0 for n in numbers):
        raise argparse.ArgumentTypeError(
            f"expected N or N,M with non-negative integers, got {value!r}"
        )
    return numbers[0] if len(numbers) == 1 else (numbers[0], numbers[1])


def _fields(value: str) -> str | list[str]:
    """Parse VEP's comma-separated ``--fields``; ``core`` is passed through."""
    if value == "core":
        return value
    names = [name.strip() for name in value.split(",")]
    if not all(names):
        raise argparse.ArgumentTypeError(f"empty field name in {value!r}")
    return names


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level ``vepyr`` parser."""
    # allow_abbrev=False here too: `vepyr --ver annotate ...` would otherwise
    # expand to --version, print it and exit 0 without annotating.
    parser = argparse.ArgumentParser(
        prog="vepyr",
        description="Rust-powered Ensembl VEP variant annotation.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"vepyr {_package_version('vepyr')}",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    # allow_abbrev=False: argparse would otherwise expand an unambiguous prefix,
    # so an ensemblvep ext.args carrying --hgvs (HGVSc and HGVSp) would silently
    # run as --hgvsc instead of failing like every other unimplemented flag.
    annotate = subcommands.add_parser(
        "annotate",
        help="Annotate a VCF against a vepyr Parquet cache.",
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )

    required = annotate.add_argument_group("required arguments")
    required.add_argument(
        "-i",
        "--input_file",
        required=True,
        metavar="FILE",
        help="Input VCF (plain, gzip or bgzip).",
    )
    required.add_argument(
        "-o",
        "--output_file",
        required=True,
        metavar="FILE",
        help="Output VCF. A .gz/.bgz suffix selects bgzf compression.",
    )
    required.add_argument(
        "--dir_cache",
        required=True,
        metavar="DIR",
        help="Parquet cache directory, e.g. .../116_GRCh38_ensembl.",
    )
    required.add_argument(
        "--fasta",
        required=True,
        metavar="FILE",
        help="Reference FASTA. Annotation always runs --everything, which needs it.",
    )

    vep = annotate.add_argument_group("Ensembl VEP options")
    # Accepted and ignored: --everything is always on, and it implies --hgvsc.
    # Kept so ensemblvep-style ext.args strings still parse.
    vep.add_argument(
        "--everything",
        action="store_true",
        help="Always on; accepted for Ensembl VEP compatibility.",
    )
    vep.add_argument(
        "--hgvsc",
        action="store_true",
        help="Implied by --everything; accepted for Ensembl VEP compatibility.",
    )
    vep.add_argument(
        "--fork",
        "--workers",
        dest="fork",
        type=int,
        default=1,
        metavar="N",
        help="Annotation pipelines to run. N>1 needs a tabix-indexed input.",
    )
    vep.add_argument(
        "--buffer_size",
        "--buffer-size",
        dest="buffer_size",
        type=int,
        metavar="N",
        help="Variants per annotation buffer. Must be positive (default: 5000).",
    )
    vep.add_argument(
        "--allow_non_variant",
        action="store_true",
        help="Keep ALT=. records instead of dropping them, as VEP does.",
    )
    vep.add_argument(
        "--cache_version",
        type=int,
        metavar="N",
        help="Assert the cache version recorded in the Parquet metadata.",
    )
    vep.add_argument(
        "--failed",
        type=int,
        choices=(0, 1),
        metavar="{0,1}",
        help="Include co-located variants flagged as failed (default: 0).",
    )
    vep.add_argument(
        "--distance",
        type=_distance,
        metavar="N[,M]",
        help="Up- and downstream distance for transcript overlap; N,M sets "
        "upstream and downstream separately (default: 5000).",
    )
    vep.add_argument(
        "--fields",
        type=_fields,
        metavar="LIST",
        help="Comma-separated CSQ fields to output, in this order, or 'core' "
        "for the eleven core fields. Plugin fields follow them.",
    )
    vep.add_argument(
        "--compress_output",
        choices=("bgzip", "gzip"),
        help="Compress the output VCF, overriding the suffix of -o.",
    )

    hgvs = annotate.add_argument_group("Ensembl VEP HGVS options")
    hgvs.add_argument(
        "--shift_hgvs",
        type=int,
        choices=(0, 1),
        metavar="{0,1}",
        help="3' shift HGVS notation (default: 1).",
    )
    hgvs.add_argument(
        "--no_escape",
        action="store_true",
        help="Do not URI-escape HGVS strings.",
    )
    hgvs.add_argument(
        "--remove_hgvsp_version",
        action="store_true",
        help="Remove the version from the HGVSp transcript identifier.",
    )
    hgvs.add_argument(
        "--hgvsp_use_prediction",
        action="store_true",
        help="Use the predicted rather than the observed protein sequence.",
    )

    transcripts = annotate.add_argument_group(
        "Ensembl VEP transcript selection options"
    )
    gencode = transcripts.add_mutually_exclusive_group()
    gencode.add_argument(
        "--gencode_basic",
        action="store_true",
        help="Restrict to transcripts in the GENCODE basic set.",
    )
    gencode.add_argument(
        "--gencode_primary",
        action="store_true",
        help="Restrict to transcripts in the GENCODE primary set (GRCh38).",
    )
    transcripts.add_argument(
        "--all_refseq",
        action="store_true",
        help="Keep all RefSeq transcripts, including CCDS and EST-based ones.",
    )
    transcripts.add_argument(
        "--exclude_predicted",
        action="store_true",
        help="Exclude predicted RefSeq transcripts (XM_, XR_).",
    )
    for flag, text in _PICK_FLAGS:
        transcripts.add_argument(flag, action="store_true", help=text)
    transcripts.add_argument(
        "--pick_order",
        metavar="LIST",
        help="Comma-separated ranking criteria for the pick options, e.g. "
        "biotype,rank,mane_select,tsl,canonical,appris,ccds,length.",
    )

    vepyr_options = annotate.add_argument_group("vepyr options")
    vepyr_options.add_argument(
        "--plugin_cache_root",
        metavar="DIR",
        help="Root of a plugin cache tree holding plugin/<name>/ directories.",
    )
    vepyr_options.add_argument(
        "--plugin",
        action="append",
        dest="plugins",
        metavar="NAME",
        help="Restrict to this plugin. Repeatable; order is CSQ block order.",
    )
    vepyr_options.add_argument(
        "--no_progress",
        action="store_true",
        help="Suppress the progress bar.",
    )
    vepyr_options.add_argument(
        "--no_preserve_record_layout",
        action="store_true",
        help="Write INFO in schema order and drop FORMAT keys missing in every "
        "sample, instead of each record's own layout.",
    )
    return parser


# Store-true flags forwarded to annotate() under the same name when given.
_SWITCHES = (
    "no_escape",
    "remove_hgvsp_version",
    "hgvsp_use_prediction",
    "gencode_basic",
    "gencode_primary",
    "all_refseq",
    "exclude_predicted",
    *(flag.removeprefix("--") for flag, _ in _PICK_FLAGS),
)


def annotate_kwargs(args: argparse.Namespace) -> dict:
    """Translate parsed arguments into :func:`vepyr.annotate` keyword arguments.

    The input VCF and cache directory are positional on the API side and are
    passed separately by :func:`main`.
    """
    kwargs: dict = {
        "output_vcf": args.output_file,
        "show_progress": not args.no_progress,
        "workers": args.fork,
        "reference_fasta": args.fasta,
    }
    if args.allow_non_variant:
        kwargs["allow_non_variant"] = True
    if args.buffer_size is not None:
        kwargs["buffer_size"] = args.buffer_size
    if args.cache_version is not None:
        # annotate() validates this as a string.
        kwargs["expected_cache_version"] = str(args.cache_version)
    if args.plugin_cache_root is not None:
        kwargs["plugin_cache_root"] = args.plugin_cache_root
    if args.plugins is not None:
        kwargs["plugins"] = list(args.plugins)
    if args.failed is not None:
        kwargs["failed"] = args.failed
    if args.distance is not None:
        kwargs["distance"] = args.distance
    if args.fields is not None:
        kwargs["fields"] = args.fields
    if args.compress_output is not None:
        kwargs["compression"] = _COMPRESSION[args.compress_output]
    if args.shift_hgvs is not None:
        kwargs["shift_hgvs"] = bool(args.shift_hgvs)
    if args.pick_order is not None:
        kwargs["pick_order"] = args.pick_order
    if args.no_preserve_record_layout:
        kwargs["preserve_record_layout"] = False
    for name in _SWITCHES:
        if getattr(args, name):
            kwargs[name] = True
    return kwargs


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``vepyr`` console script.

    Returns a process exit code: 0 on success, 2 when the annotation API
    rejects the request.

    ``RuntimeError`` is caught alongside the argument-validation errors
    because the Rust boundary surfaces every engine failure as a
    ``PyRuntimeError`` -- a bad cache version, an unreadable VCF and a
    missing contig all arrive that way, and each is a user error rather than
    a bug worth a traceback.
    """
    args = build_parser().parse_args(argv)

    # Imported here rather than at module scope so `vepyr --help` and
    # `vepyr --version` do not pay for loading the native extension.
    import vepyr

    try:
        vepyr.annotate(args.input_file, args.dir_cache, **annotate_kwargs(args))
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"vepyr: error: {exc}", file=sys.stderr)
        return 2
    return 0
