"""Command-line interface for vepyr.

A thin VCF-in / VCF-out shell over :func:`vepyr.annotate`. Flag names follow
Ensembl VEP's own spelling so that ``ext.args`` strings written for the
``ensemblvep/vep`` nf-core module carry over unchanged.

The flag set is deliberately small: annotation always runs ``--everything``,
the configuration the golden parity suite validates, so ``--fasta`` is
required and ``--everything`` / ``--hgvsc`` are accepted only so existing
argument strings keep working. Everything else stays on the Python API.
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
        "--buffer-size",
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
    return parser


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
