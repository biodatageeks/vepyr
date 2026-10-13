"""Command-line interface for vepyr.

``vepyr annotate`` is a thin VCF-in / VCF-out shell over :func:`vepyr.annotate`;
``vepyr build-cache`` and ``vepyr build-plugin-cache`` wrap
:func:`vepyr.build_cache` and :func:`vepyr.build_plugin_cache`. Flag names follow
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
import os
import re
import subprocess
import sys
import warnings
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


def _names(value: str) -> list[str]:
    """Split a comma-separated list, rejecting empty and repeated names."""
    names = [name.strip() for name in value.split(",")]
    if not all(names):
        raise argparse.ArgumentTypeError(f"empty name in {value!r}")
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise argparse.ArgumentTypeError(
            f"{', '.join(repeated)} given more than once in {value!r}"
        )
    return names


def _fields(value: str) -> str | list[str]:
    """Parse VEP's comma-separated ``--fields``; ``core`` is passed through."""
    if value.strip() == "core":
        return "core"
    names = _names(value)
    if "core" in names:
        raise argparse.ArgumentTypeError(
            "'core' selects the eleven core fields and must be given alone"
        )
    return names


def _pick_order(value: str) -> str:
    """Check VEP's comma-separated ``--pick_order``.

    The criterion names are validated by the engine, which knows the
    supported set; only the list syntax is checked here.
    """
    return ",".join(_names(value))


def _positive_int(value: str) -> int:
    """An integer of at least 1."""
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {value!r}")
    return number


def _non_negative_int(value: str) -> int:
    """An integer of at least 0."""
    try:
        number = int(value)
    except ValueError:
        number = -1
    if number < 0:
        raise argparse.ArgumentTypeError(
            f"expected a non-negative integer, got {value!r}"
        )
    return number


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
        type=_pick_order,
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

    _add_build_cache_parser(subcommands)
    _add_build_plugin_cache_parser(subcommands)
    return parser


# build_cache() entity names; the raw translation entity writes both
# translation datasets.
_ENTITIES = ("variation", "transcript", "exon", "translation", "regulatory", "motif")
_CACHE_TYPES = ("ensembl", "merged", "refseq")
_VERIFY_SOURCE_MODES = ("strict", "warn", "skip")


def _add_build_cache_parser(subcommands) -> None:
    build = subcommands.add_parser(
        "build-cache",
        help="Convert an Ensembl VEP cache to a vepyr Parquet cache.",
        description="Download (or read a local copy of) an Ensembl VEP cache "
        "and convert it to vepyr's Parquet format under "
        "DIR/<release>_<assembly>_<cache_type>.",
        allow_abbrev=False,
    )
    required = build.add_argument_group("required arguments")
    required.add_argument(
        "--release", required=True, type=int, metavar="N", help="Ensembl release."
    )
    required.add_argument(
        "--cache_type",
        required=True,
        choices=_CACHE_TYPES,
        help="Ensembl VEP cache type.",
    )
    required.add_argument(
        "--dir_cache",
        required=True,
        metavar="DIR",
        help="Root directory for the downloaded and the converted cache.",
    )
    options = build.add_argument_group("options")
    options.add_argument(
        "--local_cache",
        metavar="DIR",
        help="Already unpacked Ensembl VEP cache (the directory holding "
        "info.txt); skips the download.",
    )
    options.add_argument(
        "--entity",
        choices=_ENTITIES,
        help="Build only this entity, leaving the rest of the output untouched.",
    )
    options.add_argument(
        "--chrom",
        action="append",
        dest="chroms",
        metavar="NAME",
        help="Build only this contig. Repeatable.",
    )
    options.add_argument(
        "--species", metavar="NAME", help="Species (default: homo_sapiens)."
    )
    options.add_argument(
        "--assembly", metavar="NAME", help="Genome assembly (default: GRCh38)."
    )
    options.add_argument(
        "--partitions",
        type=_positive_int,
        metavar="N",
        help="DataFusion partitions for the conversion (default: 8).",
    )
    options.add_argument(
        "--download_retries",
        type=_non_negative_int,
        metavar="N",
        help="Resume-retries for the cache download (default: 10).",
    )
    options.add_argument(
        "--overwrite",
        action="store_true",
        help="Rebuild existing outputs instead of skipping them.",
    )
    options.add_argument(
        "--no_progress", action="store_true", help="Suppress progress bars."
    )


def _add_build_plugin_cache_parser(subcommands) -> None:
    build = subcommands.add_parser(
        "build-plugin-cache",
        help="Build a plugin cache from a vepyr-plugins manifest.",
        description="Build a per-chromosome plugin cache under "
        "PLUGIN_CACHE_ROOT/plugin/<plugin>/ from the source files named by "
        "the plugin's "
        "manifest in biodatageeks/vepyr-plugins.",
        allow_abbrev=False,
    )
    required = build.add_argument_group("required arguments")
    required.add_argument(
        "--plugin", required=True, metavar="NAME", help="Plugin name, e.g. cadd."
    )
    required.add_argument(
        "--version",
        required=True,
        metavar="TAG",
        help="vepyr-plugins git tag of the manifest, e.g. v0.2.0.",
    )
    required.add_argument(
        "--source",
        required=True,
        action="append",
        dest="sources",
        metavar="[PART=]PATH",
        help="Source file. A manifest with several sources takes one "
        "PART=PATH per source, e.g. snv=... and indel=... for CADD.",
    )
    required.add_argument(
        "--dir_cache",
        required=True,
        metavar="DIR",
        help="vepyr Parquet cache whose variation data sets the tiering, "
        "e.g. .../116_GRCh38_merged.",
    )
    required.add_argument(
        "--plugin_cache_root",
        required=True,
        metavar="DIR",
        help="Root of the plugin cache tree; the cache is written to "
        "DIR/plugin/<plugin>/.",
    )
    options = build.add_argument_group("options")
    options.add_argument(
        "--chrom",
        action="append",
        dest="chroms",
        metavar="NAME",
        help="Build only this contig. Repeatable.",
    )
    options.add_argument(
        "--plugins_repo",
        metavar="PATH",
        help="Local clone of vepyr-plugins to read the manifest from (offline).",
    )
    options.add_argument(
        "--verify_source",
        choices=_VERIFY_SOURCE_MODES,
        help="Checksum check of the source files (default: strict).",
    )
    options.add_argument(
        "--overwrite",
        action="store_true",
        help="Rebuild an existing plugin cache.",
    )


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


def build_cache_kwargs(args: argparse.Namespace) -> dict:
    """Translate ``build-cache`` arguments into :func:`vepyr.build_cache` keywords.

    ``release`` and ``dir_cache`` are positional on the API side and are passed
    separately by :func:`main`. Unset options keep the API defaults.
    """
    kwargs: dict = {
        "cache_type": args.cache_type,
        "show_progress": not args.no_progress,
    }
    for name in (
        "local_cache",
        "entity",
        "chroms",
        "species",
        "assembly",
        "partitions",
        "download_retries",
    ):
        value = getattr(args, name)
        if value is not None:
            kwargs[name] = value
    if args.overwrite:
        kwargs["overwrite"] = True
    return kwargs


# A manifest part name: what may precede "=" in --source PART=PATH. Anything
# else is a bare path, so a path such as /data/batch=1/x.tsv.gz stays whole.
_PART_NAME = re.compile(r"[A-Za-z0-9_-]+")


def _split_source(source: str) -> tuple[str, str, str]:
    """``(part, "=", path)`` for ``PART=PATH``, else ``("", "", source)``."""
    name, sep, path = source.partition("=")
    if sep and (not name or _PART_NAME.fullmatch(name)):
        return name, sep, path
    return "", "", source


def _source_path(sources: list[str]) -> str | dict[str, str]:
    """One bare path, or one ``PART=PATH`` per source of a multi-source manifest.

    A single ``--source`` is always a path, whatever it contains: a
    multi-source manifest needs at least two parts.
    """
    if len(sources) == 1:
        return sources[0]
    parts = [_split_source(source) for source in sources]
    if all(sep == "" for _, sep, _ in parts):
        raise ValueError("--source: give one bare path, or PART=PATH for every source")
    if any(sep == "" for _, sep, _ in parts):
        raise ValueError("--source: do not mix a bare path with PART=PATH sources")
    mapping: dict[str, str] = {}
    for name, _, path in parts:
        if not name or not path:
            raise ValueError("--source: expected PART=PATH with both parts non-empty")
        if name in mapping:
            raise ValueError(f"--source: part {name!r} given more than once")
        mapping[name] = path
    return mapping


def build_plugin_cache_kwargs(args: argparse.Namespace) -> dict:
    """Translate ``build-plugin-cache`` arguments into keywords.

    ``plugin`` and ``version`` are positional on the API side. Raises
    ``ValueError`` for a malformed ``--source`` list.
    """
    kwargs: dict = {
        "source_path": _source_path(args.sources),
        "cache_dir": args.dir_cache,
        "plugin_cache_root": args.plugin_cache_root,
    }
    for name in ("chroms", "plugins_repo", "verify_source"):
        value = getattr(args, name)
        if value is not None:
            kwargs[name] = value
    if args.overwrite:
        kwargs["overwrite"] = True
    return kwargs


def _print_cache_summary(
    args: argparse.Namespace, files: list[tuple[str, int]]
) -> None:
    # Group the written files by entity directory: .../<entity>/chrN.parquet.
    totals: dict[str, list[int]] = {}
    for path, rows in files:
        entity = os.path.basename(os.path.dirname(path)).removesuffix(".parquet")
        entry = totals.setdefault(entity, [0, 0])
        entry[0] += 1
        entry[1] += rows
    for entity, (count, rows) in totals.items():
        print(f"{entity}: {count} files, {rows:,} rows")
    if not files:
        print("No files written: existing outputs kept (use --overwrite to rebuild).")
    from vepyr import _converted_cache_dir

    # Unset --assembly keeps the API default, as in build_cache_kwargs().
    assembly = {"assembly": args.assembly} if args.assembly else {}
    cache = _converted_cache_dir(
        args.dir_cache, args.release, args.cache_type, **assembly
    )
    print(f"Cache ready: {cache}")
    print(f"Next: vepyr annotate --dir_cache {cache} ...")


def _print_plugin_summary(
    args: argparse.Namespace, chroms: list[tuple[str, int, int, int]]
) -> None:
    if not chroms:
        print(
            "No files written: existing plugin shards kept (use --overwrite to rebuild)."
        )
    for chrom, rows, warm, cold in chroms:
        print(f"{chrom}: {rows:,} rows ({warm:,} warm, {cold:,} cold)")
    target = os.path.join(args.plugin_cache_root, "plugin", args.plugin)
    print(f"Plugin cache ready: {target}")
    print(
        "Next: vepyr annotate ... "
        f"--plugin_cache_root {args.plugin_cache_root} --plugin {args.plugin}"
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``vepyr`` console script.

    Returns a process exit code: 0 on success, 2 when the arguments or the
    request are rejected (bad flags, a malformed ``--source`` list, or an
    error from the annotation or cache-building API).

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
        if args.command == "build-cache":
            files = vepyr.build_cache(
                args.release, args.dir_cache, **build_cache_kwargs(args)
            )
            _print_cache_summary(args, files)
        elif args.command == "build-plugin-cache":
            kwargs = build_plugin_cache_kwargs(args)
            with warnings.catch_warnings():
                warnings.simplefilter("always", RuntimeWarning)
                showwarning = warnings.showwarning

                def format_cli_warning(
                    message,
                    category,
                    filename,
                    lineno,
                    file=None,
                    line=None,
                ):
                    if issubclass(category, RuntimeWarning):
                        text = " ".join(str(message).splitlines())
                        print(f"vepyr: warning: {text}", file=sys.stderr)
                    else:
                        showwarning(message, category, filename, lineno, file, line)

                warnings.showwarning = format_cli_warning
                chroms = vepyr.build_plugin_cache(args.plugin, args.version, **kwargs)
            _print_plugin_summary(args, chroms)
        else:
            vepyr.annotate(args.input_file, args.dir_cache, **annotate_kwargs(args))
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(f"vepyr: error: {exc}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        # build_plugin_cache() resolves the manifest with git: an unknown
        # --version, a bad --plugins_repo or a failed clone end up here.
        command = " ".join(str(arg) for arg in exc.cmd)
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        # git may report over several lines; keep the error to one.
        detail = " ".join(line.strip() for line in stderr.splitlines() if line.strip())
        message = f"{command} failed with exit status {exc.returncode}"
        if args.command == "build-plugin-cache":
            message = (
                f"could not resolve the {args.plugin} manifest at "
                f"--version {args.version}: {message}"
            )
        print(
            f"vepyr: error: {message}{': ' + detail if detail else ''}", file=sys.stderr
        )
        return 2
    return 0
