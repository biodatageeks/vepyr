"""Strict fixture loading, shared by the CLI runner and its separate unit tests."""

from __future__ import annotations

import hashlib
import re
import shlex
import tomllib
from dataclasses import dataclass
from pathlib import Path

from fixture_skip import skip_reason
from fixture_sources import is_download_url

from check_test_dir import check_dir
from run_tests.cli_argv import VEPYR_KEYS, UnmappableKey, annotate_argv, effective_runs
from run_tests.verdict import Exit, RunTestsError

ROOT = Path(__file__).resolve().parents[2]
NORMALIZE_COMMAND = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"
TAGGED_TEST = re.compile(
    r"https://github.com/Ensembl/ensembl-vep/blob/release/\d+\.\d+/t/[^/#\s]+\.t(?:#[^/\s]*)?"
)


def body_lines(data: bytes) -> list[bytes]:
    """Split on LF only, retaining terminators exactly as the VCF oracle does."""
    parts = data.split(b"\n")
    lines = [line + b"\n" for line in parts[:-1]]
    if parts[-1]:
        lines.append(parts[-1])
    return [line for line in lines if not line.startswith(b"#")]


def body_md5(data: bytes) -> str:
    return hashlib.md5(b"".join(body_lines(data))).hexdigest()


def _table(value, fields: dict, optional=()) -> None:
    if not isinstance(value, dict):
        raise ValueError("expected a table")
    if extra := value.keys() - fields.keys():
        raise ValueError(f"unknown key: {sorted(extra)[0]}")
    if missing := fields.keys() - value.keys() - set(optional):
        raise ValueError(f"missing required key: {sorted(missing)[0]}")
    for key, item in value.items():
        kind = fields[key]
        if isinstance(kind, list):
            valid = isinstance(item, list) and all(type(v) is kind[0] for v in item)
        else:
            valid = type(item) is kind
        if not valid:
            raise ValueError(f"{key} has the wrong type")
        if isinstance(item, str) and not item.strip():
            raise ValueError(f"{key} must be a non-empty string")


VEPYR_FIELDS = dict.fromkeys(VEPYR_KEYS, bool) | {
    "flavour": str,
    "required_contigs": [str],
    "buffer_size": int,
}


@dataclass(frozen=True)
class Fixture:
    directory: Path
    ids: tuple[str, ...]
    runs: list[dict]
    expected: str
    skip: str | None


def load(directory: Path) -> Fixture:
    """Reject malformed metadata and modified ground truth before annotation."""
    try:
        doc = tomllib.loads((directory / "test.toml").read_text())
        _table(
            doc,
            {
                "name": str,
                "description": str,
                "skip_reason": str,
                "origin": dict,
                "tests": [dict],
                "input": dict,
                "vep": dict,
                "vepyr": dict,
                "compare": dict,
                "vepyr_run": [dict],
            },
            {"skip_reason", "origin", "tests", "vepyr_run"},
        )
        if doc["name"] != directory.name:
            raise ValueError("name does not match the directory name")
        entries = doc.get("tests", [])
        if bool(entries) == ("origin" in doc):
            raise ValueError("needs exactly one of [origin] or non-empty [[tests]]")
        ids = []
        if not entries:
            _table(doc["origin"], {"vep_test": str})
            entries = [
                dict(id=directory.name, description=doc["description"], **doc["origin"])
            ]
        for entry in entries:
            _table(
                entry,
                {"id": str, "description": str, "vep_test": str, "focus": dict},
                {"focus"},
            )
            if not TAGGED_TEST.fullmatch(entry["vep_test"]):
                raise ValueError("vep_test must be a tagged Ensembl VEP test URL")
            ids.append(entry["id"])
            if "focus" in entry:
                focus = entry["focus"]
                _table(
                    focus,
                    {
                        "kind": str,
                        "field": str,
                        "where": dict,
                        "column": int,
                        "key": str,
                        "ordered": bool,
                    },
                    {"field", "where", "column", "key", "ordered"},
                )
                if focus["kind"] not in {
                    "csq",
                    "csq_values",
                    "column",
                    "info",
                    "record_count",
                }:
                    raise ValueError("unknown focus kind")
                if any(type(v) is not str for v in focus.get("where", {}).values()):
                    raise ValueError("focus.where values must be strings")
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate test IDs")
        if directory.name not in ids:
            raise ValueError("no test ID matches the directory name")
        _table(doc["input"], {"command": str, "bcftools_version": str})
        if doc["input"]["command"] != NORMALIZE_COMMAND:
            raise ValueError("input.command is not the normalization command")
        _table(
            doc["vep"],
            dict.fromkeys(
                [
                    "image",
                    "command",
                    "date",
                    "cache_source",
                    "vep_cache",
                    "vep_cache_checksum",
                    "fasta_source",
                    "fasta_checksum",
                ],
                str,
            )
            | {"extra_flags": [str]},
            {"extra_flags"},
        )
        for key in ("cache_source", "vep_cache", "fasta_source"):
            if not is_download_url(doc["vep"][key]):
                raise ValueError(f"vep.{key} must be an HTTP(S) URL")
        _table(doc["compare"], {"body_md5": str})
        expected = doc["compare"]["body_md5"]
        if not re.fullmatch(r"[0-9a-f]{32}", expected):
            raise ValueError("compare.body_md5 must be 32 lowercase hex digits")
        if body_md5((directory / "expected_output.vcf").read_bytes()) != expected:
            raise ValueError(
                "oracle edited: expected_output.vcf body differs from compare.body_md5"
            )
        if failures := check_dir(directory):
            raise ValueError("; ".join(f.detail for f in failures))
        _table(doc["vepyr"], VEPYR_FIELDS, {"buffer_size"})
        for override in doc.get("vepyr_run", []):
            _table(override, VEPYR_FIELDS, VEPYR_FIELDS)
        runs = effective_runs(doc["vepyr"], doc.get("vepyr_run", []))
        mapping = tomllib.loads((ROOT / "tools/vep_flags.toml").read_text())["mapping"]
        flags = shlex.split(doc["vep"]["command"])
        for settings in runs:
            if settings["flavour"] != "merged" or "--merged" not in flags:
                raise ValueError(
                    "cache flavour mismatch: both VEP and vepyr must use merged"
                )
            contigs = settings["required_contigs"]
            if (
                not contigs
                or len(set(contigs)) != len(contigs)
                or any(
                    not re.fullmatch(r"chr(?:[1-9]|1[0-9]|2[0-2]|X|Y|MT)", c)
                    for c in contigs
                )
            ):
                raise ValueError(
                    "required_contigs must list distinct canonical cache contigs"
                )
            for row in mapping:
                if (
                    settings[row["vepyr_key"]] != row["vepyr_value"]
                    or row["vep_flag"] not in flags
                ):
                    raise ValueError(
                        "unsupported mode: VEP and vepyr must use --everything"
                    )
            annotate_argv(
                settings,
                input_vcf=directory / "input.vcf",
                output_vcf=Path("out.vcf"),
                cache_root=Path("cache"),
                fasta=Path("ref.fa"),
            )
        return Fixture(directory, tuple(ids), runs, expected, skip_reason(doc))
    except (OSError, ValueError, KeyError, TypeError, UnmappableKey) as exc:
        raise RunTestsError(Exit.USAGE, f"[{directory.name}] {exc}") from exc


def load_all(directories) -> list[Fixture]:
    fixtures = [load(directory) for directory in directories]
    if not fixtures:
        raise RunTestsError(Exit.USAGE, "no data fixtures found")
    owners = {}
    for fixture in fixtures:
        for id_ in fixture.ids:
            if id_ in owners:
                raise RunTestsError(
                    Exit.USAGE,
                    f"test ID {id_} declared by both {owners[id_]} "
                    f"and {fixture.directory}",
                )
            owners[id_] = fixture.directory
    return fixtures
