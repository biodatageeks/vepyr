"""Run the named data tests through the installed vepyr CLI."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from run_tests.cli_argv import annotate_argv
from run_tests.fixtures import Fixture, body_lines, body_md5
from run_tests.install import CliBuild
from run_tests.verdict import Exit, RunTestsError


@dataclass(slots=True)
class Report:
    """What :func:`run` saw, in named-test order."""

    passed: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    mismatched: list[str] = field(default_factory=list)
    errors: list[tuple[str, RunTestsError]] = field(default_factory=list)
    notes: dict[str, tuple[str, str]] = field(default_factory=dict)
    """Test id -> (fixture directory name, one-line detail) for every non-pass."""

    @property
    def code(self) -> Exit:
        """The exit code: first non-mismatch error, else MISMATCH, else OK."""
        if self.errors:
            return self.errors[0][1].code
        return Exit.MISMATCH if self.mismatched else Exit.OK

    @property
    def detail(self) -> str:
        """One-line summary detail."""
        return (
            f"data suite: {len(self.passed)} pass, {len(self.mismatched)} mismatch, "
            f"{len(self.errors)} error, {len(self.skipped)} skipped"
        )


def _progress(total: int, report: Report, status: str, out: TextIO) -> None:
    """A line-oriented bar of completed named tests."""
    passed = len(report.passed)
    failed = len(report.mismatched) + len(report.errors)
    skipped = len(report.skipped)
    done = passed + failed + skipped
    filled = done * 20 // total if total else 0
    if status not in {"START", "DONE"}:
        print(status, file=out, flush=True)
        status = ""
    suffix = f" | {status}" if status else ""
    print(
        f"[{'#' * filled}{'-' * (20 - filled)}] {done}/{total} tests | "
        f"{passed} passed, {failed} failed, {skipped} skipped{suffix}",
        file=out,
        flush=True,
    )


def run(
    fixtures: list[Fixture],
    *,
    build: CliBuild,
    cache_root: Path,
    fasta: Path,
    runner=None,
    keep_failed: Path | None = None,
    out=None,
    err=None,
) -> Report:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    runner = runner or (lambda argv: subprocess.run(argv, check=False).returncode)
    total = sum(len(f.ids) for f in fixtures)
    report = Report()
    _progress(total, report, "START", out)
    for fixture in fixtures:
        if fixture.skip:
            for id_ in fixture.ids:
                report.skipped.append((id_, fixture.skip))
                report.notes[id_] = (fixture.directory.name, fixture.skip)
                _progress(total, report, f"SKIP {id_}: {fixture.skip}", out)
            continue
        for id_ in fixture.ids:
            print(f"RUN {id_}", file=out, flush=True)
        mismatch = None
        first_diff = ""
        try:
            with tempfile.TemporaryDirectory(prefix="vepyr-data-") as scratch:
                for index, settings in enumerate(fixture.runs, 1):
                    if len(fixture.runs) > 1:
                        for id_ in fixture.ids:
                            print(
                                f"RUN {id_} | run {index}/{len(fixture.runs)}",
                                file=out,
                                flush=True,
                            )
                    output = Path(scratch) / f"run{index}.vcf"
                    argv = [
                        str(build.python),
                        "-m",
                        "vepyr",
                        *annotate_argv(
                            settings,
                            input_vcf=fixture.directory / "input.vcf",
                            output_vcf=output,
                            cache_root=cache_root,
                            fasta=fasta,
                        ),
                    ]
                    if code := runner(argv):
                        raise RunTestsError(
                            Exit.ENGINE, f"vepyr annotate exited {code} (run {index})"
                        )
                    actual_bytes = output.read_bytes()
                    actual = body_md5(actual_bytes)
                    if actual != fixture.expected and mismatch is None:
                        mismatch = actual
                        if keep_failed is not None:
                            kept = keep_failed / fixture.directory.name
                            kept.mkdir(parents=True, exist_ok=True)
                            shutil.copyfile(output, kept / output.name)
                        expected_lines = body_lines(
                            (fixture.directory / "expected_output.vcf").read_bytes()
                        )
                        actual_lines = body_lines(actual_bytes)
                        for row in range(max(len(expected_lines), len(actual_lines))):
                            want = (
                                expected_lines[row]
                                if row < len(expected_lines)
                                else b"<missing>"
                            )
                            got = (
                                actual_lines[row]
                                if row < len(actual_lines)
                                else b"<missing>"
                            )
                            if want != got:
                                first_diff = f"run {index}, record {row + 1}"
                                print(
                                    f"[{fixture.directory.name}] run {index}, "
                                    f"first differing record {row + 1}",
                                    file=err,
                                )
                                print(
                                    "VEP: " + want.decode(errors="replace").rstrip(),
                                    file=err,
                                )
                                print(
                                    "vepyr: " + got.decode(errors="replace").rstrip(),
                                    file=err,
                                )
                                break
        except (OSError, RunTestsError) as exc:
            if not isinstance(exc, RunTestsError):
                exc = RunTestsError(Exit.ENGINE, str(exc))
            print(f"[{fixture.directory.name}] {exc}", file=err, flush=True)
            for id_ in fixture.ids:
                report.errors.append((id_, exc))
                report.notes[id_] = (fixture.directory.name, str(exc))
                _progress(total, report, f"ERROR {id_}", out)
            continue
        for id_ in fixture.ids:
            if mismatch is None:
                report.passed.append(id_)
                _progress(total, report, f"PASS {id_}", out)
            else:
                report.mismatched.append(id_)
                report.notes[id_] = (
                    fixture.directory.name,
                    f"expected {fixture.expected}, got {mismatch}; {first_diff}",
                )
                _progress(
                    total,
                    report,
                    f"MISMATCH {id_} expected={fixture.expected} actual={mismatch}",
                    out,
                )
    _progress(total, report, "DONE", out)
    return report


def write_summary(report: Report, path: Path) -> None:
    """Markdown for $GITHUB_STEP_SUMMARY: one row per test that did not pass."""
    rows = (
        [(id_, "MISMATCH") for id_ in report.mismatched]
        + [(id_, "ERROR") for id_, _ in report.errors]
        + [(id_, "SKIP") for id_, _ in report.skipped]
    )
    lines = [f"### Porting tests: {report.detail}", ""]
    if rows:
        lines += ["| test | fixture | verdict | detail |", "|---|---|---|---|"]
        for id_, verdict in rows:
            fixture, detail = report.notes.get(id_, ("", ""))
            detail = detail.replace("|", "\\|")
            lines.append(f"| `{id_}` | `{fixture}` | {verdict} | {detail} |")
    path.write_text("\n".join(lines) + "\n")
