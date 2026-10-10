"""Tests for the ``tools/normalize_input`` script.

The script has no ``.py`` suffix -- it is meant to be run, and its whole source
is what issue #85's acceptance criterion 9 greps for a forbidden ``--fasta-ref``
switch. It is therefore loaded here by path rather than imported by name.

Tests that shell out to the real bcftools are skipped when it is absent; the
pure-Python ones (usage errors, the missing-tool message) always run.
"""

from __future__ import annotations

import gzip
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tomllib
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

SCRIPT: Final[Path] = Path(__file__).resolve().parent / "normalize_input"

MULTIALLELIC_VCF: Final[str] = (
    "##fileformat=VCFv4.2\n"
    "##contig=<ID=chr21,length=46709983>\n"
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    "chr21\t100\t.\tC\tT,G\t.\t.\t.\n"
)
"""One record with two ALT alleles -- ``-m -both`` must split it into two."""

requires_bcftools = pytest.mark.skipif(
    shutil.which("bcftools") is None,
    reason="needs bcftools on PATH",
)


def _load() -> ModuleType:
    """Import the extension-less script as a module.

    Returns:
        The executed module object.
    """
    loader = SourceFileLoader("normalize_input", str(SCRIPT))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolves annotations via this
    loader.exec_module(module)
    return module


normalize_input = _load()


def _run(*argv: str) -> subprocess.CompletedProcess[str]:
    """Run the script in a subprocess, exactly as a shell would.

    Args:
        *argv: Arguments after the program name.

    Returns:
        The finished process, with text streams captured.
    """
    return subprocess.run(
        [sys.executable, str(SCRIPT), *argv],
        capture_output=True,
        text=True,
    )


@pytest.fixture
def raw_vcf(tmp_path: Path) -> Path:
    """Write the multiallelic scratch VCF and return its path."""
    path = tmp_path / "multi.vcf"
    path.write_text(MULTIALLELIC_VCF, encoding="utf-8")
    return path


def test_the_script_is_executable() -> None:
    """It is invoked as ``tools/normalize_input``, so it must carry +x (AC 2)."""
    assert SCRIPT.stat().st_mode & 0o111, "normalize_input must be executable"


def test_the_source_never_mentions_a_reference_fasta() -> None:
    """``-f`` would left-align indels and move VEP's answers (AC 9).

    This is the literal grep issue #85's AC 9 runs, kept so the criterion has a
    home in the suite. On its own it is *not* a guarantee: the argv is built
    from a tuple, so the source never contains ``norm `` followed by a flag and
    the pattern cannot match even if ``-f`` were added. The falsifiable check is
    :func:`test_the_bcftools_argv_never_carries_a_reference_fasta`.
    """
    source = SCRIPT.read_text(encoding="utf-8")
    assert re.search(r"norm .*(-f|--fasta-ref)", source) is None


def _capture_norm_argv(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Return the argv ``_run_norm`` hands to :func:`subprocess.run`.

    Args:
        monkeypatch: Fixture used to intercept the subprocess call.

    Returns:
        The argument vector, with bcftools never actually started.
    """
    seen: list[str] = []

    def _fake_run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        seen[:] = argv
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(normalize_input.subprocess, "run", _fake_run)
    normalize_input._run_norm(Path("raw.vcf"), Path("out.vcf"))
    return seen


def test_the_bcftools_argv_never_carries_a_reference_fasta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real guarantee: no ``-f``/``--fasta-ref`` reaches bcftools.

    Unlike the source grep of AC 9, this fails if a reference FASTA is ever
    added to ``_NORM_FLAGS`` -- the sabotage that motivated it.
    """
    argv = _capture_norm_argv(monkeypatch)
    assert "-f" not in argv
    assert "--fasta-ref" not in argv


def test_the_bcftools_argv_is_exactly_the_fixed_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`-m -both` and nothing else, in that order (CLAUDE.md's binding rule)."""
    argv = _capture_norm_argv(monkeypatch)
    assert argv == ["bcftools", "norm", "-m", "-both", "-o", "out.vcf", "raw.vcf"]


@requires_bcftools
def test_two_runs_of_the_same_raw_file_are_byte_identical(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The stamped version/command headers are the only nondeterminism (AC 2)."""
    assert _run(str(raw_vcf), str(tmp_path / "t1")).returncode == 0
    assert _run(str(raw_vcf), str(tmp_path / "t2")).returncode == 0
    first = (tmp_path / "t1" / "input.vcf").read_bytes()
    assert first == (tmp_path / "t2" / "input.vcf").read_bytes()
    assert b"bcftools_norm" not in first


@requires_bcftools
def test_normalising_its_own_output_changes_nothing(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """Re-running on ``input.vcf`` must be a fixed point (AC 3)."""
    assert _run(str(raw_vcf), str(tmp_path / "t1")).returncode == 0
    once = tmp_path / "t1" / "input.vcf"
    assert _run(str(once), str(tmp_path / "t3")).returncode == 0
    assert once.read_bytes() == (tmp_path / "t3" / "input.vcf").read_bytes()


@requires_bcftools
def test_a_multiallelic_record_is_split_into_one_line_per_allele(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """``-m -both`` is the whole point of the fixed command (AC 4)."""
    assert _run(str(raw_vcf), str(tmp_path / "t1")).returncode == 0
    body = [
        line
        for line in (tmp_path / "t1" / "input.vcf").read_text().splitlines()
        if not line.startswith("#")
    ]
    assert len(body) == 2
    raw_body = [ln for ln in MULTIALLELIC_VCF.splitlines() if not ln.startswith("#")]
    assert len(raw_body) == 1, "negative control: the raw file has one record"


HEADER_ONLY_VCF: Final[str] = MULTIALLELIC_VCF.rsplit("#CHROM", 1)[0] + (
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)
"""A valid VCF header with no records -- a data-test that compares nothing."""


@pytest.fixture
def header_only_vcf(tmp_path: Path) -> Path:
    """Write the record-less scratch VCF and return its path."""
    path = tmp_path / "empty.vcf"
    path.write_text(HEADER_ONLY_VCF, encoding="utf-8")
    return path


@requires_bcftools
def test_header_only_is_refused(header_only_vcf: Path, tmp_path: Path) -> None:
    """A record-less input exits 1 with a message naming the raw file (#167)."""
    done = _run(str(header_only_vcf), str(tmp_path / "e1"))
    assert done.returncode == 1
    assert f"{header_only_vcf} has no records" in done.stderr


@requires_bcftools
def test_header_only_creates_no_input(header_only_vcf: Path, tmp_path: Path) -> None:
    """The refusal comes before publishing: only the directory itself remains."""
    test_dir = tmp_path / "e2"
    assert _run(str(header_only_vcf), str(test_dir)).returncode == 1
    assert test_dir.is_dir(), "created before bcftools ran, and not removed"
    assert list(test_dir.iterdir()) == []


@requires_bcftools
def test_header_only_leaves_files_untouched(
    header_only_vcf: Path, raw_vcf: Path, tmp_path: Path
) -> None:
    """A pre-existing ``input.vcf`` and ``test.toml`` stay byte-identical."""
    test_dir = tmp_path / "e3"
    assert _run(str(raw_vcf), str(test_dir)).returncode == 0
    before = {p.name: p.read_bytes() for p in test_dir.iterdir()}
    assert sorted(before) == ["input.vcf", "test.toml"]

    assert _run(str(header_only_vcf), str(test_dir)).returncode == 1

    assert {p.name: p.read_bytes() for p in test_dir.iterdir()} == before


@requires_bcftools
def test_one_record_still_accepted(tmp_path: Path) -> None:
    """Control: the guard does not over-refuse -- one record is enough."""
    raw = tmp_path / "one.vcf"
    raw.write_text(HEADER_ONLY_VCF + "chr21\t100\t.\tC\tT\t.\t.\t.\n", encoding="utf-8")
    test_dir = tmp_path / "e4"
    assert _run(str(raw), str(test_dir)).returncode == 0
    body = [
        ln
        for ln in (test_dir / "input.vcf").read_text().splitlines()
        if not ln.startswith("#")
    ]
    assert len(body) == 1


@requires_bcftools
def test_a_gzipped_raw_file_gives_the_same_bytes(raw_vcf: Path, tmp_path: Path) -> None:
    """bcftools detects the container, so the script passes it through (AC 6)."""
    gz = tmp_path / "multi.vcf.gz"
    gz.write_bytes(gzip.compress(raw_vcf.read_bytes()))
    assert _run(str(raw_vcf), str(tmp_path / "t1")).returncode == 0
    assert _run(str(gz), str(tmp_path / "t5")).returncode == 0
    assert (tmp_path / "t1" / "input.vcf").read_bytes() == (
        tmp_path / "t5" / "input.vcf"
    ).read_bytes()


@requires_bcftools
def test_test_toml_records_the_fixed_command_and_the_tool_version(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The recorded command is the template, not this run's paths (AC 7, 8)."""
    assert _run(str(raw_vcf), str(tmp_path / "t1")).returncode == 0
    lines = (tmp_path / "t1" / "test.toml").read_text(encoding="utf-8").splitlines()
    banner = subprocess.run(
        ["bcftools", "--version"], capture_output=True, text=True, check=True
    ).stdout.splitlines()[0]
    assert "[input]" in lines
    assert 'command = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"' in lines
    assert f'bcftools_version = "{banner}"' in lines
    # Success here is tied to the *pinned* version, not to "some version":
    # the run could only have got this far because the banner matched.
    assert banner == normalize_input._REQUIRED_VERSION == "bcftools 1.23"


def test_the_pinned_versions_are_stated_constants() -> None:
    """The pin is a stated constant, not whatever happens to be installed."""
    assert normalize_input._REQUIRED_VERSION == "bcftools 1.23"
    assert normalize_input._REQUIRED_HTSLIB == "htslib 1.23.1"


@requires_bcftools
def test_the_installed_htslib_is_the_pinned_one() -> None:
    """The htslib line is read off the banner and equals the pin (AC 1)."""
    assert normalize_input.bcftools_versions().htslib == "htslib 1.23.1"


def _refuse_banner(
    stdout: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """Run ``main`` against a faked ``bcftools --version`` banner.

    ``bcftools norm`` must never be reached, so the fake asserts on its own
    argv rather than quietly pretending to normalise.

    Args:
        stdout: What ``bcftools --version`` should appear to print.
        tmp_path: Directory holding the raw VCF and the refused test dir.
        monkeypatch: Fixture used to fake ``which`` and ``subprocess.run``.
        capsys: Fixture used to read the error message.

    Returns:
        The captured stderr.
    """
    raw = tmp_path / "multi.vcf"
    raw.write_text(MULTIALLELIC_VCF, encoding="utf-8")

    def _fake_run(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        assert argv[1] == "--version", f"bcftools norm must not run: {argv}"
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    monkeypatch.setattr(normalize_input.shutil, "which", lambda _name: "/usr/bin/x")
    monkeypatch.setattr(normalize_input.subprocess, "run", _fake_run)
    target = tmp_path / "t_pin"
    assert normalize_input.main([str(raw), str(target)]) == 1
    assert not target.exists(), "a refused toolchain must not create the test dir"
    return capsys.readouterr().err


def test_a_wrong_bcftools_version_is_refused_before_anything_is_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An unpinned bcftools must stop the run before it touches the test dir.

    Negative control: against the pre-pin script this fails twice over -- the
    message names no versions, and ``test_dir`` has already been created by the
    time the run gives up.
    """
    err = _refuse_banner(
        "bcftools 1.20\nUsing htslib 1.20\n", tmp_path, monkeypatch, capsys
    )
    assert "bcftools 1.20" in err and "bcftools 1.23" in err


def test_a_wrong_htslib_under_the_right_bcftools_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """bcftools is a front end; the htslib under it is what parses the VCF.

    Negative control: the bcftools line here is exactly the pinned one, so a
    script that checked only that line -- every revision of this one before the
    htslib pin -- would run bcftools and trip the fake's assertion.
    """
    err = _refuse_banner(
        "bcftools 1.23\nUsing htslib 1.20.0\n", tmp_path, monkeypatch, capsys
    )
    assert "htslib 1.20.0" in err and "htslib 1.23.1" in err


def test_a_banner_without_an_htslib_line_is_refused_not_crashed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A banner that cannot be read is refused, not indexed into blindly.

    No released bcftools does this, but the failure mode is chosen rather than
    inherited: an unreadable banner is an unverifiable one, and under a pin
    that means stopping -- with a message, not an ``IndexError``.
    """
    err = _refuse_banner("bcftools 1.23\n", tmp_path, monkeypatch, capsys)
    assert "htslib" in err
    assert "IndexError" not in err


@requires_bcftools
def test_an_existing_test_toml_keeps_its_other_tables(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The script owns ``[input]`` only; #32 and #80 own the rest.

    The comment above ``[vep]`` is the case that matters: ``test.toml`` is
    where the links to the upstream VEP test are recorded, and those links live
    in comments. An end-of-table scan that stops at the next ``[`` swallows
    them, so they are asserted explicitly here rather than implied.
    """
    test_dir = tmp_path / "t7"
    test_dir.mkdir()
    (test_dir / "test.toml").write_text(
        'name = "demo"\n'
        "\n"
        "[input]\n"
        'command = "stale"\n'
        "\n"
        "# The VEP invocation used to build expected.vcf\n"
        "# https://github.com/Ensembl/ensembl-vep/blob/release/116.2/t/AnnotationSource.t\n"
        "[vep]\n"
        'args = "--cache"\n',
        encoding="utf-8",
    )
    assert _run(str(raw_vcf), str(test_dir)).returncode == 0
    text = (test_dir / "test.toml").read_text(encoding="utf-8")
    assert 'name = "demo"' in text
    assert "stale" not in text
    assert 'command = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"' in text
    # Both comment lines survive, still attached to [vep], still after a blank
    # separator. A scan that ate the trivia would glue [vep] to the version key.
    assert (
        "\n"
        "# The VEP invocation used to build expected.vcf\n"
        "# https://github.com/Ensembl/ensembl-vep/blob/release/116.2/t/AnnotationSource.t\n"
        "[vep]\n"
        'args = "--cache"\n'
    ) in text
    assert tomllib.loads(text)["vep"]["args"] == "--cache"


def test_a_table_with_no_trivia_before_the_next_one_still_round_trips(
    tmp_path: Path,
) -> None:
    """The backward scan must not run past the table it is replacing."""
    toml_path = tmp_path / "test.toml"
    toml_path.write_text('[input]\ncommand = "stale"\n[vep]\n', encoding="utf-8")
    text = normalize_input.render_input_table(toml_path, version="bcftools 1.23")
    assert "stale" not in text
    assert "[vep]" in text
    assert tomllib.loads(text)["input"]["bcftools_version"] == "bcftools 1.23"


def test_an_empty_input_table_is_replaced_without_eating_the_next_header(
    tmp_path: Path,
) -> None:
    """``end`` may never reach ``start``, even when the table body is blank."""
    toml_path = tmp_path / "test.toml"
    toml_path.write_text('[input]\n\n[vep]\nargs = "--cache"\n', encoding="utf-8")
    parsed = tomllib.loads(
        normalize_input.render_input_table(toml_path, version="bcftools 1.23")
    )
    assert parsed["input"]["command"] == normalize_input.COMMAND_TEMPLATE
    assert parsed["vep"]["args"] == "--cache"


def test_a_bracket_inside_a_multiline_string_is_refused_not_corrupted(
    tmp_path: Path,
) -> None:
    """The line scan cannot see multi-line strings, so it must refuse to guess.

    A ``[``-starting line inside a multi-line string looks like a table header.
    Before the tomllib guard this truncated the table and wrote a file that no
    longer parsed -- silent corruption of the file #80 reads.
    """
    toml_path = tmp_path / "test.toml"
    original = '[input]\nnotes = """\n[vep] section below is authoritative\n"""\n'
    toml_path.write_text(original, encoding="utf-8")
    assert tomllib.loads(original), "negative control: the input is valid TOML"

    with pytest.raises(normalize_input.NormalizeError, match="not valid TOML"):
        normalize_input.render_input_table(toml_path, version="bcftools 1.23")
    assert toml_path.read_text(encoding="utf-8") == original, "left untouched"


@pytest.mark.parametrize("existing", [False, True], ids=["new_file", "existing_file"])
def test_a_banner_that_would_break_the_toml_is_refused(
    tmp_path: Path, existing: bool
) -> None:
    """Both branches validate, or the guard #80 relies on is only half there.

    ``version`` goes into a basic TOML string unescaped, so a quote in the
    bcftools banner makes the table unparseable. The new-file branch used to
    return it unchecked and the script exited 0 on a corrupt ``test.toml``.
    """
    toml_path = tmp_path / "test.toml"
    if existing:
        toml_path.write_text('name = "demo"\n', encoding="utf-8")

    with pytest.raises(normalize_input.NormalizeError, match="not valid TOML"):
        normalize_input.render_input_table(toml_path, version='bcftools 1.23 "x"')

    if existing:
        assert toml_path.read_text(encoding="utf-8") == 'name = "demo"\n'
    else:
        assert not toml_path.exists()


def test_a_normal_banner_still_renders_for_a_new_file(tmp_path: Path) -> None:
    """Negative control: the guard above rejects the banner, not every banner."""
    rendered = normalize_input.render_input_table(
        tmp_path / "test.toml", version="bcftools 1.23"
    )
    assert tomllib.loads(rendered)["input"]["bcftools_version"] == "bcftools 1.23"


def test_an_existing_file_that_is_not_toml_is_refused(tmp_path: Path) -> None:
    """Garbage in must not become differently-shaped garbage out."""
    toml_path = tmp_path / "test.toml"
    toml_path.write_text("this is not = = toml\n", encoding="utf-8")
    with pytest.raises(normalize_input.NormalizeError, match="the existing file"):
        normalize_input.render_input_table(toml_path, version="bcftools 1.23")


@requires_bcftools
def test_a_broken_test_toml_leaves_the_whole_test_directory_untouched(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """A refused run must write *nothing*, not just refuse the TOML.

    ``input.vcf`` used to be written before ``test.toml`` was even read, so a
    refusal left the data-test holding a fresh input with no matching
    ``[input]`` table beside it -- and an exit code saying the run had failed.
    The same promise the argparse path already keeps.
    """
    test_dir = tmp_path / "t9"
    test_dir.mkdir()
    broken = 'name = "demo"\nbroken = = toml\n'
    (test_dir / "test.toml").write_text(broken, encoding="utf-8")
    (test_dir / "input.vcf").write_text("PREVIOUS-GOOD-INPUT\n", encoding="utf-8")

    done = _run(str(raw_vcf), str(test_dir))

    assert done.returncode == 1
    assert "not valid TOML" in done.stderr
    assert (test_dir / "test.toml").read_text(encoding="utf-8") == broken
    assert (test_dir / "input.vcf").read_text(
        encoding="utf-8"
    ) == "PREVIOUS-GOOD-INPUT\n"


@requires_bcftools
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permission bits")
def test_an_unwritable_test_dir_fails_cleanly_and_changes_nothing(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """A write that cannot happen must exit 1, not raise a traceback.

    The documents are staged inside the test directory and renamed into place,
    so a directory that refuses to be written fails before either final path
    is touched -- and fails as the script's own error, not as a bare
    ``PermissionError`` from the interpreter.
    """
    test_dir = tmp_path / "t11"
    test_dir.mkdir()
    toml_before = 'name = "demo"\n'
    vcf_before = "PREVIOUS-GOOD-INPUT\n"
    (test_dir / "test.toml").write_text(toml_before, encoding="utf-8")
    (test_dir / "input.vcf").write_text(vcf_before, encoding="utf-8")
    test_dir.chmod(0o555)
    try:
        done = _run(str(raw_vcf), str(test_dir))
    finally:
        test_dir.chmod(0o755)

    assert done.returncode == 1
    assert "Traceback" not in done.stderr, done.stderr
    assert done.stderr.startswith("normalize_input: ")
    assert (test_dir / "test.toml").read_text(encoding="utf-8") == toml_before
    assert (test_dir / "input.vcf").read_text(encoding="utf-8") == vcf_before


@requires_bcftools
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permission bits")
def test_a_read_only_test_toml_in_a_writable_dir_is_still_updated(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The rename path replaces a mode-444 test.toml instead of dying on it.

    Writing to the file directly raised ``PermissionError`` halfway through the
    run, after ``input.vcf`` had already been replaced. A rename needs write
    permission on the directory, not on the file, so the pair now lands
    together.
    """
    test_dir = tmp_path / "t12"
    test_dir.mkdir()
    (test_dir / "test.toml").write_text('name = "demo"\n', encoding="utf-8")
    (test_dir / "test.toml").chmod(0o444)

    done = _run(str(raw_vcf), str(test_dir))

    assert done.returncode == 0, done.stderr
    assert "Traceback" not in done.stderr
    parsed = tomllib.loads((test_dir / "test.toml").read_text(encoding="utf-8"))
    assert parsed["name"] == "demo"
    assert parsed["input"]["command"] == normalize_input.COMMAND_TEMPLATE
    body = [
        ln
        for ln in (test_dir / "input.vcf").read_text().splitlines()
        if not ln.startswith("#")
    ]
    assert len(body) == 2


@requires_bcftools
@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores permission bits")
def test_an_unreadable_test_toml_blames_the_read_not_the_write(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The error names the operation that actually failed."""
    test_dir = tmp_path / "t14"
    test_dir.mkdir()
    toml_path = test_dir / "test.toml"
    toml_path.write_text('name = "demo"\n', encoding="utf-8")
    toml_path.chmod(0o000)
    try:
        done = _run(str(raw_vcf), str(test_dir))
    finally:
        toml_path.chmod(0o644)

    assert done.returncode == 1
    assert "could not read" in done.stderr
    assert "could not write" not in done.stderr
    assert not (test_dir / "input.vcf").exists()


@requires_bcftools
def test_a_failed_run_leaves_an_empty_dir_and_no_scratch_file(
    tmp_path: Path,
) -> None:
    """Pin the one thing a failed run *does* leave behind.

    ``test_dir`` is created before bcftools runs, so a bcftools failure cannot
    unmake it. The docstrings say so rather than claiming a failed run changes
    nothing at all; this holds them to it, and to the scratch dir being gone.
    """
    bad = tmp_path / "bad.vcf"
    bad.write_text("not a vcf at all\n", encoding="utf-8")
    test_dir = tmp_path / "t15"

    done = _run(str(bad), str(test_dir))

    assert done.returncode == 1
    assert test_dir.is_dir(), "created before bcftools ran, and not removed"
    assert list(test_dir.iterdir()) == [], "no scratch dir, no half-written files"


@requires_bcftools
def test_no_scratch_file_survives_a_successful_run(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """Staging happens inside the test dir, so it must not leave litter."""
    test_dir = tmp_path / "t13"
    assert _run(str(raw_vcf), str(test_dir)).returncode == 0
    assert sorted(p.name for p in test_dir.iterdir()) == ["input.vcf", "test.toml"]


@requires_bcftools
def test_a_refused_run_creates_no_input_vcf_at_all(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The same guarantee when the test directory had no input.vcf to begin."""
    test_dir = tmp_path / "t10"
    test_dir.mkdir()
    (test_dir / "test.toml").write_text("not = = toml\n", encoding="utf-8")
    assert _run(str(raw_vcf), str(test_dir)).returncode == 1
    assert not (test_dir / "input.vcf").exists()


def test_an_extra_flag_is_refused_before_anything_is_written(
    raw_vcf: Path, tmp_path: Path
) -> None:
    """The script defines no options, so argparse rejects ``-f`` (AC 5)."""
    target = tmp_path / "t4"
    done = _run("-f", "ref.fa", str(raw_vcf), str(target))
    assert done.returncode == 2
    assert not (target / "input.vcf").exists()


def test_a_missing_bcftools_is_reported_and_nothing_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A clear message and a non-zero code beat a confusing traceback (AC 10)."""
    raw = tmp_path / "multi.vcf"
    raw.write_text(MULTIALLELIC_VCF, encoding="utf-8")
    monkeypatch.setattr(normalize_input.shutil, "which", lambda _name: None)
    target = tmp_path / "t6"
    assert normalize_input.main([str(raw), str(target)]) == 1
    assert "bcftools" in capsys.readouterr().err
    assert not (target / "input.vcf").exists()


def test_a_missing_raw_file_is_reported(tmp_path: Path) -> None:
    """Nothing downstream can recover from a typo'd source path."""
    done = _run(str(tmp_path / "nope.vcf"), str(tmp_path / "t8"))
    assert done.returncode == 1
    assert "nope.vcf" in done.stderr
