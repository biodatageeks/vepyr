"""Tests for ``tools/check_normalised_input.py`` (#89).

CI runs the script in the ``input-normalised-check`` workflow. Most tests
inject a fake normaliser so they run anywhere. The integration tests run
the real ``tools/normalize_input`` and are skipped unless bcftools 1.23 on
htslib 1.23.1 is on ``PATH`` -- the only toolchain it accepts.
"""

from __future__ import annotations

import filecmp
import shutil
import subprocess
from pathlib import Path
from typing import Final

import pytest

import check_normalised_input
from check_normalised_input import Status, check_test, discover, main

REPO: Final[Path] = Path(__file__).resolve().parent.parent
DATA: Final[Path] = REPO / "tests" / "data"

VCF: Final[str] = (
    "##fileformat=VCFv4.2\n"
    '##FILTER=<ID=PASS,Description="All filters passed">\n'
    "##contig=<ID=21,length=46709983>\n"
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
    "21\t100\t.\tC\tT\t.\t.\t.\n"
)
TOML: Final[str] = (
    'name = "t"\n\n[input]\n'
    'command = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"\n'
    'bcftools_version = "bcftools 1.23"\n'
)


def _pinned_bcftools() -> bool:
    """Say whether the exact pinned bcftools/htslib pair is on ``PATH``."""
    if shutil.which("bcftools") is None:
        return False
    out = subprocess.run(
        ["bcftools", "--version"], capture_output=True, text=True, check=False
    ).stdout.splitlines()
    return out[:2] == ["bcftools 1.23", "Using htslib 1.23.1"]


requires_pinned_bcftools = pytest.mark.skipif(
    not _pinned_bcftools(), reason="needs bcftools 1.23 / htslib 1.23.1 on PATH"
)


def _identity(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
    """Fake normaliser: copy ``raw`` unchanged and leave ``test.toml`` alone."""
    shutil.copyfile(raw, test_dir / "input.vcf")
    return subprocess.CompletedProcess([], 0, "", "")


def _failing(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
    """Fake normaliser that refuses to run, as with a wrong bcftools."""
    return subprocess.CompletedProcess([], 1, "", "normalize_input: version mismatch")


def _make(root: Path, name: str, *, vcf: str = VCF, toml: str = TOML) -> Path:
    """Create a data-test directory ``root/name`` and return it."""
    d = root / name
    d.mkdir(parents=True)
    (d / "input.vcf").write_text(vcf, encoding="utf-8")
    (d / "test.toml").write_text(toml, encoding="utf-8")
    return d


def test_discover_takes_only_dirs_with_test_toml(tmp_path: Path) -> None:
    _make(tmp_path, "b")
    _make(tmp_path, "a")
    (tmp_path / "no_toml").mkdir()
    assert discover(tmp_path) == [tmp_path / "a", tmp_path / "b"]


def test_ok_when_normaliser_reproduces_files(tmp_path: Path) -> None:
    d = _make(tmp_path, "t")
    assert check_test(d, normalize=_identity).status is Status.OK


def test_toml_rewrite_is_a_mismatch_with_diff(tmp_path: Path) -> None:
    d = _make(tmp_path, "t", toml=TOML.replace("1.23", "1.22"))

    def rewrite(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
        _identity(raw, test_dir)
        (test_dir / "test.toml").write_text(TOML, encoding="utf-8")
        return subprocess.CompletedProcess([], 0, "", "")

    result = check_test(d, normalize=rewrite)
    assert result.status is Status.MISMATCH
    assert "-bcftools_version" in result.details
    assert "+bcftools_version" in result.details


def test_normaliser_failure_is_a_mismatch(tmp_path: Path) -> None:
    d = _make(tmp_path, "t")
    result = check_test(d, normalize=_failing)
    assert result.status is Status.MISMATCH
    assert "version mismatch" in result.details


def test_main_exit_codes_and_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _make(tmp_path, "good")
    assert main([str(tmp_path)], normalize=_identity) == 0
    assert capsys.readouterr().out == f"OK {tmp_path / 'good'}\n"
    assert main([str(tmp_path)], normalize=_failing) == 1
    assert capsys.readouterr().out.startswith(f"MISMATCH {tmp_path / 'good'}\n")


def test_main_fails_when_nothing_found(tmp_path: Path) -> None:
    assert main([str(tmp_path)], normalize=_identity) == 1
    assert main([str(tmp_path / "missing")], normalize=_identity) == 1


def _silent(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
    """Fake normaliser that exits 0 but writes nothing."""
    return subprocess.CompletedProcess([], 0, "", "")


def test_single_dir_ok(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    d = _make(tmp_path, "t")
    assert discover(d) == [d]
    assert main([str(d)], normalize=_identity) == 0
    assert capsys.readouterr().out == f"OK {d}\n"


def test_single_dir_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    d = _make(tmp_path, "t")

    def rewrite(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
        shutil.copyfile(raw, test_dir / "input.vcf")
        (test_dir / "test.toml").write_text(TOML.replace("1.23", "1.22"), "utf-8")
        return subprocess.CompletedProcess([], 0, "", "")

    assert main([str(d)], normalize=rewrite) == 1
    out = capsys.readouterr().out
    assert out.startswith(f"MISMATCH {d}\n")
    assert '+bcftools_version = "bcftools 1.22"' in out


def test_single_dir_exit_codes(tmp_path: Path) -> None:
    d = _make(tmp_path, "t")
    assert main([str(d)], normalize=_identity) == 0
    assert main([str(d)], normalize=_failing) == 1
    empty = tmp_path / "empty"
    empty.mkdir()
    assert main([str(empty)], normalize=_identity) == 1


def test_parent_mode_unchanged(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    a, b = _make(tmp_path, "a"), _make(tmp_path, "b")
    assert discover(tmp_path) == [a, b]
    assert main([str(tmp_path)], normalize=_identity) == 0
    assert capsys.readouterr().out == f"OK {a}\nOK {b}\n"


def test_normaliser_wrote_nothing_is_mismatch(tmp_path: Path) -> None:
    result = check_test(_make(tmp_path, "t"), normalize=_silent)
    assert result.status is Status.MISMATCH
    assert result.details == "tools/normalize_input exited 0 but wrote no input.vcf\n"


def test_normaliser_wrote_nothing_no_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    d = _make(tmp_path, "t")
    assert main([str(tmp_path)], normalize=_silent) == 1
    assert capsys.readouterr().out == (
        f"MISMATCH {d}\ntools/normalize_input exited 0 but wrote no input.vcf\n"
    )


def test_main_rejects_extra_arguments() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["a", "b"])
    assert exc.value.code == 2


def test_never_writes_under_data_dir(tmp_path: Path) -> None:
    d = _make(tmp_path, "t")

    def scribble(raw: Path, test_dir: Path) -> subprocess.CompletedProcess[str]:
        assert not test_dir.is_relative_to(tmp_path)
        (test_dir / "input.vcf").write_text("changed\n", encoding="utf-8")
        return subprocess.CompletedProcess([], 0, "", "")

    assert check_test(d, normalize=scribble).status is Status.MISMATCH
    assert sorted(p.name for p in d.iterdir()) == ["input.vcf", "test.toml"]
    assert (d / "input.vcf").read_text(encoding="utf-8") == VCF


@requires_pinned_bcftools
def test_real_data_tests_pass(tmp_path: Path) -> None:
    copy = tmp_path / "d"
    shutil.copytree(DATA, copy)
    results = list(check_normalised_input.check_all(copy))
    assert results and all(r.status is Status.OK for r in results), results
    assert not filecmp.dircmp(DATA, copy).diff_files


@requires_pinned_bcftools
def test_real_normaliser_catches_stamp_line(tmp_path: Path) -> None:
    d = _make(tmp_path, "t")
    assert check_test(d).status is Status.OK
    lines = VCF.splitlines(keepends=True)
    lines.insert(1, "##bcftools_normVersion=1.23+htslib-1.23.1\n")
    (d / "input.vcf").write_text("".join(lines), encoding="utf-8")
    result = check_test(d)
    assert result.status is Status.MISMATCH
    assert "-##bcftools_normVersion" in result.details
