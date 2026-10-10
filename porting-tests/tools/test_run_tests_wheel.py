"""--wheel, --summary-md and --keep-failed (vepyr CI)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from run_tests import cli, install, suite
from run_tests.verdict import Exit, RunTestsError


def test_wheel_flag_parses(tmp_path):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"x")
    args = cli.parse_args(["--wheel", str(wheel), "--summary-md", "s.md"])
    assert (
        args.vepyr is None and args.wheel == wheel and args.summary_md == Path("s.md")
    )


@pytest.mark.parametrize("argv", [[], ["0.9.0", "--wheel", "x.whl"]])
def test_exactly_one_of_version_or_wheel(argv):
    with pytest.raises(SystemExit):
        cli.parse_args(argv)


def test_wheel_target_is_content_addressed(tmp_path):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"abc")
    assert install.wheel_target(wheel) == "wheel-" + hashlib.sha256(b"abc").hexdigest()


@pytest.mark.parametrize("name", ["missing.whl", "vepyr.tar.gz"])
def test_wheel_target_rejects_non_wheels(tmp_path, name):
    path = tmp_path / name
    if name.endswith(".tar.gz"):
        path.write_bytes(b"x")
    with pytest.raises(RunTestsError) as info:
        install.wheel_target(path)
    assert info.value.code == Exit.USAGE


def test_install_wheel_installs_that_file(tmp_path, monkeypatch):
    wheel = tmp_path / "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.whl"
    wheel.write_bytes(b"abc")
    calls = []
    monkeypatch.setattr(install, "_run", lambda argv, **kw: calls.append(argv) or "")
    build = install.install(None, tmp_path / "cache", wheel=wheel)
    assert build.source == "local wheel"
    assert build.target == install.wheel_target(wheel)
    pip = next(c for c in calls if c[:3] == ["uv", "pip", "install"])
    assert pip[-1] == str(wheel.resolve())


def test_write_summary_lists_every_non_pass(tmp_path):
    report = suite.Report(
        passed=["ok"],
        skipped=[("sk", "symbolic deletion")],
        mismatched=["mm"],
        errors=[("er", RunTestsError(Exit.ENGINE, "vepyr annotate exited 1 (run 1)"))],
    )
    report.notes.update(
        sk=("fx_skip", "symbolic deletion"),
        mm=("fx_mm", "expected aaa, got bbb; run 1, record 3"),
        er=("fx_er", "vepyr annotate exited 1 (run 1)"),
    )
    out = tmp_path / "summary.md"
    suite.write_summary(report, out)
    text = out.read_text()
    assert "1 pass, 1 mismatch, 1 error, 1 skipped" in text
    for needle in ("`mm`", "`fx_mm`", "MISMATCH", "`er`", "ERROR", "`sk`", "SKIP"):
        assert needle in text
    assert "`ok`" not in text


def test_write_summary_escapes_pipes(tmp_path):
    report = suite.Report(mismatched=["mm"])
    report.notes["mm"] = ("fx", "a|b")
    out = tmp_path / "s.md"
    suite.write_summary(report, out)
    assert "a\\|b" in out.read_text()
