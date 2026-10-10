"""Tests for ``tools/workspace_guard`` (issue #163).

The script has no ``.py`` suffix, so it is loaded by path (as in
``test_normalize_input.py``) and registered in :data:`sys.modules` first (its
``@dataclass(slots=True)`` class needs its module there). Every case builds
throwaway git repositories under ``tmp_path`` (resolved with ``realpath``, since
git reports ``/private/var/...`` for macOS's ``/var/...``); nothing touches the
network or this checkout.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from collections.abc import Callable
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

SCRIPT: Final[Path] = Path(__file__).resolve().parent / "workspace_guard"
GIT_ID: Final[tuple[str, ...]] = ("-c", "user.name=t", "-c", "user.email=t@t")

type Guard = Callable[..., tuple[int, str]]


def load_guard() -> ModuleType:
    """Import ``tools/workspace_guard`` by path as the module ``workspace_guard``."""
    loader = SourceFileLoader("workspace_guard", str(SCRIPT))
    spec = importlib.util.spec_from_loader("workspace_guard", loader)
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["workspace_guard"] = mod
    loader.exec_module(mod)
    return mod


def git(cwd: Path, *args: str) -> str:
    """Run ``git -C cwd <args>`` (fail on error) and return its stripped stdout."""
    return subprocess.run(
        ["git", "-C", str(cwd), *GIT_ID, *args],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


@pytest.fixture
def guard(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> Guard:
    """The tool's ``main``, ``DT_ALLOW_MAIN``/``DT_REPO`` unset -> (exit, stdout)."""
    monkeypatch.delenv("DT_ALLOW_MAIN", raising=False)
    monkeypatch.delenv("DT_REPO", raising=False)
    mod = load_guard()

    def call(*argv: str | Path) -> tuple[int, str]:
        capsys.readouterr()
        try:
            code = mod.main([str(a) for a in argv])
        except SystemExit as e:  # argparse usage errors
            code = int(e.code or 0)
        return code, capsys.readouterr().out

    return call


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """``tmp_path`` resolved (``/private/var/...`` on macOS), outside every checkout."""
    return Path(os.path.realpath(tmp_path))


@pytest.fixture
def work(root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Clone ``w`` of a bare origin, one commit on ``origin/master``; cwd is ``w``."""
    git(root, "init", "-q", "--bare", "o.git")
    git(root, "clone", "-q", str(root / "o.git"), "w")
    w = root / "w"
    git(w, "commit", "-q", "--allow-empty", "-m", "one")
    git(w, "push", "-q", "origin", "HEAD:refs/heads/master")
    git(w, "fetch", "-q", "origin")
    (w / "tests" / "data").mkdir(parents=True)
    monkeypatch.chdir(w)
    return w


def other_dir(root: Path) -> Path:
    """An existing directory outside every checkout."""
    (d := root / "elsewhere").mkdir(exist_ok=True)
    return d


# ---------------------------------------------------------------- write-target


def test_relative_protect(guard: Guard, work: Path) -> None:
    # "." is the cwd, itself the target checkout: resolving it would refuse with exit 1.
    code, out = guard("write-target", work / "tests/data/new", "--protect", ".")
    assert code == 2, out
    assert "not an absolute path" in out


def test_missing_protect(guard: Guard, work: Path, root: Path) -> None:
    assert (
        guard(
            "write-target",
            work / "tests/data/new",
            "--protect",
            root / "does-not-exist",
        )[0]
        == 2
    )
    assert guard("write-target", work / "tests/data/new", "--protect", "")[0] == 2


def test_no_protect(guard: Guard, work: Path) -> None:
    assert guard("write-target", work / "tests/data/new")[0] == 2


def test_symlink_protect(guard: Guard, work: Path, root: Path) -> None:
    (link := root / "link").symlink_to(work)
    code, out = guard("write-target", work / "tests/data/new", "--protect", link)
    assert code == 1, out
    assert "protected checkout" in out


def test_case_variant_protect(guard: Guard, work: Path) -> None:
    variant = Path(str(work).swapcase())
    if not variant.exists():
        pytest.skip("case-sensitive filesystem: the case variant does not exist")
    code, out = guard("write-target", work / "tests/data/new", "--protect", variant)
    assert code == 1, out


def test_protected_checkout_refused(
    guard: Guard, work: Path, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DT_REPO", str(work))  # never an override
    code, out = guard(
        "write-target",
        work / "tests/data/new",
        "--protect",
        other_dir(root),
        "--protect",
        work,
    )
    assert code == 1, out
    assert out.startswith("REFUSED write-target")


def test_allow_main_override(
    guard: Guard, work: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DT_ALLOW_MAIN", "1")
    code, out = guard("write-target", work / "tests/data/new", "--protect", work)
    assert code == 0, out
    assert "allowed by DT_ALLOW_MAIN=1" in out
    monkeypatch.setenv("DT_ALLOW_MAIN", "yes")  # only the exact value "1" overrides
    assert guard("write-target", work / "tests/data/new", "--protect", work)[0] == 1


def test_nested_target(guard: Guard, work: Path, root: Path) -> None:
    code, out = guard(
        "write-target", work / "tests/data/a/b", "--protect", other_dir(root)
    )
    assert code == 1, out
    assert "not a direct child" in out
    assert (
        guard("write-target", work / "tests/data/a", "--protect", other_dir(root))[0]
        == 0
    )


def test_outside_tests_data(guard: Guard, work: Path, root: Path) -> None:
    assert (
        guard("write-target", work / "elsewhere", "--protect", other_dir(root))[0] == 1
    )
    assert (
        guard("write-target", work / "tests/data", "--protect", other_dir(root))[0] == 1
    )
    assert (
        guard("write-target", root / "tests/data/x", "--protect", other_dir(root))[0]
        == 1
    )  # no checkout
    assert (
        guard("write-target", "tests/data/x", "--protect", other_dir(root))[0] == 2
    )  # relative DIR


def test_cwd_other_checkout(
    guard: Guard, work: Path, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    git(root, "init", "-q", "second")
    for cwd in (root / "second", other_dir(root)):
        monkeypatch.chdir(cwd)
        code, out = guard(
            "write-target", work / "tests/data/new", "--protect", other_dir(root)
        )
        assert code == 1, out
        assert "cwd checkout" in out


def test_expect_checkout_mismatch(guard: Guard, work: Path, root: Path) -> None:
    git(root, "init", "-q", "second")
    target = work / "tests/data/new"
    code, out = guard(
        "write-target",
        target,
        "--protect",
        other_dir(root),
        "--expect-checkout",
        root / "second",
    )
    assert code == 1, out
    assert "expected checkout" in out
    assert (
        guard(
            "write-target",
            target,
            "--protect",
            other_dir(root),
            "--expect-checkout",
            work,
        )[0]
        == 0
    )
    assert (
        guard(
            "write-target",
            target,
            "--protect",
            other_dir(root),
            "--expect-checkout",
            "rel",
        )[0]
        == 2
    )


# ---------------------------------------------------------------- outside-checkouts


def test_scratch_inside_checkout(guard: Guard, work: Path, root: Path) -> None:
    assert guard("outside-checkouts", root / "venv-ok")[0] == 0
    code, out = guard("outside-checkouts", root / "venv-ok", work / ".venv" / "deep")
    assert code == 1, out
    assert f"inside git checkout {work}" in out
    git(work, "worktree", "add", "-q", "--detach", str(root / "wt"))
    assert (
        guard("outside-checkouts", root / "wt" / "scr")[0] == 1
    )  # a worktree is a checkout too


def test_scratch_relative(guard: Guard, work: Path) -> None:
    assert guard("outside-checkouts", "")[0] == 2
    assert guard("outside-checkouts", "rel/venv")[0] == 2
    assert guard("outside-checkouts")[0] == 2


# ---------------------------------------------------------------- base


def test_base_contains(guard: Guard, work: Path) -> None:
    git(work, "checkout", "-q", "--detach")
    code, out = guard("base", "--ref", "origin/master")
    assert (code, out.split(":")[0]) == (0, "OK base origin/master")


def test_base_not_contains(guard: Guard, work: Path, root: Path) -> None:
    git(root, "clone", "-q", str(root / "o.git"), "w2")
    git(root / "w2", "checkout", "-q", "-B", "m", "origin/master")
    git(root / "w2", "commit", "-q", "--allow-empty", "-m", "two")
    git(root / "w2", "push", "-q", "origin", "HEAD:refs/heads/master")
    git(work, "fetch", "-q", "origin")
    assert guard("base")[0] == 1  # default --ref origin/master


def test_base_missing_ref(
    guard: Guard, work: Path, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert guard("base", "--ref", "origin/nonexistent")[0] == 2
    monkeypatch.chdir(other_dir(root))
    assert guard("base")[0] == 2  # not a git repository


# ---------------------------------------------------------------- upstream


def test_upstream_tracks_master(guard: Guard, work: Path) -> None:
    git(work, "checkout", "-q", "-b", "trk", "origin/master")
    code, out = guard("upstream", "--not", "origin/master")
    assert code == 1, out
    assert "trk tracks origin/master" in out


def test_upstream_no_track(guard: Guard, work: Path) -> None:
    git(work, "checkout", "-q", "-b", "ntr", "--no-track", "origin/master")
    assert guard("upstream", "--not", "origin/master")[0] == 0
    git(work, "update-ref", "refs/remotes/origin/other", "HEAD")
    git(work, "branch", "-q", "--set-upstream-to", "origin/other")
    code, out = guard("upstream", "--not", "origin/master")
    assert code == 0, out
    assert "tracks origin/other" in out


def test_upstream_detached(
    guard: Guard, work: Path, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    git(work, "checkout", "-q", "--detach")
    assert guard("upstream", "--not", "origin/master")[0] == 0
    monkeypatch.chdir(other_dir(root))
    assert guard("upstream", "--not", "origin/master")[0] == 2  # not a git repository
