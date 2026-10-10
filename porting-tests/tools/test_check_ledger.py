"""Tests for ``tools/check_ledger`` (#110).

The script has no ``.py`` suffix, so it is loaded by path, like
``tools/build_test_index`` in ``test_build_test_index.py``. Every test runs
offline: the upstream is a synthetic git repository made in ``tmp_path`` and
``PINNED_COMMIT`` is re-pinned to its HEAD, so the pin and clean-tree checks
run for real against it. The fixtures mirror criteria 2-11, 15 and 21 of #110.
"""

from __future__ import annotations

import csv
import io
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

TOOLS: Final[Path] = Path(__file__).resolve().parent
SCRIPT: Final[Path] = TOOLS / "check_ledger"


def _load() -> ModuleType:
    """Import the extension-less script as a module."""
    loader = SourceFileLoader("check_ledger", str(SCRIPT))
    spec = spec_from_loader(loader.name, loader)
    assert spec is not None
    module = module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolve the module by name
    loader.exec_module(module)
    return module


cl: Final[ModuleType] = _load()

A_T: Final[str] = """\
use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warnings qw(warning);
use_ok('Foo');
ok(1, 'one');   # is(2) in a comment is not an assertion
is(1, 1, "is: pass the test");
my $w = warning { carp 'x' };
like($w, qr/x/, 'captured');
warning { 1 };
cmp_deeply([1], [re('^1$')], 'deep');
throws_ok { die } qr/ok/, 'dies';
done_testing();
"""
"""Six rows (use_ok ok is like cmp_deeply throws_ok); two captures, one comparator."""

B_T: Final[str] = """\
use Test::More;
my $cfg = {};
$cfg->{pass} = 1;
my %h = (fail => 1);
is_deeply(
  [1],
  [1],
  'multi-line'
);
done_testing;
"""
"""One row; ``{pass}`` and ``fail =>`` are autoquoted hash keys, not calls."""

EXPECTED_LIST: Final[str] = (
    "t/A.t\t1\t6\tuse_ok\nt/A.t\t2\t7\tok\nt/A.t\t3\t8\tis\nt/A.t\t4\t10\tlike\n"
    "t/A.t\t5\t12\tcmp_deeply\nt/A.t\t6\t13\tthrows_ok\nt/B.t\t1\t5\tis_deeply\n"
)


def _git(root: Path, *args: str) -> str:
    """Run git in ``root`` with a fixed identity and return stdout."""
    done = subprocess.run(
        ["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout.strip()


@pytest.fixture
def upstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A clean synthetic upstream checkout; the tool is pinned to its HEAD."""
    root = tmp_path / "up"
    (root / "t").mkdir(parents=True)
    (root / "t" / "A.t").write_text(A_T)
    (root / "t" / "B.t").write_text(B_T)
    (root / "t" / "Support.pm").write_text("package Support;\nuse warnings;\n1;\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "fixture")
    monkeypatch.setattr(cl, "PINNED_COMMIT", _git(root, "rev-parse", "HEAD"))
    return root


def _records(root: Path) -> list[list[str]]:
    """A valid 13-column CSV (header first) for the upstream enumeration."""
    rows = [list(cl.COLUMNS)]
    for a in cl.enumerate_upstream(root):
        rows.append([a.vep_file, str(a.n), str(a.perl_line), a.perl_kind,
                     "no-feature", f"desc {a.n}", "why", "unchanged",
                     "", "", "area", "", ""])  # fmt: skip
    return rows


def _write(path: Path, records: list[list[str]]) -> Path:
    """Write ``records`` canonically (the #109 format)."""
    path.write_text(cl.canonical(records), newline="")
    return path


def _check(
    csv_path: Path, root: Path, capsys: pytest.CaptureFixture[str]
) -> tuple[int, str, str]:
    """Run the default mode; return (exit, stdout, stderr)."""
    code = cl.main(["--csv", str(csv_path), "--upstream", str(root)])
    out, err = capsys.readouterr()
    return code, out, err


def test_list_prints_the_line_start_enumeration(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cl.main(["--upstream", str(upstream), "--list"]) == 0
    assert capsys.readouterr().out == EXPECTED_LIST


def test_bare_warning_statements_are_not_rows(upstream: Path) -> None:
    kinds = [a.perl_kind for a in cl.enumerate_upstream(upstream)]
    assert "warning" not in kinds and len(kinds) == 7


def test_list_glob_selects_support_modules(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cl.main(["--upstream", str(upstream), "--list", "--glob", "t/*.pm"]) == 0
    assert capsys.readouterr().out == ""


def test_base_run_passes(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, _ = _check(_write(tmp_path / "d.csv", _records(upstream)), upstream,
                          capsys)  # fmt: skip
    assert (code, out) == (0, "rows 7, files 2, missing 0, orphan 0\n")


type Mutation = Callable[[list[list[str]]], None]


def _append_orphan(r: list[list[str]]) -> None:
    r.append([*r[-1][:1], str(int(r[-1][1]) + 1), *r[-1][2:]])


def _swap_rows(r: list[list[str]]) -> None:
    r[1], r[2] = r[2], r[1]


def _set(row: int, col: int, value: str) -> Mutation:
    def mutate(r: list[list[str]]) -> None:
        r[row][col] = value

    return mutate


def _set_fn(row: int, col: int, fn: Callable[[str], str]) -> Mutation:
    def mutate(r: list[list[str]]) -> None:
        r[row][col] = fn(r[row][col])

    return mutate


def _plus_one(value: str) -> str:
    return str(int(value) + 1)


MUTATIONS: Final[list[tuple[str, Mutation, str]]] = [
    ("2 deleted row", lambda r: r.pop(1), "missing row"),
    ("3 duplicated row", lambda r: r.insert(2, list(r[1])), "duplicate key"),
    ("4 orphan row", _append_orphan, "orphan row"),
    ("5 perl_line", _set_fn(1, 2, _plus_one), "perl_line 7 != upstream 6"),
    ("6 perl_kind", _set(1, 3, "ok"), "perl_kind ok != upstream use_ok"),
    ("7 bad enum", _set(1, 4, "failed"), "category 'failed'"),
    ("8 empty rationale", _set(1, 6, ""), "rationale is empty"),
    ("9 unsorted", _swap_rows, "row out of order"),
    ("10 newline", _set_fn(1, 5, lambda v: v + "\nx"), "newline in field desc"),
    ("i bad issue", _set(1, 12, "http://x"), "neither empty nor an https:// URL"),
    ("i ported without rust_test", _set(1, 4, "unit-port"), "rust_test is empty"),
    ("i vep_file prefix", _set(1, 0, "A.t"), "does not start with t/"),
    ("i non-integer n", _set(1, 1, "01"), "is not a positive integer"),
]  # fmt: skip


@pytest.mark.parametrize(
    ("mutation", "message"),
    [pytest.param(m, msg, id=name) for name, m, msg in MUTATIONS],
)
def test_mutation_is_a_violation(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mutation: Mutation,
    message: str,
) -> None:
    records = _records(upstream)
    mutation(records)
    code, out, err = _check(_write(tmp_path / "m.csv", records), upstream, capsys)
    assert code == 1, out
    assert message in out
    assert "violation" in err


def test_deleted_row_is_the_only_message(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    records = _records(upstream)
    del records[1]
    _, out, _ = _check(_write(tmp_path / "m.csv", records), upstream, capsys)
    assert out.splitlines() == [
        "t/A.t:1: missing row (upstream line 6, use_ok)",
        "rows 6, files 2, missing 1, orphan 0",
    ]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        pytest.param(lambda t: t.replace("\n", "\r\n"), "canonical", id="11 CRLF"),
        pytest.param(lambda t: "﻿" + t, "BOM", id="11 BOM"),
        pytest.param(lambda t: t.rstrip("\n"), "canonical", id="no final LF"),
        pytest.param(lambda t: t.replace("desc 1", '"desc 1"'), "canonical",
                     id="needless quotes"),
        pytest.param(lambda t: "", "empty file", id="empty"),
    ],
)  # fmt: skip
def test_non_canonical_bytes_are_a_violation(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    raw: Callable[[str], str],
    message: str,
) -> None:
    path = tmp_path / "m.csv"
    path.write_bytes(raw(cl.canonical(_records(upstream))).encode())
    code, out, _ = _check(path, upstream, capsys)
    assert code == 1 and message in out


def _with_verdict(records: list[list[str]]) -> list[list[str]]:
    """The #101 14-column form of ``records`` (as criterion 21's ``mk14``)."""
    return [r[:13] + (["data_test_verdict"] if i == 0 else ["UNSURE"])
            for i, r in enumerate(records)]  # fmt: skip


def test_fourteenth_column_is_accepted(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    records = _with_verdict(_records(upstream))
    code, out, _ = _check(_write(tmp_path / "m.csv", records), upstream, capsys)
    assert (code, out) == (0, "rows 7, files 2, missing 0, orphan 0\n")


def _pop(r: list[list[str]]) -> None:
    r[1].pop()


def _fifteenth(r: list[list[str]]) -> None:
    for row in r:
        row.append("x")


FOURTEEN: Final[list[tuple[str, Mutation, str]]] = [
    ("other header", _set(0, 13, "data_test_verdicts"), "header must be"),
    ("empty value", _set(1, 13, ""), "data_test_verdict is empty"),
    ("15th column", _fifteenth, "header must be"),
    ("short row", _pop, "13 fields, header has 14"),
    ("perl_line", _set_fn(1, 2, _plus_one), "perl_line 7 != upstream 6"),
]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [pytest.param(m, msg, id=f"21 {name}") for name, m, msg in FOURTEEN],
)
def test_fourteenth_column_mutations(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mutation: Mutation,
    message: str,
) -> None:
    records = _with_verdict(_records(upstream))
    mutation(records)
    code, out, _ = _check(_write(tmp_path / "m.csv", records), upstream, capsys)
    assert code == 1 and message in out


def test_extra_commit_cannot_be_measured(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    _git(upstream, "commit", "-q", "--allow-empty", "-m", "x")
    code, _, err = _check(csv_path, upstream, capsys)
    assert code == 2 and "!= pinned" in err


def test_dirty_tree_cannot_be_measured(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    with (upstream / "t" / "A.t").open("a") as fh:
        fh.write("x")
    code, _, err = _check(csv_path, upstream, capsys)
    assert code == 2 and "dirty" in err


# --- Checkout integrity: bytes vs pinned blobs (#184) ---------------------------------

EDIT: Final[bytes] = b"1;\n"
"""The edit of the #184 reproductions: the file then has no assertion."""


def _modes(root: Path, capsys: pytest.CaptureFixture[str]) -> list[tuple[int, str]]:
    """Run ``--list`` and ``--sweep`` on ``root``; return (exit, stderr) of each."""
    results: list[tuple[int, str]] = []
    for mode in ("--list", "--sweep"):
        code = cl.main(["--upstream", str(root), mode])
        results.append((code, capsys.readouterr().err))
    return results


def _git_status_lines(root: Path) -> int:
    """Number of ``git status --porcelain`` lines of ``root``."""
    return len(_git(root, "status", "--porcelain").splitlines())


def test_assume_unchanged_edit_is_refused(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _git(upstream, "update-index", "--assume-unchanged", "t/A.t")
    (upstream / "t" / "A.t").write_bytes(EDIT)
    assert _git_status_lines(upstream) == 0  # git status is blind to the edit
    for code, err in _modes(upstream, capsys):
        assert code == 2 and "t/A.t is marked assume-unchanged" in err


def test_default_mode_refuses_assume_unchanged(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    records = [r for r in _records(upstream) if r[0] != "t/A.t"]
    csv_path = _write(tmp_path / "noA.csv", records)
    _git(upstream, "update-index", "--assume-unchanged", "t/A.t")
    (upstream / "t" / "A.t").write_bytes(EDIT)
    code, out, err = _check(csv_path, upstream, capsys)
    assert code == 2 and "t/A.t is marked assume-unchanged" in err and not out


def test_skip_worktree_flag_is_refused(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _git(upstream, "update-index", "--skip-worktree", "t/B.t")
    assert _git_status_lines(upstream) == 0
    for code, err in _modes(upstream, capsys):
        assert code == 2 and "t/B.t is marked skip-worktree" in err


def test_content_differs_from_pinned_blob_is_refused(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Hide an edit with assume-unchanged, then check the hash alone catches it."""
    _git(upstream, "update-index", "--assume-unchanged", "t/A.t")
    (upstream / "t" / "A.t").write_bytes(EDIT)
    with pytest.raises(cl.CannotMeasure, match=r"pinned tree \(1: t/A.t\)"):
        cl.verify_blobs(upstream, cl.pinned_blobs(upstream, "t/*.t"))
    cl.verify_blobs(upstream, cl.pinned_blobs(upstream, "t/B.t"))  # B is untouched


def test_pinned_blob_id_matches_git() -> None:
    for data in (b"", A_T.encode(), b"x\r\ny\n"):
        done = subprocess.run(
            ["git", "hash-object", "--no-filters", "--stdin"],
            input=data, capture_output=True, check=True,
        )  # fmt: skip
        assert cl.blob_id(data, 40) == done.stdout.decode().strip()


def test_autocrlf_checkout(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """#184 Q1 = (b): a ``core.autocrlf=true`` clone passes with the same rows."""
    crlf = tmp_path / "crlf"
    subprocess.run(
        ["git", "clone", "-q", "-c", "core.autocrlf=true", str(upstream), str(crlf)],
        check=True,
    )
    assert b"\r\n" in (crlf / "t" / "A.t").read_bytes()
    assert cl.main(["--upstream", str(crlf), "--list"]) == 0
    assert capsys.readouterr().out == EXPECTED_LIST
    # A lone CR is not what autocrlf produces: the content is refused.
    (crlf / "t" / "A.t").write_bytes(A_T.encode().replace(b"\n", b"\r\n") + b"\r")
    with pytest.raises(cl.CannotMeasure, match="differs from the pinned tree"):
        cl.verify_blobs(crlf, cl.pinned_blobs(crlf, "t/*.t"))


def test_lone_cr_is_not_autocrlf_converted(
    upstream: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lone CR makes git skip the CRLF conversion, so the candidate is refused.

    The pinned file itself holds a lone CR; on disk every LF becomes CRLF. The
    CRLF-to-LF candidate then equals the pinned bytes, but git's clean filter
    would leave such a file unchanged, so the content differs from the pin.
    """
    pinned = b"ok(1);\rx\n"
    (upstream / "t" / "CR.pm").write_bytes(pinned)
    _git(upstream, "add", "t/CR.pm")
    _git(upstream, "commit", "-q", "-m", "lone CR")
    monkeypatch.setattr(cl, "PINNED_COMMIT", _git(upstream, "rev-parse", "HEAD"))
    cl.verify_blobs(upstream, cl.pinned_blobs(upstream, "t/CR.pm"))  # control
    on_disk = pinned.replace(b"\n", b"\r\n")
    assert on_disk.replace(b"\r\n", b"\n") == pinned
    (upstream / "t" / "CR.pm").write_bytes(on_disk)
    with pytest.raises(cl.CannotMeasure, match=r"pinned tree \(1: t/CR.pm\)"):
        cl.verify_blobs(upstream, cl.pinned_blobs(upstream, "t/CR.pm"))


def test_replace_ref_is_refused(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """F1 of the PR #191 review: a replace ref swaps HEAD's tree for an edited one.

    ``rev-parse HEAD`` still prints the pin and ``git status`` compares with the
    replaced tree, so it is clean; only ``--no-replace-objects`` exposes the edit.
    """
    assert all(code == 0 for code, _ in _modes(upstream, capsys))  # control
    pin = _git(upstream, "rev-parse", "HEAD")
    (upstream / "t" / "A.t").write_bytes(EDIT)
    _git(upstream, "add", "t/A.t")
    fake = _git(upstream, "commit-tree", _git(upstream, "write-tree"), "-m", "x")
    _git(upstream, "replace", pin, fake)
    _git(upstream, "reset", "-q")
    assert _git(upstream, "rev-parse", "HEAD") == pin
    assert _git_status_lines(upstream) == 0  # git (with replace refs) sees no edit
    for code, err in _modes(upstream, capsys):
        assert code == 2 and "upstream tree is dirty" in err and "t/A.t" in err, err


def test_core_worktree_redirect_is_refused(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """F: the checkout's own ``core.worktree`` points git at a clean copy."""
    clean = tmp_path / "clean"
    shutil.copytree(upstream, clean)
    (upstream / "t" / "A.t").write_bytes(EDIT)
    _git(upstream, "config", "core.worktree", str(clean))
    assert _git_status_lines(upstream) == 0
    for code, err in _modes(upstream, capsys):
        assert code == 2 and "differs from the pinned tree (1: t/A.t)" in err
    _git(upstream, "config", "--unset", "core.worktree")  # control: git sees it now
    assert all(code == 2 and "dirty" in err for code, err in _modes(upstream, capsys))


def test_fsmonitor_hook_is_refused(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """I: a ``core.fsmonitor`` hook reporting nothing, edit keeps the mtime."""
    hook = tmp_path / "fsm.sh"
    hook.write_text("#!/bin/sh\nprintf 'tok\\0'\n")
    hook.chmod(0o755)
    _git(upstream, "config", "core.fsmonitor", str(hook))
    _git(upstream, "config", "core.fsmonitorHookVersion", "2")
    _git(upstream, "status")
    _git(upstream, "status")
    target = upstream / "t" / "A.t"
    stat = target.stat()
    target.write_bytes(EDIT)
    os.utime(target, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    if _git_status_lines(upstream) != 0:
        pytest.skip("this git does not honour the fsmonitor hook here")
    for code, err in _modes(upstream, capsys):
        assert code == 2 and "differs from the pinned tree (1: t/A.t)" in err


@pytest.mark.parametrize("var", ["GIT_WORK_TREE", "GIT_DIR", "GIT_INDEX_FILE"])
def test_inherited_git_env_cannot_redirect_the_check(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    var: str,
) -> None:
    """A dirty checkout stays dirty when ``GIT_*`` points git at a clean copy."""
    clean = tmp_path / "clean"
    subprocess.run(["cp", "-R", str(upstream), str(clean)], check=True)
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    (upstream / "t" / "A.t").write_text("1;\n")
    baseline, _, _ = _check(csv_path, upstream, capsys)
    assert baseline == 2
    target = {
        "GIT_WORK_TREE": clean,
        "GIT_DIR": clean / ".git",
        "GIT_INDEX_FILE": clean / ".git" / "index",
    }[var]
    monkeypatch.setenv(var, str(target))
    if var == "GIT_DIR":
        monkeypatch.setenv("GIT_WORK_TREE", str(clean))
    code, _, err = _check(csv_path, upstream, capsys)
    assert code == baseline and "dirty" in err, err


def test_git_env_drops_every_git_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Removing the scrub makes this fail: no ``GIT_*`` reaches a git child."""
    monkeypatch.setenv("GIT_WORK_TREE", "/elsewhere")
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", "/elsewhere/objects")
    monkeypatch.setenv("KEEP_ME", "1")
    env = cl._git_env()
    assert not [k for k in env if k.startswith("GIT_")] and env["KEEP_ME"] == "1"


def test_clean_run_ignores_a_bogus_git_env(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive control: a nonexistent ``GIT_WORK_TREE`` does not break a clean run."""
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    monkeypatch.setenv("GIT_WORK_TREE", "/nonexistent")
    monkeypatch.setenv("GIT_DIR", "/nonexistent/.git")
    code, out, err = _check(csv_path, upstream, capsys)
    assert code == 0, (out, err)


def _sparse_without_b(upstream: Path) -> None:
    """Sparse-checkout ``upstream`` without ``t/B.t``: clean, pinned, one file short."""
    _git(upstream, "sparse-checkout", "set", "--no-cone", "/t/*", "!/t/B.t")
    assert not (upstream / "t" / "B.t").exists()
    assert _git(upstream, "status", "--porcelain") == ""


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param(("--list",), id="list"),
        pytest.param(("--sweep",), id="sweep"),
        pytest.param(("--sweep", "--glob", "t/*.pm"), id="sweep-pm-unaffected"),
    ],
)
def test_sparse_checkout_list_and_sweep(
    upstream: Path, capsys: pytest.CaptureFixture[str], mode: tuple[str, ...]
) -> None:
    _sparse_without_b(upstream)
    code = cl.main(["--upstream", str(upstream), *mode])
    _, err = capsys.readouterr()
    if "t/*.pm" in mode:
        assert code == 0, err  # t/Support.pm is present: that glob is complete
    else:
        assert code == 2 and "missing 1 (t/B.t)" in err, err


def test_sparse_checkout_cannot_be_measured(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A CSV without B.t's rows passes coverage on a sparse checkout lacking B.t."""
    records = [r for r in _records(upstream) if r[0] != "t/B.t"]
    csv_path = _write(tmp_path / "noB.csv", records)
    _sparse_without_b(upstream)
    code, out, err = _check(csv_path, upstream, capsys)
    assert code == 2, out
    assert "differ from the pinned tree (missing 1 (t/B.t))" in err
    _git(upstream, "sparse-checkout", "disable")
    assert _check(csv_path, upstream, capsys)[0] == 1  # full tree: B.t is missing


def test_untracked_ignored_file_is_extra(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A file on disk that is not in the pinned tree (ignored, so not dirty)."""
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    (upstream / ".git" / "info" / "exclude").write_text("t/C.t\n")
    (upstream / "t" / "C.t").write_text("ok(1);\n")
    code, _, err = _check(csv_path, upstream, capsys)
    assert code == 2 and "extra 1 (t/C.t)" in err, err


@pytest.mark.parametrize(
    "mode",
    [pytest.param((), id="default"), pytest.param(("--list",), id="list"),
     pytest.param(("--sweep",), id="sweep")],
)  # fmt: skip
def test_subdirectory_upstream_cannot_be_measured(
    upstream: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mode: tuple[str, ...],
) -> None:
    """``--upstream root/t`` passes the pin and clean checks; it must still refuse."""
    csv_path = _write(tmp_path / "hdr.csv", [list(cl.COLUMNS)])
    csv_args = () if mode else ("--csv", str(csv_path))
    code = cl.main(["--upstream", str(upstream / "t"), *csv_args, *mode])
    out, err = capsys.readouterr()
    assert code == 2, (out, err)
    assert "not the top level of the work tree (prefix 't/')" in err, err


def test_file_set_reads_the_tree_from_its_top_level(upstream: Path) -> None:
    """Without ``--full-tree`` a subdirectory would see a tree with no ``t/*.t``."""
    with pytest.raises(cl.CannotMeasure, match=r"missing 2 \(t/A\.t, t/B\.t\)"):
        cl.verify_file_set(upstream / "t", cl.DEFAULT_GLOB)


def test_header_only_csv_on_the_root_is_a_violation(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv_path = _write(tmp_path / "hdr.csv", [list(cl.COLUMNS)])
    code, out, _ = _check(csv_path, upstream, capsys)
    assert code == 1 and out.endswith("rows 0, files 0, missing 7, orphan 0\n"), out


@pytest.mark.parametrize(
    "mode",
    [pytest.param((), id="default"), pytest.param(("--list",), id="list"),
     pytest.param(("--sweep",), id="sweep")],
)  # fmt: skip
def test_pinned_tree_without_matching_files_cannot_be_measured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mode: tuple[str, ...],
) -> None:
    """A clean pinned root whose tree has no ``t/*.t`` must not pass vacuously."""
    root = tmp_path / "empty"
    (root / "lib").mkdir(parents=True)
    (root / "lib" / "X.pm").write_text("1;\n")
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(root, "commit", "-q", "-m", "no tests")
    monkeypatch.setattr(cl, "PINNED_COMMIT", _git(root, "rev-parse", "HEAD"))
    csv_path = _write(tmp_path / "hdr.csv", [list(cl.COLUMNS)])
    csv_args = () if mode else ("--csv", str(csv_path))
    code = cl.main(["--upstream", str(root), *csv_args, *mode])
    out, err = capsys.readouterr()
    assert code == 2, (out, err)
    assert "the pinned tree has no file matching 't/*.t'" in err, err


def test_glob_matches_like_path_glob() -> None:
    assert cl.glob_matches("t/A.t", "t/*.t")
    assert not cl.glob_matches("t/sub/A.t", "t/*.t")
    assert not cl.glob_matches("x/t/A.t", "t/*.t")
    assert not cl.glob_matches("t/A.pm", "t/*.t")
    with pytest.raises(cl.CannotMeasure):
        cl.glob_matches("t/A.t", "t/**/*.t")


def test_indented_assertions_are_rows(tmp_path: Path) -> None:
    """``ROW_RE`` allows leading whitespace (spaces and tabs) before the name."""
    (tmp_path / "t").mkdir()
    path = tmp_path / "t" / "I.t"
    path.write_text("SKIP: {\n    ok(1);\n\tis(1, 1);\n}\nis_deeply([], []);\n")
    found = [(a.n, a.perl_line, a.perl_kind) for a in cl.enumerate_file(tmp_path, path)]
    assert found == [(1, 2, "ok"), (2, 3, "is"), (3, 5, "is_deeply")]


def test_missing_upstream_or_csv_cannot_be_measured(
    upstream: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    csv_path = _write(tmp_path / "d.csv", _records(upstream))
    assert _check(csv_path, tmp_path / "nonexistent", capsys)[0] == 2
    assert _check(tmp_path / "absent.csv", upstream, capsys)[0] == 2
    plain = tmp_path / "plain"
    plain.mkdir()
    assert _check(csv_path, plain, capsys)[0] == 2
    (tmp_path / "latin1.csv").write_bytes(b"\xff\n")
    assert _check(tmp_path / "latin1.csv", upstream, capsys)[0] == 2


# --- sweep --------------------------------------------------------------------------


def _sweep_dir(tmp_path: Path, extra: str) -> Path:
    """An unpinned directory with ``t/A.t``, ``t/B.t``; ``extra`` appended to A.t."""
    root = tmp_path / "sweep"
    (root / "t").mkdir(parents=True)
    (root / "t" / "A.t").write_text(A_T + extra)
    (root / "t" / "B.t").write_text(B_T)
    return root


def _sweep(root: Path, capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    code = cl.main(["--sweep-dir", str(root)])
    out, err = capsys.readouterr()
    return code, out, err


def test_sweep_base_explains_everything(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, err = _sweep(_sweep_dir(tmp_path, ""), capsys)
    assert (code, out) == (0, "")
    assert err == (
        "sweep (unpinned): files 2, counted 7; capture warning/warnings 2, "
        "comparator 1, done_testing 2, import list 1, use/no warnings 1; "
        "unexplained 0\n"
    )


@pytest.mark.parametrize(
    ("extra", "report"),
    [
        pytest.param('1; lives_ok { 1 } "x";\n', "t/A.t:15: uncounted `lives_ok`: "
                     '1; lives_ok { 1 } "x";', id="15 mid-line lives_ok"),
        pytest.param("had_no_warnings();\n", "t/A.t:15: uncounted `had_no_warnings`: "
                     "had_no_warnings();", id="15 line-start had_no_warnings"),
        pytest.param("warning(1);\n", "t/A.t:15: uncounted `warning`: warning(1);",
                     id="warning call form"),
        pytest.param("my $x = 1 + fail;\n", "uncounted `fail`", id="mid-line fail"),
        pytest.param("re('x');\n", "uncounted `re`", id="comparator as statement"),
        pytest.param("ok(1) && is(1, 1);\n", "uncounted `is`",
                     id="second assertion on a counted line"),
    ],
)  # fmt: skip
def test_sweep_reports_uncounted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], extra: str, report: str
) -> None:
    code, out, err = _sweep(_sweep_dir(tmp_path, extra), capsys)
    assert code == 1
    assert report in out and len(out.splitlines()) == 1
    assert "unexplained 1" in err


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param("warning { 1 };\n", id="bare warning capture"),
        pytest.param("my @w = warnings { 1 };\n", id="warnings capture after ="),
        pytest.param("# lives_ok { 1 };\n", id="comment"),
        pytest.param("my $s = 'lives_ok { 1 }';\n", id="string"),
        pytest.param("my $s = q{had_no_warnings()};\n", id="q literal"),
        pytest.param("my $r = qr/ok\\(fail\\)/;\n", id="regex literal"),
        pytest.param("$x =~ s/is/isnt/g;\n", id="substitution"),
        pytest.param("my $n = $#fail;\n", id="array length"),
        pytest.param("$obj->is(1);\n", id="method call"),
    ],
)
def test_sweep_explains_or_ignores(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], extra: str
) -> None:
    code, out, _ = _sweep(_sweep_dir(tmp_path, extra), capsys)
    assert (code, out) == (0, "")


def test_bare_warning_is_tallied_as_capture(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _, _, err = _sweep(_sweep_dir(tmp_path, "warning { 1 };\n"), capsys)
    assert "capture warning/warnings 3" in err


def test_pinned_sweep_over_support_modules(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cl.main(["--upstream", str(upstream), "--sweep", "--glob", "t/*.pm"]) == 0
    out, err = capsys.readouterr()
    assert out == "" and f"pinned {cl.PINNED_COMMIT}" in err


def test_pinned_sweep_refuses_a_dirty_tree(
    upstream: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with (upstream / "t" / "A.t").open("a") as fh:
        fh.write("had_no_warnings();\n")
    assert cl.main(["--upstream", str(upstream), "--sweep"]) == 2


def test_strip_perl_keeps_positions() -> None:
    src = "ok(1, 'a # b'); # c\nmy $x = qq{\n  is\n}; is(2);\n"
    code = cl.strip_perl(src)
    assert len(code) == len(src) and code.count("\n") == src.count("\n")
    assert code.split("\n")[3].endswith("is(2);")
    assert "a # b" not in code and "# c" not in code and " is\n" not in code


def test_canonical_matches_the_109_format() -> None:
    records = [["a", "b,c", 'd"e', "f g"]]
    expected = io.StringIO(newline="")
    csv.writer(expected, lineterminator="\n").writerows(records)
    assert cl.canonical(records) == expected.getvalue() == 'a,"b,c","d""e",f g\n'
