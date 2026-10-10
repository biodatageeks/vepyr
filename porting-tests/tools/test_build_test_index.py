"""Tests for the ``tools/build_test_index`` script (#92).

The script has no ``.py`` suffix, so it is loaded by path, like
``tools/normalize_input`` in ``test_normalize_input.py``.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
import sys
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

TOOLS: Final[Path] = Path(__file__).resolve().parent
SCRIPT: Final[Path] = TOOLS / "build_test_index"
FIXTURES: Final[Path] = TOOLS / "fixtures" / "build_test_index"
REPO: Final[Path] = TOOLS.parent
HEADER: Final[str] = (
    "dir,id,description,skip_reason,vep_test,cache_source,vep_cache,fasta_source,"
    "required_contigs,vepyr_runs,body_md5\n"
)


def _load() -> ModuleType:
    """Import the extension-less script as a module."""
    loader = SourceFileLoader("build_test_index", str(SCRIPT))
    spec = spec_from_loader(loader.name, loader)
    assert spec is not None
    module = module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolve the module by name
    loader.exec_module(module)
    return module


bti: Final[ModuleType] = _load()


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A writable copy of the fixture root."""
    return Path(shutil.copytree(FIXTURES, tmp_path / "root"))


def _run(*args: str | Path) -> int:
    """Run ``main`` in-process and return its exit code."""
    return bti.main([str(a) for a in args])


def test_fixture_rows_and_format(root: Path, tmp_path: Path) -> None:
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    raw = out.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), "no BOM"
    assert b"\r" not in raw, "LF only"
    text = raw.decode("utf-8")
    assert text.startswith(HEADER)
    rows = list(csv.DictReader(text.splitlines()))
    assert [r["dir"] for r in rows] == ["alpha_full", "beta_minimal"]
    assert [r["id"] for r in rows] == ["alpha_full", "beta_minimal"]
    alpha, beta = rows
    assert alpha["description"] == 'Synthetic test, with a comma and a "quoted" word.'
    assert alpha["vepyr_runs"] == "2"
    assert alpha["vep_test"].startswith("https://")
    assert alpha["vep_cache"].startswith("https://")
    assert alpha["required_contigs"] == "chr21;chr22"
    assert beta["vepyr_runs"] == "0"
    assert alpha["skip_reason"] == beta["skip_reason"] == ""


def test_skip_reason_is_retained(root: Path, tmp_path: Path) -> None:
    path = root / "alpha_full" / "test.toml"
    path.write_text(
        'skip_reason = "Unsupported symbolic deletion"\n' + path.read_text()
    )
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    rows = list(csv.DictReader(out.read_text().splitlines()))
    assert len(rows) == 2
    assert rows[0]["skip_reason"] == "Unsupported symbolic deletion"


@pytest.mark.parametrize("value", ['""', '" "', "false"])
def test_invalid_skip_reason_rejected(root: Path, tmp_path: Path, value: str) -> None:
    path = root / "alpha_full" / "test.toml"
    path.write_text(f"skip_reason = {value}\n" + path.read_text())
    assert _run("--root", root, "--out", tmp_path / "i.csv") == 2


def test_rows_sorted_by_directory_name(root: Path, tmp_path: Path) -> None:
    shutil.copytree(root / "alpha_full", root / "0_first")
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    names = [line.split(",", 1)[0] for line in out.read_text().splitlines()[1:]]
    assert names == [
        "alpha_full",
        "alpha_full",
        "beta_minimal",
    ]  # 0_first holds name=alpha_full


def test_check_up_to_date_then_stale(
    root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    assert _run("--root", root, "--out", out, "--check") == 0
    out.write_text(out.read_text().replace("fedcba98", "00000000"))
    assert _run("--root", root, "--out", out, "--check") == 1
    assert "beta_minimal" in capsys.readouterr().err


def test_check_does_not_write(root: Path, tmp_path: Path) -> None:
    out = tmp_path / "absent.csv"
    assert _run("--root", root, "--out", out, "--check") == 1
    assert not out.exists()


def test_check_extra_row_is_stale(root: Path, tmp_path: Path) -> None:
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    shutil.rmtree(root / "beta_minimal")
    assert _run("--root", root, "--out", out, "--check") == 1


@pytest.mark.parametrize(
    ("table_line", "key"),
    [
        ("body_md5", "compare.body_md5"),
        ("vep_test", "origin.vep_test"),
        ("required_contigs", "vepyr.required_contigs"),
        ("name", "name"),
    ],
)
def test_missing_required_key_exits_2(
    root: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    table_line: str,
    key: str,
) -> None:
    toml = root / "beta_minimal" / "test.toml"
    toml.write_text(
        "".join(
            ln
            for ln in toml.read_text().splitlines(True)
            if not ln.startswith(table_line)
        )
    )
    assert _run("--root", root, "--out", tmp_path / "k.csv") == 2
    err = capsys.readouterr().err
    assert key in err and str(toml) in err
    assert not (tmp_path / "k.csv").exists()


def test_missing_test_toml_exits_2(
    root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (root / "beta_minimal" / "test.toml").unlink()
    assert _run("--root", root, "--out", tmp_path / "k.csv", "--check") == 2
    assert "missing test.toml" in capsys.readouterr().err


def test_wrong_type_exits_2(root: Path, tmp_path: Path) -> None:
    toml = root / "beta_minimal" / "test.toml"
    toml.write_text(toml.read_text().replace('["chr1"]', '"chr1"'))
    assert _run("--root", root, "--out", tmp_path / "k.csv") == 2


def test_empty_root_is_header_only(tmp_path: Path) -> None:
    (tmp_path / "e").mkdir()
    out = tmp_path / "e.csv"
    assert _run("--root", tmp_path / "e", "--out", out) == 0
    assert out.read_text() == HEADER
    assert _run("--root", tmp_path / "e", "--out", out, "--check") == 0


def test_committed_index_is_current() -> None:
    """The committed ``tests/INDEX.csv`` matches ``tests/data`` (what CI checks)."""
    assert _run("--check") == 0


def test_script_runs_as_executable(root: Path, tmp_path: Path) -> None:
    out = tmp_path / "i.csv"
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), "--out", str(out)],
        check=False,
    )
    assert proc.returncode == 0
    assert out.read_text().startswith(HEADER)


def test_test_tables_give_one_row_each(root: Path, tmp_path: Path) -> None:
    """#238: a [[tests]] directory gives one row per table, in file order."""
    beta = (root / "beta_minimal" / "test.toml").read_text()
    start = beta.index("[origin]")
    end = beta.index("\n[", start + 1) + 1
    commit = "a" * 40
    props = "".join(
        f"""[[tests]]
id = "{pid}"
description = "NamedTest {pid}."
vep_test = "https://github.com/o/r/blob/{commit}/t/{pid}.t"
{extra}
"""
        for pid, extra in (("gamma", ""), ("gamma_b", ""))
    )
    gamma = root / "gamma"
    gamma.mkdir()
    text = beta[:start] + beta[end:]
    text = text.replace('name = "beta_minimal"', 'name = "gamma"')
    (gamma / "test.toml").write_text(text + "\n" + props)
    out = tmp_path / "i.csv"
    assert _run("--root", root, "--out", out) == 0
    rows = list(csv.DictReader(out.read_text().splitlines()))
    got = [(r["dir"], r["id"]) for r in rows[2:]]
    assert got == [("gamma", "gamma"), ("gamma", "gamma_b")]
    assert rows[3]["description"] == "NamedTest gamma_b."
    assert rows[3]["vep_test"].endswith("/t/gamma_b.t")
    assert rows[2]["body_md5"] == rows[3]["body_md5"]
