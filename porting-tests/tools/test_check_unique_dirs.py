"""Tests for ``tools/check_unique_dirs`` and ``tools/merge_duplicate_dirs`` (#238).

Fixtures are copies of the synthetic deduplication case
(``tools/fixtures/check_unique_dirs/case``) in ``tmp_path``.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Final

import pytest
from check_unique_dirs import duplicate_groups, key_of, main
from check_unique_dirs import test_dirs as list_test_dirs
from merge_duplicate_dirs import main as merge_main
from merge_duplicate_dirs import render_test_toml

REPO: Final[Path] = Path(__file__).resolve().parent.parent
TOOL: Final[Path] = REPO / "tools" / "check_unique_dirs"
CASE: Final[Path] = REPO / "tools" / "fixtures" / "check_unique_dirs" / "case"


def _copy(root: Path, name: str, *, toml_edit: tuple[str, str] | None = None) -> Path:
    """Copy the self-test case to ``root/name`` with ``name`` set (and one edit)."""
    d = Path(shutil.copytree(CASE, root / name))
    text = (d / "test.toml").read_text().replace('name = "case"', f'name = "{name}"')
    if toml_edit is not None:
        text = text.replace(*toml_edit)
    (d / "test.toml").write_text(text)
    return d


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_copied_dir_is_duplicate(tmp_path: Path) -> None:
    """AC2: a directory copied under another name is a duplicate (exit 1)."""
    root = tmp_path / "data"
    _copy(root, "case")
    single = subprocess.run([str(TOOL), str(root)], capture_output=True, text=True)
    assert single.returncode == 0, single.stdout + single.stderr
    _copy(root, "case_copy")
    run = subprocess.run([str(TOOL), str(root)], capture_output=True, text=True)
    assert run.returncode == 1, run.stdout + run.stderr
    assert "DUPLICATE 2 case case_copy" in run.stdout


def test_header_only_difference_is_still_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _copy(root, "a")
    b = _copy(root, "b")
    oracle = b / "expected_output.vcf"
    oracle.write_text("##extra header\n" + oracle.read_text())
    assert main([str(root)]) == 1


@pytest.mark.parametrize(
    ("label", "edit"),
    [
        ("vepyr table", ('required_contigs = ["chr1"]', 'required_contigs = ["chr2"]')),
        ("vep command", ("hand-written", "hand-made")),
        (
            "vepyr_run list",
            ("\n[compare]\n", "\n[[vepyr_run]]\nbuffer_size = 7\n\n[compare]\n"),
        ),
    ],
)
def test_config_difference_is_not_duplicate(
    tmp_path: Path, label: str, edit: tuple[str, str]
) -> None:
    root = tmp_path / "data"
    _copy(root, "a")
    _copy(root, "b", toml_edit=edit)
    assert main([str(root)]) == 0, label


def test_body_difference_is_not_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _copy(root, "a")
    b = _copy(root, "b")
    with (b / "expected_output.vcf").open("a") as fh:
        fh.write("chr1\t99\t.\tA\tC\t.\t.\t.\n")
    assert main([str(root)]) == 0


def test_empty_root_and_unreadable_dir_fail(tmp_path: Path) -> None:
    assert main([str(tmp_path)]) == 1
    assert main([str(tmp_path / "absent")]) == 1
    d = _copy(tmp_path, "a")
    (d / "input.vcf").unlink()
    assert main([str(tmp_path)]) == 1


def test_groups_sorted_by_size_then_name(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for name in ("z1", "z2"):
        _copy(root, name, toml_edit=("chr1", "chr9"))
    for name in ("b", "a", "c"):
        _copy(root, name)
    groups = duplicate_groups(list_test_dirs(root))
    assert [[d.name for d in g] for g in groups] == [["a", "b", "c"], ["z1", "z2"]]
    assert key_of(root / "a") == key_of(root / "c")


# ----------------------------------------------------------------------------
# merge_duplicate_dirs
# ----------------------------------------------------------------------------


def test_merge_keeps_first_dir_and_every_test(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for name in ("m_b", "m_a", "m_c"):
        _copy(root, name, toml_edit=("#L244-L292", f"#L{len(name)}"))
    _copy(root, "lone", toml_edit=("chr1", "chr9"))
    kept = root / "m_a"
    before = {f: _sha(kept / f) for f in ("input.vcf", "expected_output.vcf")}
    index = tmp_path / "INDEX.csv"
    assert merge_main([str(root), "--index", str(index)]) == 0
    assert sorted(d.name for d in root.iterdir()) == ["lone", "m_a"]
    assert {f: _sha(kept / f) for f in before} == before
    doc = tomllib.loads((kept / "test.toml").read_text())
    assert "origin" not in doc
    assert [p["id"] for p in doc["tests"]] == ["m_a", "m_b", "m_c"]
    assert doc["tests"][1]["vep_test"].endswith("#L3")
    assert "issue" not in doc["tests"][1]
    assert doc["tests"][0]["description"].startswith("Synthetic fixture")
    assert doc["name"] == "m_a"
    assert main([str(root)]) == 0
    rows = index.read_text().splitlines()
    assert [r.split(",")[:2] for r in rows[1:]] == [
        ["lone", "lone"],
        ["m_a", "m_a"],
        ["m_a", "m_b"],
        ["m_a", "m_c"],
    ]
    # Idempotent: a second run changes nothing.
    text = (kept / "test.toml").read_text()
    assert merge_main([str(root)]) == 0
    assert (kept / "test.toml").read_text() == text


def test_merge_into_an_already_merged_dir(tmp_path: Path) -> None:
    """Re-run after more duplicates appear (e.g. after #237): tests carry over."""
    root = tmp_path / "data"
    for name in ("a", "b"):
        _copy(root, name)
    assert merge_main([str(root)]) == 0
    _copy(root, "c")
    assert merge_main([str(root)]) == 0
    doc = tomllib.loads((root / "a" / "test.toml").read_text())
    assert [p["id"] for p in doc["tests"]] == ["a", "b", "c"]


def test_merge_excluded_flavour_is_left_alone(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for name in ("a", "b"):
        _copy(root, name)  # flavour = "ensembl"
    assert merge_main([str(root), "--exclude-flavour", "ensembl"]) == 0
    assert sorted(d.name for d in root.iterdir()) == ["a", "b"]
    assert main([str(root)]) == 1


def test_merge_dry_run_writes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for name in ("a", "b"):
        _copy(root, name)
    text = (root / "a" / "test.toml").read_text()
    assert merge_main([str(root), "--dry-run"]) == 0
    assert sorted(d.name for d in root.iterdir()) == ["a", "b"]
    assert (root / "a" / "test.toml").read_text() == text


def test_merge_refuses_colliding_ids(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for name in ("a", "b"):
        _copy(root, name)
    assert merge_main([str(root)]) == 0
    c = _copy(root, "c")
    props = (root / "a" / "test.toml").read_text().split("[[tests]]", 1)[1]
    text = (c / "test.toml").read_text()
    start = text.index("[origin]")
    end = text.index("\n[", start + 1) + 1
    (c / "test.toml").write_text(text[:start] + text[end:] + "\n[[tests]]" + props)
    assert merge_main([str(root)]) == 1
    assert (root / "c").is_dir()


def test_render_keeps_other_lines_and_comments() -> None:
    text = (CASE / "test.toml").read_text()
    prop = {"id": "case", "description": "D.", "vep_test": "u", "issue": 1}
    out = render_test_toml(text, [prop], "Top.")
    assert "\n[origin]\n" not in out
    assert out.startswith(text.split('description = "')[0])
    assert 'description = "Top."\n' in out
    assert out.endswith(
        '[[tests]]\nid = "case"\ndescription = "D."\nvep_test = "u"\nissue = 1\n'
    )
    for table in ("\n[input]\n", "\n[vep]\n", "\n[vepyr]\n", "\n[compare]\n"):
        section = text.split(table, 1)[1].split("\n[", 1)[0]
        assert table + section in out


@pytest.mark.parametrize("other_reason", [None, "Another reason"])
def test_merge_refuses_conflicting_skip_policy(
    tmp_path: Path, other_reason: str | None
) -> None:
    root = tmp_path / "data"
    for name, reason in [("a", "Unsupported symbolic deletion"), ("b", other_reason)]:
        directory = _copy(root, name)
        path = directory / "test.toml"
        if reason is not None:
            path.write_text(f'skip_reason = "{reason}"\n' + path.read_text())
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert main([str(root)]) == 1  # Still a duplicate comparison.
    assert merge_main([str(root)]) == 1
    assert {p: p.read_bytes() for p in root.rglob("*") if p.is_file()} == before


def test_merge_preserves_shared_skip_reason(tmp_path: Path) -> None:
    for name in ["a", "b"]:
        directory = _copy(tmp_path, name)
        path = directory / "test.toml"
        path.write_text(
            'skip_reason = "Unsupported symbolic deletion"\n' + path.read_text()
        )
    assert merge_main([str(tmp_path)]) == 0
    doc = tomllib.loads((tmp_path / "a" / "test.toml").read_text())
    assert doc["skip_reason"] == "Unsupported symbolic deletion"
    assert len(doc["tests"]) == 2
