"""Tests for ``tools/check_vep_version`` and ``tools/vep_pin.py`` (#239).

The script has no ``.py`` suffix, so it is loaded by path, like
``tools/build_test_index`` in ``test_build_test_index.py``. Each test builds a
throw-away git repository holding a copy of the real pin and synthetic
``tests/data/*/test.toml`` files. This file is excluded from the checker's own
grep by name: it needs the legacy VEP 116.0 literals as negative fixtures.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import ModuleType
from typing import Final

import pytest

from vep_pin import PIN_FILE, PinError, VepPin, load_pin

TOOLS: Final[Path] = Path(__file__).resolve().parent
SCRIPT: Final[Path] = TOOLS / "check_vep_version"
REPO: Final[Path] = TOOLS.parent

LEGACY_IMAGE: Final[str] = (
    "ensemblorg/ensembl-vep@sha256:"
    "f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11"
)
LEGACY_COMMIT: Final[str] = "57ea5c52340acc1f156267f810ad162e26597082"
UP: Final[str] = "https://github.com/Ensembl/ensembl-vep/blob"


def _load() -> ModuleType:
    """Import the extension-less script as a module."""
    loader = SourceFileLoader("check_vep_version", str(SCRIPT))
    spec = spec_from_loader(loader.name, loader)
    assert spec is not None
    module = module_from_spec(spec)
    sys.modules[loader.name] = module  # dataclasses resolve the module by name
    loader.exec_module(module)
    return module


cvv: Final[ModuleType] = _load()
PIN: Final[VepPin] = load_pin()


def toml_text(
    *,
    image: str = PIN.pinned_image,
    vep_test_ref: str = PIN.upstream_tag,
) -> str:
    """A minimal ``test.toml`` with the keys the checker reads."""
    return (
        'name = "x"\n\n[origin]\n'
        f'vep_test = "{UP}/{vep_test_ref}/t/Runner.t#L1-L2"\n'
        f'[vep]\nimage = "{image}"\n'
    )


LEGACY_TOML: Final[str] = toml_text(image=LEGACY_IMAGE, vep_test_ref="release/116.0")


def write_test(root: Path, name: str, text: str) -> Path:
    """Write ``tests/data/<name>/test.toml`` under ``root``."""
    path = root / "tests" / "data" / name / "test.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A git work tree with the real pin and one pinned data-test."""
    root = tmp_path / "repo"
    (root / "tools").mkdir(parents=True)
    shutil.copy(PIN_FILE, root / "tools" / "vep_pin.toml")
    write_test(root, "good", toml_text())
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    return root


def legacy_list(root: Path, count: int = cvv.LEGACY_COUNT) -> list[str]:
    """Create ``count`` legacy data-tests and an allow-list naming them."""
    names = [f"legacy_{i:02d}" for i in range(count)]
    for name in names:
        write_test(root, name, LEGACY_TOML)
    allow = root / "tools" / "vep_pin_legacy_allowlist.txt"
    allow.write_text("".join(f"{n}\n" for n in names), encoding="utf-8")
    return names


def exit_code(root: Path) -> int:
    """Run the checker's ``main`` on ``root``."""
    return cvv.main(["--root", str(root)])


def test_pinned_repo_passes(repo: Path) -> None:
    """A repo whose only data-test uses the pin passes, with no allow-list."""
    assert exit_code(repo) == 0


def test_vep_test_at_commit_rejected(repo: Path) -> None:
    """The public test link uses the readable release tag only."""
    write_test(repo, "good", toml_text(vep_test_ref=PIN.upstream_commit))
    assert exit_code(repo) == 1


def test_legacy_digest_fails(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """#239 AC3: a ``test.toml`` with the 116.0 image digest makes the check fail."""
    write_test(repo, "good", toml_text(image=LEGACY_IMAGE))
    assert exit_code(repo) == 1
    assert "[vep] image" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"vep_test_ref": "release/116.0"}, "vep_test"),
        ({"vep_test_ref": "master"}, "vep_test"),
        ({"image": "ensemblorg/ensembl-vep:release_116.2"}, "[vep] image"),
    ],
)
def test_each_key_is_checked(
    repo: Path, capsys: pytest.CaptureFixture[str], kwargs: dict[str, str], needle: str
) -> None:
    """Each pinned key, changed alone, fails the check."""
    write_test(repo, "good", toml_text(**kwargs))
    assert exit_code(repo) == 1
    assert needle in capsys.readouterr().err


def _as_tests(text: str, second_tag: str = PIN.upstream_tag) -> str:
    """Turn ``toml_text()``'s ``[origin]`` into two ``[[tests]]`` tables (#238)."""
    head, rest = text.split("[origin]\n", 1)
    links, vep = rest.split("\n[vep]", 1)
    second = links.replace(PIN.upstream_tag, second_tag)
    return (
        f'{head}[vep]{vep}\n[[tests]]\nid = "x"\n{links}\n[[tests]]\nid = "y"\n{second}'
    )


def test_test_links_pinned_pass(repo: Path) -> None:
    """#238: a multi-test dir whose every test is pinned passes."""
    write_test(repo, "good", _as_tests(toml_text()))
    assert exit_code(repo) == 0


def test_test_link_unpinned_fails(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """#238: one ``[[tests]]`` at another commit fails, naming its id."""
    write_test(repo, "good", _as_tests(toml_text(), "f" * 40))
    assert exit_code(repo) == 1
    err = capsys.readouterr().err
    assert "[[tests]] 'y' vep_test" in err
    assert "'x'" not in err


def test_legacy_literal_in_comment_fails(repo: Path) -> None:
    """A legacy literal anywhere in a non-listed ``test.toml`` fails (AC7c)."""
    write_test(repo, "good", toml_text() + "# from release/116.0\n")
    assert exit_code(repo) == 1


def test_full_allowlist_passes(repo: Path) -> None:
    """Exactly 16 legacy tests on the list pass (AC7a-c)."""
    legacy_list(repo)
    assert exit_code(repo) == 0


def test_seventeenth_entry_fails(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC7d: a 17th entry (here the pinned test) fails."""
    legacy_list(repo)
    allow = repo / "tools" / "vep_pin_legacy_allowlist.txt"
    allow.write_text(allow.read_text() + "good\n", encoding="utf-8")
    assert exit_code(repo) == 1
    err = capsys.readouterr().err
    assert f"has {cvv.LEGACY_COUNT + 1} entries" in err
    assert "'good' no longer records the legacy image digest" in err


def test_reblessed_entry_fails(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """AC7d: a listed directory whose test.toml names the 116.2 digest fails."""
    names = legacy_list(repo)
    write_test(repo, names[3], toml_text())
    assert exit_code(repo) == 1
    assert f"{names[3]!r} no longer records" in capsys.readouterr().err


def test_unlisted_legacy_dir_fails(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """AC7d: a non-listed directory with the legacy digest fails."""
    names = legacy_list(repo)
    allow = repo / "tools" / "vep_pin_legacy_allowlist.txt"
    allow.write_text("".join(f"{n}\n" for n in names[1:]), encoding="utf-8")
    assert exit_code(repo) == 1
    err = capsys.readouterr().err
    assert f"has {cvv.LEGACY_COUNT - 1} entries" in err
    assert f"tests/data/{names[0]}/test.toml" in err


def test_missing_allowlist_is_empty(repo: Path) -> None:
    """A missing list is an empty list: legacy tests then fail (the #237 end state)."""
    legacy_list(repo)
    (repo / "tools" / "vep_pin_legacy_allowlist.txt").unlink()
    assert exit_code(repo) == 1


def test_nonexistent_entry_fails(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A listed name that is not a data-test directory fails."""
    names = legacy_list(repo)
    shutil.rmtree(repo / "tests" / "data" / names[0])
    assert exit_code(repo) == 1
    assert "is not a data-test directory" in capsys.readouterr().err


@pytest.mark.parametrize(
    "literal",
    ["ensemblorg/ensembl-vep:release_116.0", "blob/release/116.0/t", LEGACY_COMMIT[:8]],
)
def test_repo_grep_finds_legacy_literal(
    repo: Path, capsys: pytest.CaptureFixture[str], literal: str
) -> None:
    """A legacy literal in any non-excluded file fails (AC1), untracked included."""
    (repo / "README.md").write_text(f"see {literal}\n", encoding="utf-8")
    assert exit_code(repo) == 1
    assert "README.md:1: legacy literal" in capsys.readouterr().err


@pytest.mark.parametrize("excluded", sorted(cvv.GREP_EXCLUDES))
def test_repo_grep_excludes(repo: Path, excluded: str) -> None:
    """The ledger axis, the index, porting notes and the checker itself are skipped."""
    path = repo / excluded.replace("**", "notes.md")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("release/116.0 57ea5c52\n", encoding="utf-8")
    assert exit_code(repo) == 0


def test_bad_pin_exits_2(repo: Path) -> None:
    """An unreadable pin is a setup error, not a violation."""
    (repo / "tools" / "vep_pin.toml").write_text("[vep]\n", encoding="utf-8")
    assert exit_code(repo) == 2


def test_real_repo_passes() -> None:
    """#239 AC2: the checker passes on this repository."""
    proc = subprocess.run([str(SCRIPT)], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr


def test_pin_values() -> None:
    """The pin parses and its derived values have the recorded shape."""
    assert PIN.image_repo == "ensemblorg/ensembl-vep"
    assert PIN.image_tag == f"{PIN.image_repo}:release_116.2"
    assert PIN.upstream_tag == "release/116.2"
    assert PIN.cache_version == "116"
    assert PIN.pinned_image.startswith(f"{PIN.image_repo}@sha256:")


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("image_digest", "sha256:abc"),
        ("upstream_commit", "2cb0bbe2"),
        ("image_tag", "ensemblorg/ensembl-vep"),
        ("cache_version", "116.2"),
        ("upstream_tag", ""),
    ],
)
def test_pin_rejects_malformed(key: str, value: str) -> None:
    """Each malformed pin value is rejected by name."""
    text = PIN_FILE.read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if ln.startswith(f"{key} ="))
    with pytest.raises(PinError, match=key):
        VepPin.parse(text.replace(line, f'{key} = "{value}"'))
