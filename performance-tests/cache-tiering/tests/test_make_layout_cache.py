import json
import os
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import make_layout_cache as mlc  # noqa: E402


@pytest.fixture
def source(tmp_path: Path) -> Path:
    src = tmp_path / "116_GRCh38_merged"
    for d in ("transcript", "exon", "variation"):
        (src / d).mkdir(parents=True)
    (src / "reference_policy.json").write_text("{}")
    (src / "transcript" / "chr22.parquet").write_text("t22")
    for c in ("chr21", "chr22"):
        (src / "variation" / f"{c}.parquet").write_text(f"v-{c}")
    return src


@pytest.fixture
def stub_relayout(tmp_path: Path) -> Path:
    """Copies --input to --output and prints the JSON line relayout prints."""
    stub = tmp_path / "relayout"
    stub.write_text(
        "#!/usr/bin/env python3\n"
        "import shutil, sys, json\n"
        "a = sys.argv\n"
        "src, dst, layout = a[a.index('--input') + 1], a[a.index('--output') + 1], a[a.index('--layout') + 1]\n"
        "shutil.copyfile(src, dst)\n"
        "open(dst, 'a').write('-' + layout)\n"
        "print(json.dumps({'rows': 1, 'runs': 1 if layout == 'sorted' else 2, 'start_pages': 1, 'bytes': 5}))\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return stub


def test_only_requested_contigs_rewritten(tmp_path, source, stub_relayout):
    cache = mlc.make_layout_cache(
        source, tmp_path / "out", "sorted", ["chr22"], stub_relayout
    )
    var = cache / "variation"
    assert not (var / "chr22.parquet").is_symlink()
    assert (var / "chr22.parquet").read_text() == "v-chr22-sorted"
    assert (var / "chr21.parquet").is_symlink()
    assert os.readlink(var / "chr21.parquet") == str(
        source / "variation" / "chr21.parquet"
    )


def test_other_entries_are_symlinks(tmp_path, source, stub_relayout):
    cache = mlc.make_layout_cache(
        source, tmp_path / "out", "tiered", ["chr22"], stub_relayout
    )
    assert cache == tmp_path / "out" / "tiered" / "116_GRCh38_merged"
    for name in ("transcript", "exon", "reference_policy.json"):
        assert (cache / name).is_symlink()
        assert os.readlink(cache / name) == str(source / name)
    assert (cache / "variation").is_dir() and not (cache / "variation").is_symlink()


def test_refuses_existing_dest(tmp_path, source, stub_relayout):
    mlc.make_layout_cache(source, tmp_path / "out", "sorted", ["chr22"], stub_relayout)
    with pytest.raises(FileExistsError):
        mlc.make_layout_cache(
            source, tmp_path / "out", "sorted", ["chr22"], stub_relayout
        )


def test_layout_json_written(tmp_path, source, stub_relayout):
    mlc.make_layout_cache(source, tmp_path / "out", "sorted", ["chr22"], stub_relayout)
    info = json.loads((tmp_path / "out" / "sorted" / "layout.json").read_text())
    assert info["layout"] == "sorted"
    assert info["contigs"] == ["chr22"]
    assert info["source"] == str(source)
    assert info["relayout"]["chr22"] == {
        "rows": 1,
        "runs": 1,
        "start_pages": 1,
        "bytes": 5,
    }


def test_refuses_when_disk_low(tmp_path, source, stub_relayout, monkeypatch):
    monkeypatch.setattr(mlc, "free_bytes", lambda path: 10 * 2**30)
    with pytest.raises(RuntimeError, match="free"):
        mlc.make_layout_cache(
            source, tmp_path / "out", "sorted", ["chr22"], stub_relayout
        )
    assert not (tmp_path / "out" / "sorted").exists()


def test_unknown_layout_rejected(tmp_path, source, stub_relayout):
    with pytest.raises(ValueError):
        mlc.make_layout_cache(
            source, tmp_path / "out", "random", ["chr22"], stub_relayout
        )
