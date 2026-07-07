"""Plugin-cache builder manifest resolution + annotate plugin_cache_root plumbing."""

import subprocess
from pathlib import Path

import pytest

import vepyr

# A complete manifest the Rust builder can actually load (the resolution-only
# test above uses a stub `plugin_name = "demo"` line that won't deserialize).
_FULL_MANIFEST = """\
plugin_name = "demo"
coordinate_system = "1-based"
ingest_sql = "SELECT chrom, CAST(pos AS INT) AS start, CAST(pos AS INT) AS end, concat(ref, '/', alt) AS allele_string, CAST(score AS FLOAT) AS demo_score FROM plugin_demo_src"

[[source]]
provider = "csv"
path = "placeholder.tsv.gz"
  [source.csv]
  delimiter = "\\t"
  has_header = false
  schema = [
    { name = "chrom", type = "Utf8" },
    { name = "pos",   type = "Utf8" },
    { name = "ref",   type = "Utf8" },
    { name = "alt",   type = "Utf8" },
    { name = "score", type = "Utf8" },
  ]

[[value_columns]]
column = "demo_score"
csq_field = "DEMO"
type = "Float32"
"""

# A second `[[source]]` block making the manifest multi-part.
_SECOND_SOURCE = """
[[source]]
part = "b"
provider = "csv"
path = "placeholder_b.tsv.gz"
  [source.csv]
  delimiter = "\\t"
  has_header = false
  schema = [
    { name = "chrom", type = "Utf8" },
    { name = "pos",   type = "Utf8" },
    { name = "ref",   type = "Utf8" },
    { name = "alt",   type = "Utf8" },
    { name = "score", type = "Utf8" },
  ]
"""


def _init_full_repo(root: Path, *, multi_source: bool = False) -> Path:
    """A plugins repo whose demo manifest the Rust builder can load."""
    repo = root / "vepyr-plugins-full"
    (repo / "plugins" / "demo").mkdir(parents=True)
    body = _FULL_MANIFEST + (_SECOND_SOURCE if multi_source else "")
    (repo / "plugins" / "demo" / "demo.source.toml").write_text(body)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "tag", "v0.1.0"], cwd=repo, check=True)
    return repo


def _init_plugins_repo(root: Path) -> Path:
    repo = root / "vepyr-plugins"
    (repo / "plugins" / "demo").mkdir(parents=True)
    (repo / "plugins" / "demo" / "demo.source.toml").write_text(
        'plugin_name = "demo"\n'
    )
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "tag", "v0.1.0"], cwd=repo, check=True)
    # A second commit changes the file; the v0.1.0 checkout must ignore it.
    (repo / "plugins" / "demo" / "demo.source.toml").write_text(
        'plugin_name = "demo2"\n'
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "v2"],
        cwd=repo,
        check=True,
    )
    return repo


def test_resolve_manifest_offline_checks_out_tag(tmp_path):
    repo = _init_plugins_repo(tmp_path)
    with vepyr._resolve_plugin_manifest(
        "demo", "v0.1.0", plugins_repo=str(repo)
    ) as path:
        assert (
            Path(path).read_text().strip() == 'plugin_name = "demo"'
        )  # the tagged version
        worktree = Path(path).parents[2]  # <worktree>/plugins/demo/demo.source.toml
        assert worktree.exists()
    # On exit the worktree is removed and its registration in the caller's repo
    # (which we must NOT delete) is pruned — no /tmp leak, no stale worktree.
    assert not worktree.exists()
    assert repo.exists()  # caller-supplied clone is left untouched
    listed = subprocess.run(
        ["git", "-C", str(repo), "worktree", "list", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert str(worktree) not in listed


def test_plugin_cache_root_reaches_options(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_annotate_vcf(vcf, cache_dir, output_path, options_json, *rest):
        captured["options_json"] = options_json
        return 0

    monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
    vepyr.annotate(
        "in.vcf",
        "cache",
        output_vcf=str(tmp_path / "out.vcf"),
        plugin_cache_root="/tmp/pc",
        show_progress=False,
    )
    assert '"plugin_cache_root": "/tmp/pc"' in captured["options_json"]


def test_no_plugin_cache_root_omits_key(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_annotate_vcf(vcf, cache_dir, output_path, options_json, *rest):
        captured["options_json"] = options_json
        return 0

    monkeypatch.setattr(vepyr, "_annotate_vcf", fake_annotate_vcf)
    vepyr.annotate(
        "in.vcf", "cache", output_vcf=str(tmp_path / "out.vcf"), show_progress=False
    )
    assert "plugin_cache_root" not in captured["options_json"]


def test_failed_clone_leaves_no_temp_dir():
    """A failing online clone (bad repo_url) must not leak a vepyr-plugins-* temp
    dir — the clone happens inside the cleanup scope."""
    import glob
    import os
    import tempfile

    pat = os.path.join(tempfile.gettempdir(), "vepyr-plugins-*")
    before = set(glob.glob(pat))
    with pytest.raises(subprocess.CalledProcessError):
        # plugins_repo=None → online path; a nonexistent local url fails the clone.
        with vepyr._resolve_plugin_manifest(
            "demo",
            "v0.1.0",
            repo_url=str(Path(tempfile.gettempdir()) / "no-such-repo.git"),
        ):
            pass
    assert set(glob.glob(pat)) == before  # no leaked clone/worktree


def test_plugin_cache_root_reaches_streaming_options(monkeypatch):
    """The LazyFrame/streaming path (output_vcf=None) must forward
    plugin_cache_root too — it flows through the same options_json to
    _create_annotator, so plugin CSQ fields are emitted in both output modes."""
    import pyarrow as pa

    captured: dict = {}

    class _Probe:
        schema = pa.schema([pa.field("chrom", pa.string())])

    def fake_create_annotator(vcf, cache_dir, options_json, skip_csq, n_rows):
        captured["options_json"] = options_json
        return _Probe()

    monkeypatch.setattr(vepyr, "_create_annotator", fake_create_annotator)
    lf = vepyr.annotate(
        "in.vcf", "cache", plugin_cache_root="/tmp/pc", show_progress=False
    )
    assert lf is not None  # a LazyFrame, not a written path
    assert '"plugin_cache_root": "/tmp/pc"' in captured["options_json"]


def test_build_rejects_multi_source_manifest(tmp_path):
    """A manifest with >1 [[source]] can't be mapped by a single source_path."""
    repo = _init_full_repo(tmp_path, multi_source=True)
    with pytest.raises(ValueError, match="multi-part sources"):
        vepyr.build_plugin_cache(
            "demo",
            "v0.1.0",
            source_path=str(tmp_path / "src.tsv.gz"),
            cache_dir=str(tmp_path / "cache"),
            plugin_cache_root=str(tmp_path / "pc"),
            plugins_repo=str(repo),
        )


def test_build_refuses_existing_cache_without_overwrite(tmp_path):
    """overwrite=False must not clobber an existing plugin cache."""
    repo = _init_full_repo(tmp_path)
    pc = tmp_path / "pc"
    (pc / "plugin" / "demo").mkdir(parents=True)
    (pc / "plugin" / "demo" / "manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="already exists"):
        vepyr.build_plugin_cache(
            "demo",
            "v0.1.0",
            source_path=str(tmp_path / "src.tsv.gz"),
            cache_dir=str(tmp_path / "cache"),
            plugin_cache_root=str(pc),
            plugins_repo=str(repo),
            overwrite=False,
        )
    # The pre-existing manifest is untouched (build never ran).
    assert (pc / "plugin" / "demo" / "manifest.json").read_text() == "{}"
