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

# A manifest whose sole [[source]] carries a part -- the shape a {part: path}
# mapping addresses.
_PARTED_MANIFEST = _FULL_MANIFEST.replace(
    '[[source]]\nprovider = "csv"\npath = "placeholder.tsv.gz"',
    '[[source]]\npart = "a"\nprovider = "csv"\npath = "placeholder_a.tsv.gz"',
).replace("FROM plugin_demo_src", "FROM plugin_demo_src_a")

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


def _init_full_repo(
    root: Path, *, multi_source: bool = False, parted: bool = False
) -> Path:
    """A plugins repo whose demo manifest the Rust builder can load."""
    repo = root / "vepyr-plugins-full"
    (repo / "plugins" / "demo").mkdir(parents=True)
    base = _PARTED_MANIFEST if parted else _FULL_MANIFEST
    body = base + (_SECOND_SOURCE if multi_source else "")
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


def _build(repo, tmp_path, source_path, **kw):
    """Drive a build far enough to prove source_path was applied.

    Every call here fails on the absent variation cache -- that is the point: it
    means path mapping succeeded and the build reached the join. A mapping error
    raises earlier, with its own message.
    """
    return vepyr.build_plugin_cache(
        "demo",
        "v0.1.0",
        source_path=source_path,
        cache_dir=str(tmp_path / "cache"),
        plugin_cache_root=str(tmp_path / "pc"),
        plugins_repo=str(repo),
        chroms=["1"],
        **kw,
    )


def test_multi_source_manifest_maps_a_path_per_part(tmp_path):
    """The CADD shape: two [[source]] parts, one path each, combined in SQL."""
    repo = _init_full_repo(tmp_path, multi_source=True, parted=True)
    with pytest.raises(Exception) as exc:
        _build(
            repo,
            tmp_path,
            {"a": str(tmp_path / "a.tsv.gz"), "b": str(tmp_path / "b.tsv.gz")},
        )
    # Reached the build itself -- both parts resolved.
    assert "variation shard" in str(exc.value)


def test_multi_source_manifest_rejects_a_bare_path(tmp_path):
    """One path cannot address two sources; the others would read placeholders."""
    repo = _init_full_repo(tmp_path, multi_source=True, parted=True)
    with pytest.raises(ValueError, match="pass a dict mapping each part"):
        _build(repo, tmp_path, str(tmp_path / "src.tsv.gz"))


def test_missing_part_is_an_error_not_a_placeholder_read(tmp_path):
    """An unmapped source would silently read its placeholder path."""
    repo = _init_full_repo(tmp_path, multi_source=True, parted=True)
    with pytest.raises(ValueError, match="missing an entry for part"):
        _build(repo, tmp_path, {"a": str(tmp_path / "a.tsv.gz")})


def test_unknown_part_is_an_error(tmp_path):
    """A typo'd part would otherwise leave the real source on its placeholder."""
    repo = _init_full_repo(tmp_path, multi_source=True, parted=True)
    with pytest.raises(ValueError, match="does not declare"):
        _build(repo, tmp_path, {"a": "x", "b": "y", "snv": "z"})


def test_dict_requires_every_source_to_declare_a_part(tmp_path):
    """Without a part there is no key to address the source by."""
    repo = _init_full_repo(tmp_path, multi_source=True)  # first source has no part
    with pytest.raises(ValueError, match="declares\\s+no `part`|no `part`"):
        _build(repo, tmp_path, {"b": str(tmp_path / "b.tsv.gz")})


def test_single_source_still_takes_a_plain_path(tmp_path):
    """Back-compat: the common case is unchanged."""
    repo = _init_full_repo(tmp_path)
    with pytest.raises(Exception) as exc:
        _build(repo, tmp_path, str(tmp_path / "src.tsv.gz"))
    assert "variation shard" in str(exc.value)


def test_source_path_rejects_a_nonsense_type(tmp_path):
    repo = _init_full_repo(tmp_path)
    with pytest.raises(ValueError, match="must be a str or a dict"):
        _build(repo, tmp_path, 42)


def test_full_rebuild_refuses_existing_cache_without_overwrite(tmp_path):
    """An UNFILTERED (chroms=None) build over an existing cache is refused without
    overwrite — it would rewrite every chrom."""
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


def test_empty_chroms_treated_as_full_rebuild(tmp_path):
    """chroms=[] is 'no filter' (full rebuild) to the builder, so the overwrite
    guard must treat it like chroms=None and refuse an existing cache."""
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
            chroms=[],
            overwrite=False,
        )
    assert (pc / "plugin" / "demo" / "manifest.json").read_text() == "{}"


def test_filtered_build_not_blocked_by_overwrite_guard(tmp_path):
    """A filtered (chroms=[...]) build upserts into an existing cache, so the
    overwrite guard must NOT block it — incremental per-chromosome builds work.
    Here it still fails, but on the missing variation cache, not the guard."""
    repo = _init_full_repo(tmp_path)
    pc = tmp_path / "pc"
    (pc / "plugin" / "demo").mkdir(parents=True)
    (pc / "plugin" / "demo" / "manifest.json").write_text("{}")
    with pytest.raises(Exception) as exc:
        vepyr.build_plugin_cache(
            "demo",
            "v0.1.0",
            source_path=str(tmp_path / "src.tsv.gz"),
            cache_dir=str(tmp_path / "cache"),
            plugin_cache_root=str(pc),
            plugins_repo=str(repo),
            chroms=["1"],
            overwrite=False,
        )
    msg = str(exc.value)
    assert "already exists" not in msg  # guard did NOT fire
    assert "variation shard" in msg  # it reached the build (no variation cache)


def test_full_overwrite_wipes_stale_plugin_dir(tmp_path):
    """A full (chroms=None) overwrite rebuild must start from a clean slate. The
    builder's `with_overwrite` is a no-op and `build_all` SEEDS its manifest from
    any existing plugin manifest, preserving chroms not in the new build set — so
    rebuilding a smaller set into the same root would leave stale chrom entries and
    shards that `annotate()` keeps emitting. Verify the pre-existing plugin dir
    (manifest + shards) is removed before the build runs."""
    repo = _init_full_repo(tmp_path)
    pc = tmp_path / "pc"
    (pc / "plugin" / "demo").mkdir(parents=True)
    # A stale manifest entry + shard for a chrom the new build won't produce.
    (pc / "plugin" / "demo" / "manifest.json").write_text(
        '{"chroms": [{"chrom": "chrZZ"}]}'
    )
    (pc / "plugin" / "demo" / "chrZZ.parquet").write_bytes(b"stale")
    # No variation cache exists, so the build itself fails AFTER the wipe.
    with pytest.raises(Exception) as exc:
        vepyr.build_plugin_cache(
            "demo",
            "v0.1.0",
            source_path=str(tmp_path / "src.tsv.gz"),
            cache_dir=str(tmp_path / "cache"),
            plugin_cache_root=str(pc),
            plugins_repo=str(repo),
            overwrite=True,
        )
    assert "already exists" not in str(exc.value)  # overwrite bypassed the guard
    # The stale shard + manifest were removed before the (failing) build.
    assert not (pc / "plugin" / "demo" / "chrZZ.parquet").exists()
    assert not (pc / "plugin" / "demo" / "manifest.json").exists()


def test_single_source_manifest_rejects_a_mapping(tmp_path):
    """A dict needs a `part` to key each source by; a lone source declares none.

    Documented in docs/plugins.md alongside the multi-source shape, but the
    direction was previously only covered for multi-source manifests.
    """
    repo = _init_full_repo(tmp_path)
    with pytest.raises(ValueError, match="no `part`"):
        _build(repo, tmp_path, {"snv": str(tmp_path / "src.tsv.gz")})
