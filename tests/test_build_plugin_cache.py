"""Plugin-cache builder manifest resolution + annotate plugin_cache_root plumbing."""

import subprocess
from pathlib import Path

import vepyr


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
    path = vepyr._resolve_plugin_manifest("demo", "v0.1.0", plugins_repo=str(repo))
    assert (
        Path(path).read_text().strip() == 'plugin_name = "demo"'
    )  # the tagged version


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
