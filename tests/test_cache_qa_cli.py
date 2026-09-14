import json
import subprocess
from pathlib import Path

import polars as pl
import pytest
from cache_qa import card, cli
from cache_qa_synthetic import SyntheticCache


class FakeRunner:
    def __init__(self, readme_text="# card\n\n## Usage\n\nu\n"):
        self.calls = []
        self.readme_text = readme_text

    def run(self, argv):
        self.calls.append(list(argv))
        if argv[:2] == ["hf", "download"]:
            local_dir = Path(argv[argv.index("--local-dir") + 1])
            local_dir.mkdir(parents=True, exist_ok=True)
            (local_dir / "README.md").write_text(self.readme_text, encoding="utf-8")
            return subprocess.CompletedProcess(argv, 0, "", "")
        if argv[:3] == ["hf", "datasets", "info"]:
            upload = next(c for c in self.calls if c[:2] == ["hf", "upload"])
            stage_dir = Path(upload[3])
            siblings = [
                {"rfilename": p.name, "size": p.stat().st_size}
                for p in stage_dir.iterdir()
            ]
            body = json.dumps({"sha": "h", "siblings": siblings})
            return subprocess.CompletedProcess(argv, 0, body, "")
        return subprocess.CompletedProcess(argv, 0, "", "")


def test_parse_requires_plugin_and_root():
    with pytest.raises(SystemExit):
        cli.parse_args([])
    a = cli.parse_args(["demo", "--root", "/r"])
    assert a.plugin == "demo" and a.root == "/r"
    assert a.publish is None and a.json_only is False


def test_pass_writes_json_and_readme(tmp_path):
    cache = SyntheticCache(tmp_path)
    cache.write()
    readme = tmp_path / "README.md"
    readme.write_text("# T\n\n## Usage\n\nu\n", encoding="utf-8")
    rc = cli.main(
        ["demo", "--root", str(tmp_path), "--readme", str(readme)],
        now=lambda: "2026-09-05T00:00:00Z",
    )
    assert rc == 0
    out = json.loads((cache.plugin_dir / "qa_profile.json").read_text(encoding="utf-8"))
    assert out["status"] == "pass" and out["generated_at"] == "2026-09-05T00:00:00Z"
    text = readme.read_text(encoding="utf-8")
    assert card.START in text and "## Quality profile" in text


def test_json_only_skips_readme(tmp_path):
    SyntheticCache(tmp_path).write()
    readme = tmp_path / "README.md"
    readme.write_text("# T\n", encoding="utf-8")
    args = ["demo", "--root", str(tmp_path), "--readme", str(readme), "--json-only"]
    assert cli.main(args) == 0
    assert readme.read_text(encoding="utf-8") == "# T\n"


def test_failed_invariant_exits_1_and_blocks_publish(tmp_path):
    cache = SyntheticCache(tmp_path)
    df = cache.rows["chrX"]
    cache.rows["chrX"] = df.with_columns(pl.lit(2).cast(pl.Int8).alias("tier"))
    cache.write()
    runner = FakeRunner()
    args = [
        "demo",
        "--root",
        str(tmp_path),
        "--readme-from-hub",
        "org/repo",
        "--publish",
        "org/repo",
        "--tag",
        "v1",
    ]
    assert cli.main(args, runner=runner) == 1
    assert not any(c[:2] == ["hf", "upload"] for c in runner.calls)
    out = json.loads((cache.plugin_dir / "qa_profile.json").read_text(encoding="utf-8"))
    assert out["status"] == "fail"


def test_missing_manifest_exits_2(tmp_path):
    assert cli.main(["nope", "--root", str(tmp_path)]) == 2


def test_publish_requires_readme_source(tmp_path):
    SyntheticCache(tmp_path).write()
    args = ["demo", "--root", str(tmp_path), "--publish", "org/repo", "--tag", "v1"]
    assert cli.main(args, runner=FakeRunner()) == 2


def test_publish_happy_path(tmp_path):
    SyntheticCache(tmp_path).write()
    runner = FakeRunner()
    args = [
        "demo",
        "--root",
        str(tmp_path),
        "--readme-from-hub",
        "org/repo",
        "--publish",
        "org/repo",
        "--tag",
        "v0.1.1",
        "--commit-message",
        "m",
    ]
    assert cli.main(args, runner=runner) == 0
    kinds = [tuple(c[:2]) for c in runner.calls]
    assert ("hf", "download") in kinds and ("hf", "upload") in kinds
    upload = next(c for c in runner.calls if c[:2] == ["hf", "upload"])
    staged = sorted(p.name for p in Path(upload[3]).iterdir())
    assert staged == [
        "README.md",
        "chr1.parquet",
        "chr2.parquet",
        "chrX.parquet",
        "manifest.json",
        "qa_profile.json",
    ]
    assert card.START in (Path(upload[3]) / "README.md").read_text(encoding="utf-8")
