import json
import subprocess

import pytest
from cache_qa import manifest, stage
from cache_qa_synthetic import SyntheticCache


class FakeRunner:
    def __init__(self, info=None, tag_info=None, fail_on=None):
        self.calls: list[list[str]] = []
        self.info = info or {"sha": "abc123"}
        self.tag_info = tag_info
        self.fail_on = fail_on

    def run(self, argv):
        self.calls.append(list(argv))
        if self.fail_on and self.fail_on in " ".join(argv):
            return subprocess.CompletedProcess(argv, 1, "", "boom")
        if argv[:3] == ["hf", "datasets", "info"]:
            body = (
                self.tag_info if "--revision" in argv and self.tag_info else self.info
            )
            if "--expand" in argv:  # the Hub omits sha when siblings are expanded
                body = {k: v for k, v in body.items() if k != "sha"}
            return subprocess.CompletedProcess(argv, 0, json.dumps(body), "")
        return subprocess.CompletedProcess(argv, 0, "", "")


def _staged(tmp_path):
    m = manifest.load_manifest(SyntheticCache(tmp_path).write())
    qa = m.plugin_dir / "qa_profile.json"
    qa.write_text("{}\n")
    readme = tmp_path / "README.md"
    readme.write_text("# card\n")
    stage_dir = tmp_path / "stage_demo"
    staged = stage.build_stage(m, qa, readme, stage_dir)
    return m, stage_dir, staged


def test_build_stage_links_every_listed_file(tmp_path):
    m, stage_dir, staged = _staged(tmp_path)
    names = sorted(p.name for p in staged)
    assert names == [
        "README.md",
        "chr1.parquet",
        "chr2.parquet",
        "chrX.parquet",
        "manifest.json",
        "qa_profile.json",
    ]
    src = m.plugin_dir / "chr1.parquet"
    assert (stage_dir / "chr1.parquet").stat().st_ino == src.stat().st_ino


def test_build_stage_skips_zero_row_missing_shard(tmp_path):
    cache = SyntheticCache(tmp_path)
    md = cache.manifest_dict()
    md["chroms"].append(
        {"chrom": "chrMT", "file": "chrMT.parquet", "rows": 0, "warm": 0, "cold": 0}
    )
    cache.set_manifest(md)
    m = manifest.load_manifest(cache.write())
    qa = m.plugin_dir / "qa_profile.json"
    qa.write_text("{}\n")
    readme = tmp_path / "README.md"
    readme.write_text("# c\n")
    staged = stage.build_stage(m, qa, readme, tmp_path / "s")
    assert "chrMT.parquet" not in {p.name for p in staged}


def test_publish_runs_upload_and_moves_tag(tmp_path):
    _, stage_dir, _ = _staged(tmp_path)
    runner = FakeRunner()
    head = stage.publish(runner, "org/repo", stage_dir, "v0.1.1", "msg")
    assert head == "abc123"
    argv = runner.calls
    assert argv[0][:3] == ["hf", "upload", "org/repo"]
    assert str(stage_dir) in argv[0] and "dataset" in argv[0]
    assert argv[0][argv[0].index("--commit-message") + 1] == "msg"
    assert argv[1] == [
        "hf",
        "repos",
        "tag",
        "delete",
        "org/repo",
        "v0.1.1",
        "--type",
        "dataset",
        "--yes",
    ]
    assert argv[2][:6] == ["hf", "repos", "tag", "create", "org/repo", "v0.1.1"]


def test_publish_raises_when_upload_fails(tmp_path):
    _, stage_dir, _ = _staged(tmp_path)
    with pytest.raises(stage.PublishError) as e:
        stage.publish(FakeRunner(fail_on="hf upload"), "org/repo", stage_dir, "v1", "m")
    assert "hf upload" in str(e.value) and "boom" in str(e.value)


def test_verify_reports_size_and_tag_mismatch(tmp_path):
    _, stage_dir, staged = _staged(tmp_path)
    siblings = [{"rfilename": p.name, "size": p.stat().st_size} for p in staged]
    siblings[0]["size"] += 1
    siblings.pop()
    runner = FakeRunner(
        info={"sha": "head1", "siblings": siblings}, tag_info={"sha": "old"}
    )
    problems = stage.verify(runner, "org/repo", stage_dir, "v0.1.1")
    assert any("chr1.parquet" in p and "size" in p for p in problems)
    assert any("missing" in p for p in problems)
    assert any("tag" in p for p in problems)


def test_verify_clean(tmp_path):
    _, stage_dir, staged = _staged(tmp_path)
    siblings = [{"rfilename": p.name, "size": p.stat().st_size} for p in staged]
    runner = FakeRunner(info={"sha": "h", "siblings": siblings})
    assert stage.verify(runner, "org/repo", stage_dir, "v0.1.1") == []


def test_check_hf_available_raises():
    with pytest.raises(stage.PublishError):
        stage.check_hf_available(FakeRunner(fail_on="hf auth whoami"))
