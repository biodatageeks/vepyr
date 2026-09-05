"""Hard-link staging directory and Hugging Face publishing via an injected runner."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Protocol

from cache_qa.manifest import CacheManifest


class PublishError(RuntimeError):
    """A Hub command failed or the published state differs from the staged files."""


class Runner(Protocol):
    def run(self, argv: list[str]) -> subprocess.CompletedProcess: ...


class SubprocessRunner:
    def run(self, argv: list[str]) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(argv, capture_output=True, text=True, check=False)
        except FileNotFoundError as e:
            raise PublishError(
                f"`{argv[0]}` is not on PATH; install with "
                "`curl -LsSf https://hf.co/cli/install.sh | bash`"
            ) from e


def _run_ok(runner: Runner, argv: list[str]) -> subprocess.CompletedProcess:
    cp = runner.run(argv)
    if cp.returncode != 0:
        msg = cp.stderr.strip() or cp.stdout.strip()
        raise PublishError(f"`{' '.join(argv)}` failed (exit {cp.returncode}): {msg}")
    return cp


def check_hf_available(runner: Runner) -> None:
    """Fail before any upload when `hf` is missing or not logged in."""
    _run_ok(runner, ["hf", "auth", "whoami"])


def _link_or_copy(src: Path, dst: Path) -> None:
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def build_stage(
    m: CacheManifest, qa_profile: Path, readme: Path, stage_dir: Path
) -> list[Path]:
    stage_dir = Path(stage_dir)
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True)
    staged: list[Path] = []
    for entry, path in m.present_shards():
        dst = stage_dir / entry.file
        _link_or_copy(path, dst)
        staged.append(dst)
    extras = (
        (m.plugin_dir / "manifest.json", "manifest.json"),
        (Path(qa_profile), "qa_profile.json"),
        (Path(readme), "README.md"),
    )
    for src, name in extras:
        dst = stage_dir / name
        _link_or_copy(src, dst)
        staged.append(dst)
    return staged


def _info(
    runner: Runner, repo: str, revision: str | None = None, siblings: bool = False
) -> dict:
    argv = ["hf", "datasets", "info", repo, "--format", "json"]
    if revision:
        argv += ["--revision", revision]
    if siblings:
        argv += ["--expand", "siblings"]
    cp = _run_ok(runner, argv)
    try:
        return json.loads(cp.stdout)
    except json.JSONDecodeError as e:
        raise PublishError(
            f"`{' '.join(argv)}` returned non-JSON output: {cp.stdout[:200]}"
        ) from e


def publish(
    runner: Runner, repo: str, stage_dir: Path, tag: str, commit_message: str
) -> str:
    _run_ok(
        runner,
        [
            "hf",
            "upload",
            repo,
            str(stage_dir),
            ".",
            "--type",
            "dataset",
            "--commit-message",
            commit_message,
        ],
    )
    # An absent tag is fine; only creation must succeed.
    runner.run(
        ["hf", "repos", "tag", "delete", repo, tag, "--type", "dataset", "--yes"]
    )
    _run_ok(
        runner,
        [
            "hf",
            "repos",
            "tag",
            "create",
            repo,
            tag,
            "--type",
            "dataset",
            "--message",
            commit_message,
        ],
    )
    return str(_info(runner, repo)["sha"])


def verify(runner: Runner, repo: str, stage_dir: Path, tag: str) -> list[str]:
    """Return mismatches between the staged files/tag and the Hub; empty means ok."""
    problems: list[str] = []
    info = _info(runner, repo, siblings=True)
    remote = {s["rfilename"]: s for s in info.get("siblings", [])}
    for path in sorted(Path(stage_dir).iterdir()):
        r = remote.get(path.name)
        if r is None:
            problems.append(f"{path.name}: missing on the hub")
            continue
        size = r.get("size")
        if size is not None and int(size) != path.stat().st_size:
            local = path.stat().st_size
            problems.append(f"{path.name}: size {local} local vs {size} on the hub")
    head = str(info.get("sha"))
    tag_sha = str(_info(runner, repo, revision=tag).get("sha"))
    if tag_sha != head:
        problems.append(f"tag {tag} resolves to {tag_sha[:7]}, head is {head[:7]}")
    return problems
