"""Install a PyPI wheel or build an exact vepyr Git commit with uv."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from run_tests.progress import Progress
from run_tests.verdict import Exit, RunTestsError

REPOSITORY = "https://github.com/biodatageeks/vepyr.git"
SHA = re.compile(r"[0-9a-fA-F]{40}")
VERSION = re.compile(r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?")


def validate_target(value: str) -> str:
    if SHA.fullmatch(value):
        return value.lower()
    if VERSION.fullmatch(value):
        return value
    raise RunTestsError(
        Exit.USAGE,
        "vepyr must be a release version (e.g. 0.9.0) or a full 40-character Git SHA",
    )


@dataclass(frozen=True)
class CliBuild:
    python: Path
    target: str
    source: str

    @property
    def label(self) -> str:
        return f"vepyr {self.target} ({self.source}) via {self.python}"


def _run(argv: list[str], *, cwd: Path | None = None, capture=False) -> str:
    """Keep build output live, with a heartbeat during otherwise quiet steps."""
    started = time.monotonic()
    try:
        with subprocess.Popen(
            argv,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE if capture else None,
        ) as process:
            while True:
                try:
                    stdout, _ = process.communicate(timeout=15)
                    break
                except subprocess.TimeoutExpired:
                    print(
                        f"Preparing vepyr: {argv[0]} {argv[1]} running "
                        f"({int(time.monotonic() - started)}s)",
                        flush=True,
                    )
            if process.returncode:
                raise RunTestsError(
                    Exit.ENGINE, f"{' '.join(argv[:3])} exited {process.returncode}"
                )
            return stdout or ""
    except OSError as exc:
        raise RunTestsError(Exit.ENGINE, f"cannot run {argv[0]}: {exc}") from exc


def install(target: str, cache_root: Path) -> CliBuild:
    target = validate_target(target)
    from_git = bool(SHA.fullmatch(target))
    source = "Git build with uv" if from_git else "PyPI wheel"
    base = cache_root / ".vepyr_cli" / target
    base.mkdir(parents=True, exist_ok=True)
    python = base / "venv/bin/python"
    receipt = base / "INSTALL.json"
    with (base / "install.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if receipt.is_file() and python.is_file():
            try:
                record = json.loads(receipt.read_text())
            except (ValueError, OSError):
                record = {}
            if record.get("target") == target and record.get("source") == source:
                print(f"Reusing vepyr {target} ({source})", flush=True)
                return CliBuild(python, target, source)
        progress = Progress(
            f"Preparing vepyr {target}", 4 if from_git else 2, unit="steps"
        )
        _run(
            [
                "uv",
                "venv",
                "--allow-existing",
                "--python",
                sys.executable,
                str(base / "venv"),
            ]
        )
        progress.update(1, force=True)
        record = {"target": target, "source": source}
        if from_git:
            src = base / "src"
            src.mkdir(exist_ok=True)
            if not (src / ".git").is_dir():
                _run(["git", "init", "-q", str(src)])
            _run(["git", "fetch", "--depth", "1", REPOSITORY, target], cwd=src)
            _run(["git", "checkout", "--detach", "FETCH_HEAD"], cwd=src)
            actual = _run(["git", "rev-parse", "HEAD"], cwd=src, capture=True).strip()
            if actual != target:
                raise RunTestsError(
                    Exit.ENGINE, f"Git checkout {actual} != requested {target}"
                )
            dirty = _run(
                ["git", "status", "--porcelain", "--untracked-files=normal"],
                cwd=src,
                capture=True,
            ).strip()
            if dirty:
                raise RunTestsError(
                    Exit.ENGINE,
                    f"cached vepyr source has local changes at {src}; "
                    "cannot build it as the requested Git SHA",
                )
            progress.update(2, force=True)
            wheels = base / "wheel"
            # A failed earlier build must not leave a wheel eligible for installation.
            if wheels.exists():
                for wheel in wheels.glob("*.whl"):
                    wheel.unlink()
            _run(
                [
                    "uv",
                    "build",
                    "--wheel",
                    "--python",
                    str(python),
                    "--out-dir",
                    str(wheels),
                    str(src),
                ]
            )
            built = list(wheels.glob("vepyr-*.whl"))
            if len(built) != 1:
                raise RunTestsError(
                    Exit.ENGINE,
                    f"expected one vepyr wheel in {wheels}, got {len(built)}",
                )
            wheel = built[0]
            with wheel.open("rb") as handle:
                record["wheel_sha256"] = hashlib.file_digest(
                    handle, "sha256"
                ).hexdigest()
            progress.update(3, force=True)
            requirement = str(wheel)
        else:
            requirement = f"vepyr=={target}"
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "--no-config",
                "--index-url",
                "https://pypi.org/simple",
                "--only-binary",
                ":all:",
                requirement,
            ]
        )
        _run([str(python), "-m", "vepyr", "--version"])
        temporary = receipt.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, indent=2) + "\n")
        os.replace(temporary, receipt)
        progress.update(4 if from_git else 2, force=True)
        return CliBuild(python, target, source)
