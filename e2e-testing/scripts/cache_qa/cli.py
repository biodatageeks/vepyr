"""profile_plugin_cache.py: verify, profile, render the card, optionally publish."""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import polars as pl

from cache_qa import card, invariants, profile, report, stage
from cache_qa.manifest import ManifestError, load_manifest

EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="profile_plugin_cache.py",
        description=(
            "Check a plugin cache's invariants, profile its content, update its "
            "Hub card."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("plugin", help="plugin name, e.g. clinvar")
    p.add_argument(
        "--root", required=True, help="plugin cache root containing plugin/<name>/"
    )
    p.add_argument(
        "--out",
        default=None,
        help="qa_profile.json path (default: <root>/plugin/<name>/qa_profile.json)",
    )
    p.add_argument("--readme", default=None, help="README.md to update in place")
    p.add_argument(
        "--readme-from-hub",
        default=None,
        metavar="REPO",
        help="fetch README.md from this dataset repo first",
    )
    p.add_argument(
        "--publish",
        default=None,
        metavar="REPO",
        help="upload shards, manifest, JSON and README as one commit",
    )
    p.add_argument(
        "--tag",
        default=None,
        help="tag to move to the new head (required with --publish)",
    )
    p.add_argument("--commit-message", default=None)
    p.add_argument(
        "--json-only",
        action="store_true",
        help="skip the card even if --readme is given",
    )
    return p.parse_args(argv)


def fetch_readme(runner: stage.Runner, repo: str, dest_dir: Path) -> Path:
    argv = [
        "hf",
        "download",
        repo,
        "README.md",
        "--type",
        "dataset",
        "--local-dir",
        str(dest_dir),
    ]
    cp = runner.run(argv)
    if cp.returncode != 0:
        raise stage.PublishError(
            f"hf download README.md from {repo} failed: {cp.stderr.strip()}"
        )
    path = Path(dest_dir) / "README.md"
    if not path.exists():
        raise stage.PublishError(f"hf download did not produce {path}")
    return path


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main(
    argv: list[str] | None = None,
    runner: stage.Runner | None = None,
    now: Callable[[], str] | None = None,
) -> int:
    args = parse_args(argv)
    runner = runner or stage.SubprocessRunner()
    now = now or _utc_now
    if args.publish and not (args.readme or args.readme_from_hub):
        print(
            "error: --publish needs --readme or --readme-from-hub (card would go stale)",
            file=sys.stderr,
        )
        return EXIT_USAGE
    if args.publish and not args.tag:
        print("error: --publish needs --tag", file=sys.stderr)
        return EXIT_USAGE

    plugin_dir = Path(args.root) / "plugin" / args.plugin
    try:
        m = load_manifest(plugin_dir)
    except ManifestError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE

    scratch = Path(tempfile.mkdtemp(prefix=f"cache_qa_{args.plugin}_"))
    readme_path: Path | None = Path(args.readme) if args.readme else None
    try:
        if args.publish:
            stage.check_hf_available(runner)
        if args.readme_from_hub:
            readme_path = fetch_readme(runner, args.readme_from_hub, scratch / "readme")
        try:
            results = invariants.run_all(m)
            prof = profile.profile_cache(m)
        except (OSError, pl.exceptions.PolarsError) as e:
            print(f"error: cannot read shards: {e}", file=sys.stderr)
            return EXIT_USAGE
        rep = report.build_report(m, results, prof, now(), report.tool_versions())
        out = Path(args.out) if args.out else plugin_dir / "qa_profile.json"
        report.write_report(rep, out)
        for r in results:
            print(f"{r.id:16s} {r.status:5s} {r.detail}")
        print(f"status={rep['status']} rows={rep['summary']['rows']:,} json={out}")

        if readme_path is not None and not args.json_only:
            text = card.splice(readme_path.read_text(), card.render_section(rep))
            readme_path.write_text(text)
            print(f"card updated: {readme_path}")

        if rep["status"] == "fail":
            print("invariants failed; nothing published", file=sys.stderr)
            return EXIT_FAILED
        if args.publish:
            if readme_path is None:
                return EXIT_USAGE
            stage_dir = scratch / f"stage_{args.plugin}"
            stage.build_stage(m, out, readme_path, stage_dir)
            version = m.cache_source_version or "unversioned"
            message = args.commit_message or (
                f"{args.plugin}: quality profile {rep['generated_at'][:10]} ({version})"
            )
            head = stage.publish(runner, args.publish, stage_dir, args.tag, message)
            problems = stage.verify(runner, args.publish, stage_dir, args.tag)
            if problems:
                print(
                    "published but verification failed:\n  " + "\n  ".join(problems),
                    file=sys.stderr,
                )
                return EXIT_FAILED
            print(f"published {args.publish} @ {head[:7]}, tag {args.tag}")
        return EXIT_OK
    except stage.PublishError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE
