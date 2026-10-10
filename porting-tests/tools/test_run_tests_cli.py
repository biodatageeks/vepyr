"""Runner checks are separate from ./run_tests's named data suite."""

from __future__ import annotations

import dataclasses
import io
import json
import shutil
from pathlib import Path

import pytest

from run_tests import cli, fixtures, install, suite
from run_tests.verdict import Exit, RunTestsError

ROOT = fixtures.ROOT
SOURCE = ROOT / "tests/data/runner_buffer_size_invariance"
BUILD = install.CliBuild(Path("/fake/python"), "0.9.0", "test double")


@pytest.fixture
def case(tmp_path):
    dest = tmp_path / SOURCE.name
    shutil.copytree(SOURCE, dest)
    return dest


def test_entire_suite_loads_and_preserves_counts():
    cases = fixtures.load_all(sorted((ROOT / "tests/data").iterdir()))
    assert len(cases) == 70
    assert sum(len(c.ids) for c in cases) == 205
    assert sum(len(c.runs) for c in cases) == 74
    assert sum(len(c.ids) for c in cases if c.skip) == 1


@pytest.mark.parametrize("target", ["0.9.0", "0.10.0rc1", "a" * 40])
def test_one_positional_argument(target):
    assert cli.parse_args([target]).vepyr == target


@pytest.mark.parametrize(
    "args", [[], ["0.9.0", "--via-cli"], ["--vepyr", "0.9.0"], ["0.9.0", "--only", "x"]]
)
def test_legacy_flags_are_not_parameters(args):
    with pytest.raises(SystemExit):
        cli.parse_args(args)


@pytest.mark.parametrize(
    "target", ["master", "latest", "abc1234", "../x", "0.9.0;echo x"]
)
def test_ref_or_package_expression_is_not_accepted(target):
    with pytest.raises(RunTestsError):
        cli.parse_args([target])


@pytest.mark.parametrize(
    ("before", "after", "message"),
    [
        ('flavour = "merged"', 'flavour = "ensembl"', "cache flavour mismatch"),
        ("everything = true", "everything = false", "unsupported mode"),
        ("buffer_size = 1", "buffer_size = 0", "positive integer"),
        ("buffer_size = 1", "buffer_size = true", "wrong type"),
        ("buffer_size = 1", "unknown_key = 1", "unknown key"),
        ("--everything --vcf", "--vcf", "unsupported mode"),
        ('required_contigs = ["chr21"]', "required_contigs = []", "required_contigs"),
        ("release/116.2/t/", "master/t/", "tagged Ensembl"),
        ('cache_source = "https://', 'cache_source = "file://', "HTTP"),
        ('name = "runner_buffer_size_invariance"', 'name = "wrong"', "directory name"),
        (
            'description = "Fixture',
            'skip_reason = ""\ndescription = "Fixture',
            "non-empty",
        ),
    ],
)
def test_loader_rejects_metadata_mutations(case, before, after, message):
    path = case / "test.toml"
    text = path.read_text()
    assert before in text
    path.write_text(text.replace(before, after, 1))
    with pytest.raises(RunTestsError, match=message):
        fixtures.load(case)


def test_edited_oracle_fails_even_when_skipped(case):
    path = case / "test.toml"
    path.write_text('skip_reason = "unsupported"\n' + path.read_text())
    with (case / "expected_output.vcf").open("ab") as f:
        f.write(b"21\t123\t.\tA\tG\t.\t.\t.\n")
    with pytest.raises(RunTestsError, match="oracle edited"):
        fixtures.load(case)


def test_duplicate_global_ids_fail(case):
    with pytest.raises(RunTestsError, match="declared by both"):
        fixtures.load_all([case, case])


def test_empty_selection_cannot_pass():
    with pytest.raises(RunTestsError, match="no data fixtures"):
        fixtures.load_all([])


def test_body_rule_preserves_crlf_and_final_unterminated_line():
    assert fixtures.body_lines(b"#header\r\na\r\nb\nc") == [b"a\r\n", b"b\n", b"c"]


def test_each_buffer_run_executes_but_named_progress_counts_once(case, tmp_path):
    fixture = fixtures.load(case)
    calls = []

    def runner(argv):
        calls.append(argv)
        Path(argv[argv.index("--output_file") + 1]).write_bytes(
            (case / "expected_output.vcf").read_bytes()
        )
        return 0

    out = io.StringIO()
    report = suite.run(
        [fixture],
        build=BUILD,
        cache_root=tmp_path,
        fasta=Path("ref.fa"),
        runner=runner,
        out=out,
    )
    assert report.code == Exit.OK
    assert len(calls) == 5
    assert [a[a.index("--buffer-size") + 1] for a in calls[:4]] == ["1", "2", "3", "5"]
    assert "--buffer-size" not in calls[4]
    assert report.passed == [case.name]
    assert out.getvalue().count(f"PASS {case.name}") == 1
    assert "1/1 tests | 1 passed, 0 failed, 0 skipped | DONE" in out.getvalue()


@pytest.mark.parametrize(
    ("behavior", "code", "label"),
    [("mismatch", 8, "MISMATCH"), ("error", 6, "ERROR"), ("missing", 6, "ERROR")],
)
def test_failures_are_never_green(case, tmp_path, behavior, code, label):
    calls = []

    def runner(argv):
        calls.append(argv)
        if behavior == "error":
            return 2
        if behavior == "mismatch":
            Path(argv[argv.index("--output_file") + 1]).write_bytes(b"#header\n")
        return 0

    out = io.StringIO()
    report = suite.run(
        [fixtures.load(case)],
        build=BUILD,
        cache_root=tmp_path,
        fasta=Path("ref.fa"),
        runner=runner,
        out=out,
        err=io.StringIO(),
    )
    assert report.code == code
    assert not report.passed
    assert f"{label} {case.name}" in out.getvalue()


def test_skipped_fixture_never_invokes_cli(case, tmp_path):
    fixture = dataclasses.replace(fixtures.load(case), skip="unsupported deletion")

    def never(_):
        pytest.fail("skipped fixture invoked vepyr")

    report = suite.run(
        [fixture],
        build=BUILD,
        cache_root=tmp_path,
        fasta=Path("ref.fa"),
        runner=never,
        out=io.StringIO(),
    )
    assert report.skipped == [(case.name, "unsupported deletion")]
    assert not report.passed


def test_selection_only_invokes_data_cli(case, tmp_path):
    def runner(argv):
        assert argv[:3] == [str(BUILD.python), "-m", "vepyr"]
        Path(argv[argv.index("--output_file") + 1]).write_bytes(
            (case / "expected_output.vcf").read_bytes()
        )
        return 0

    code = cli.run_selection(
        "0.9.0",
        [case],
        root=tmp_path,
        preparer=lambda *_: Path("ref.fa"),
        installer=lambda *_: BUILD,
        runner=runner,
    )
    assert code == 0


def test_release_install_is_wheel_only_and_reused(tmp_path, monkeypatch):
    calls = []

    def run(argv, **kw):
        calls.append(argv)
        if argv[:2] == ["uv", "venv"]:
            python = Path(argv[-1]) / "bin/python"
            python.parent.mkdir(parents=True)
            python.touch()
        return ""

    monkeypatch.setattr(install, "_run", run)
    result = install.install("0.9.0", tmp_path)
    assert result.source == "PyPI wheel"
    command = next(a for a in calls if a[:3] == ["uv", "pip", "install"])
    assert command[-1] == "vepyr==0.9.0"
    assert command[command.index("--only-binary") + 1] == ":all:"
    assert not any(a[0] in {"git", "cargo"} or a[:2] == ["uv", "build"] for a in calls)
    count = len(calls)
    assert install.install("0.9.0", tmp_path) == result
    assert len(calls) == count


def test_git_build_verifies_sha_and_records_wheel(tmp_path, monkeypatch):
    calls = []
    sha = "a" * 40

    def run(argv, **kw):
        calls.append(argv)
        if argv[:2] == ["uv", "build"]:
            out = Path(argv[argv.index("--out-dir") + 1])
            out.mkdir()
            (out / "vepyr-test.whl").write_bytes(b"wheel")
        if argv[:2] == ["git", "rev-parse"]:
            return sha + "\n"
        return ""

    monkeypatch.setattr(install, "_run", run)
    install.install(sha, tmp_path)
    assert any(a[:2] == ["uv", "build"] for a in calls)
    assert next(a for a in calls if a[:2] == ["git", "fetch"])[-1] == sha
    assert json.loads((tmp_path / ".vepyr_cli" / sha / "INSTALL.json").read_text())[
        "wheel_sha256"
    ]


def test_git_checkout_mismatch_cannot_build_or_write_receipt(tmp_path, monkeypatch):
    calls = []

    def run(argv, **kw):
        calls.append(argv)
        return "b" * 40 if argv[:2] == ["git", "rev-parse"] else ""

    monkeypatch.setattr(install, "_run", run)
    with pytest.raises(RunTestsError, match="requested"):
        install.install("a" * 40, tmp_path)
    assert not any(a[:2] == ["uv", "build"] for a in calls)
    assert not list(tmp_path.rglob("INSTALL.json"))


def test_cache_rejects_required_shard_omitted_from_remote_manifest(
    tmp_path, monkeypatch
):
    import hashlib

    from test_fetch import PINS_TOML, build_hub

    from run_tests import fetch

    hub = build_hub(tmp_path / "remote", contigs=("chr1", "chr21"))
    manifest = tmp_path / "remote/merged/exon/chrom_manifest.json"
    manifest.write_text(
        json.dumps(
            [
                entry
                for entry in json.loads(manifest.read_text())
                if entry["dataset"] != "chr21.parquet"
            ]
        )
    )
    repo = tmp_path / "repo"
    repo.mkdir()
    fasta_bytes = b">21\nA\n"
    (repo / "PINS.toml").write_text(
        PINS_TOML
        + '\n[grch38_fasta]\nrepo = "https://example.org"\nref = "tiny.fa.gz"\n'
        + f'sha = "{hashlib.sha256(fasta_bytes).hexdigest()}"\n'
        + 'ensembl_sum = "0 0"\n'
    )
    cache = tmp_path / "cache"
    (cache / "fasta").mkdir(parents=True)
    (cache / "fasta/tiny.fa").write_bytes(fasta_bytes)
    (cache / "fasta/tiny.fa.fai").write_text("21\t1\t4\t1\t2\n")
    original = fixtures.load(SOURCE)
    fixture = dataclasses.replace(
        original,
        runs=[dict(original.runs[0], required_contigs=["chr1", "chr21"])],
    )
    monkeypatch.setattr(fetch, "hub_lister", hub.lister)
    monkeypatch.setattr(fetch, "hub_downloader", hub.downloader)
    with pytest.raises(RunTestsError, match="unlisted in manifest") as failure:
        cli.prepare_cache([fixture], cache, repo)
    assert failure.value.code == Exit.INCOMPLETE
    entity = cache / "116_GRCh38_merged/exon"
    assert (entity / "chr21.parquet").is_file()
    assert "chr21.parquet" not in (entity / fetch.MANIFEST).read_text()


@pytest.mark.parametrize("mutation", ["tracked", "untracked"])
def test_git_install_retry_rejects_modified_checkout(tmp_path, monkeypatch, mutation):
    import subprocess

    upstream = tmp_path / "upstream"
    upstream.mkdir()

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=upstream, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-q")
    (upstream / "module.py").write_text("ORIGINAL\n")
    git("add", "module.py")
    git(
        "-c",
        "user.name=Review",
        "-c",
        "user.email=review@example.invalid",
        "commit",
        "-qm",
        "baseline",
    )
    sha = git("rev-parse", "HEAD")
    cache = tmp_path / "cache"
    base = cache / ".vepyr_cli" / sha
    builds = []
    run_git = install._run

    def run(argv, **kwargs):
        if argv[0] == "git":
            return run_git(argv, **kwargs)
        if argv[:2] == ["uv", "venv"]:
            python = Path(argv[-1]) / "bin/python"
            python.parent.mkdir(parents=True, exist_ok=True)
            python.touch()
        if argv[:2] == ["uv", "build"]:
            builds.append((Path(argv[-1]) / "module.py").read_text())
            wheels = Path(argv[argv.index("--out-dir") + 1])
            wheels.mkdir(exist_ok=True)
            (wheels / "vepyr-test.whl").write_bytes(b"wheel")
        return ""

    monkeypatch.setattr(install, "REPOSITORY", str(upstream))
    monkeypatch.setattr(install, "_run", run)
    install.install(sha, cache)
    receipt = base / "INSTALL.json"
    receipt.unlink()
    changed = "module.py" if mutation == "tracked" else "injected.py"
    (base / "src" / changed).write_text("MUTATED\n")
    with pytest.raises(RunTestsError, match="local changes") as failure:
        install.install(sha, cache)
    assert failure.value.code == Exit.ENGINE
    assert builds == ["ORIGINAL\n"]
    assert not receipt.exists()
