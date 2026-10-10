"""Tests for ``./bless`` (``tools/bless``) that need no docker, cache or network."""

from __future__ import annotations

import hashlib
import os
import re
import shlex
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Final

import pytest

from bless import BlessError, cli, ensembl, testdir, vep

REPO: Final[Path] = Path(__file__).resolve().parent.parent

INPUT_TABLE: Final[str] = (
    '[input]\ncommand = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"\n'
    'bcftools_version = "bcftools 1.23"\n'
)
VEPYR_TABLE: Final[str] = (
    '[vepyr]\nflavour = "ensembl"\neverything = true\nreference_fasta = true\n'
    "preserve_record_layout = true\n"
)
BODY: Final[str] = "21\t100\t.\tC\tT\t.\t.\tCSQ=T|x\n"


@pytest.fixture
def test_dir(tmp_path: Path) -> Path:
    """A normalised data-test directory with a blessed-looking oracle."""
    d = tmp_path / "case"
    d.mkdir()
    (d / "input.vcf").write_text(
        "##fileformat=VCFv4.2\n#CHROM\tPOS\n21\t100\t.\tC\tT\t.\t.\t.\n"
    )
    (d / "expected_output.vcf").write_text("##VEP=v116\n#CHROM\n" + BODY)
    md5 = hashlib.md5(BODY.encode()).hexdigest()
    (d / "test.toml").write_text(
        f'name = "case"\n\n{INPUT_TABLE}\n{VEPYR_TABLE}\n[vep]\n# kept\n\n'
        f'[compare]\nbody_md5 = "{md5}"\n'
    )
    return d


@pytest.fixture
def complete_cache(tmp_path: Path) -> Path:
    """A directory with the layout of a complete release-116 cache."""
    base = tmp_path / "cache" / ensembl.CACHE_SUBDIR
    for contig in ensembl.REQUIRED_CONTIGS:
        (base / contig).mkdir(parents=True)
    (base / "info.txt").write_text("species\thomo_sapiens\n")
    return tmp_path / "cache"


@pytest.fixture
def fasta(tmp_path: Path) -> Path:
    """A two-record FASTA."""
    fa = tmp_path / "g.fa"
    fa.write_text(">21 dna\nACGT\nAC\n>22\nGGGG\n")
    return fa


def run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    """Run ``bless`` in-process and return (exit code, stdout, stderr)."""
    code = cli.main(argv)
    out = capsys.readouterr()
    return code, out.out, out.err


def test_normalize_command_matches_the_script() -> None:
    """The #85 command duplicated in bless equals the one normalize_input writes."""
    source = (REPO / "tools" / "normalize_input").read_text()
    assert f'COMMAND_TEMPLATE: Final[str] = "{testdir.NORMALIZE_COMMAND}"' in source


def test_check_passes_and_catches_drift(
    test_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--check: 0 on a matching oracle, 1 with a message after one appended byte."""
    assert run(["--check", str(test_dir)], capsys)[0] == 0
    with (test_dir / "expected_output.vcf").open("a") as fh:
        fh.write("x")
    code, _, err = run(["--check", str(test_dir)], capsys)
    assert code == 1
    assert "drifted" in err and err.count("\n") == 1


def test_check_needs_no_docker(
    test_dir: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--check never looks for docker."""
    monkeypatch.setattr(vep, "require_docker", lambda: pytest.fail("docker consulted"))
    assert run(["--check", str(test_dir)], capsys)[0] == 0


@pytest.mark.parametrize(
    ("flags", "named"),
    [
        (["--vep-fasta", "/x.fa"], ["--vep-cache-dir", "--download-vep-cache-to-dir"]),
        (["--vep-cache-dir", "/c"], ["--vep-fasta", "--download-vep-fasta-to"]),
        (
            [
                "--vep-cache-dir",
                "/c",
                "--download-vep-cache-to-dir",
                "/d",
                "--vep-fasta",
                "/x.fa",
            ],
            ["--vep-cache-dir", "--download-vep-cache-to-dir"],
        ),
        (
            [
                "--vep-cache-dir",
                "/c",
                "--vep-fasta",
                "/x.fa",
                "--download-vep-fasta-to",
                "/y.fa",
            ],
            ["--vep-fasta", "--download-vep-fasta-to"],
        ),
    ],
)
def test_flag_pairs(flags: list[str], named: list[str], test_dir: Path, capsys) -> None:
    """Neither or both flags of a pair: exit 1, one line naming both flags."""
    code, _, err = run([*flags, str(test_dir)], capsys)
    assert code == 1
    assert err.count("\n") == 1
    assert all(n in err for n in named)


def test_incomplete_cache_is_refused_and_untouched(
    tmp_path: Path, test_dir: Path, fasta: Path, capsys
) -> None:
    """An empty cache dir exits 1 as incomplete and stays empty."""
    empty = tmp_path / "empty"
    empty.mkdir()
    code, _, err = run(
        ["--vep-cache-dir", str(empty), "--vep-fasta", str(fasta), str(test_dir)],
        capsys,
    )
    assert code == 1 and "not a complete" in err
    assert list(empty.iterdir()) == []


def test_partial_cache_names_missing_contigs(complete_cache: Path) -> None:
    """A missing contig directory is named."""
    (complete_cache / ensembl.CACHE_SUBDIR / "MT").rmdir()
    assert ensembl.missing_cache_parts(complete_cache) == [
        f"{ensembl.CACHE_SUBDIR}/MT/"
    ]


def test_missing_docker(
    complete_cache: Path, fasta: Path, test_dir: Path, capsys, monkeypatch
) -> None:
    """No docker on PATH: exit 1, message names docker."""
    monkeypatch.setenv("PATH", "/nonexistent")
    code, _, err = run(
        [
            "--vep-cache-dir",
            str(complete_cache),
            "--vep-fasta",
            str(fasta),
            str(test_dir),
        ],
        capsys,
    )
    assert code == 1 and "docker" in err


def test_unreadable_fasta(
    complete_cache: Path, tmp_path: Path, test_dir: Path, capsys
) -> None:
    """A missing --vep-fasta exits 1 naming the flag."""
    code, _, err = run(
        [
            "--vep-cache-dir",
            str(complete_cache),
            "--vep-fasta",
            str(tmp_path / "no.fa"),
            str(test_dir),
        ],
        capsys,
    )
    assert code == 1 and "--vep-fasta" in err


def test_input_provenance_required(
    complete_cache: Path, fasta: Path, test_dir: Path, capsys
) -> None:
    """No #85 command in [input]: refused, naming test.toml and [input]."""
    toml = test_dir / "test.toml"
    toml.write_text(
        toml.read_text().replace(
            'command = "bcftools norm -m -both -o <out.vcf> <in.vcf.gz>"\n', ""
        )
    )
    code, _, err = run(
        [
            "--vep-cache-dir",
            str(complete_cache),
            "--vep-fasta",
            str(fasta),
            str(test_dir),
        ],
        capsys,
    )
    assert code == 1
    assert "test.toml" in err and "[input]" in err and "command" in err


def test_dry_run_prints_fixed_command(test_dir: Path, capsys) -> None:
    """--dry-run prints the tag and the fixed VEP command once each."""
    code, out, _ = run(
        ["--dry-run", "--vep-cache-dir", "/c", "--vep-fasta", "/x.fa", str(test_dir)],
        capsys,
    )
    assert code == 0
    assert (
        out.count("--species homo_sapiens --cache_version 116 --assembly GRCh38") == 1
    )
    assert out.count(vep.IMAGE_TAG) == 1


def test_reproduce_requires_check(test_dir: Path, capsys) -> None:
    """--reproduce alone is refused with a readable line."""
    code, _, err = run(
        ["--reproduce", "--vep-cache-dir", "/c", "--vep-fasta", "/x.fa", str(test_dir)],
        capsys,
    )
    assert code == 1 and "--check" in err


def test_set_keys_keeps_comments_and_round_trips(test_dir: Path) -> None:
    """set_keys fills existing and new tables without touching anything else."""
    text = (test_dir / "test.toml").read_text()
    out = testdir.set_keys(
        text,
        {
            "vep": {"image": "ensemblorg/ensembl-vep@sha256:ab", "date": "2026-09-23"},
            "compare": {"body_md5": "0" * 32},
        },
    )
    assert "# kept\n" in out
    parsed = tomllib.loads(out)
    assert parsed["vep"] == {
        "image": "ensemblorg/ensembl-vep@sha256:ab",
        "date": "2026-09-23",
    }
    assert parsed["compare"]["body_md5"] == "0" * 32
    assert len(re.findall(r"^body_md5 = ", out, re.M)) == 1
    assert re.search(r'^image = "ensemblorg/ensembl-vep@sha256:', out, re.M)


def test_set_keys_appends_missing_table() -> None:
    """A missing table is appended."""
    out = testdir.set_keys('name = "x"\n', {"compare": {"body_md5": "f" * 32}})
    assert tomllib.loads(out) == {"name": "x", "compare": {"body_md5": "f" * 32}}


def test_set_keys_keeps_test_tables() -> None:
    """#238: re-blessing a multi-test dir keeps its [[tests]] tables."""
    text = (
        'name = "x"\n\n[compare]\nbody_md5 = "old"\n\n'
        '[[tests]]\nid = "x"\n\n[[tests]]\nid = "y"\nfocus = { kind = "csq" }\n'
    )
    out = testdir.set_keys(text, {"compare": {"body_md5": "f" * 32}})
    parsed = tomllib.loads(out)
    assert parsed["compare"] == {"body_md5": "f" * 32}
    assert [p["id"] for p in parsed["tests"]] == ["x", "y"]
    assert out == text.replace('"old"', '"' + "f" * 32 + '"')


def test_fai_matches_samtools_format(fasta: Path) -> None:
    """The built .fai has samtools faidx's five columns."""
    fai = ensembl.ensure_fai(fasta)
    assert fai.read_text() == "21\t6\t8\t4\t5\n22\t4\t20\t4\t5\n"


def test_wrapper_runs_without_path(test_dir: Path) -> None:
    """./bless --check works with PATH=/nonexistent (issue #32 AC9)."""
    done = subprocess.run(
        [str(REPO / "bless"), "--check", str(test_dir)],
        env={
            "PATH": "/nonexistent",
            "HOME": str(Path.home()),
            **{k: v for k, v in os.environ.items() if k.startswith("UV_")},
        },
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr


def test_bless_error_is_runtime_error() -> None:
    """BlessError stays catchable as RuntimeError."""
    assert issubclass(BlessError, RuntimeError)


def _fake_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the docker lookup and digest resolution (no daemon needed)."""
    monkeypatch.setattr(vep, "require_docker", lambda: "docker")
    monkeypatch.setattr(
        vep, "resolve_digest", lambda _d, _r: f"{vep.IMAGE_REPO}@sha256:{'a' * 64}"
    )


def test_default_docker_work_dir_is_repo_dot_bless() -> None:
    """The default is <repo-root>/.bless/, located from the source, not the cwd."""
    repo_root = Path(__file__).resolve().parents[1]
    assert (repo_root / "tools" / "bless" / "cli.py").is_file()
    assert cli.DEFAULT_DOCKER_WORK_DIR == repo_root / ".bless"
    assert ".bless/" in (repo_root / ".gitignore").read_text().splitlines()


@pytest.mark.parametrize("override", [False, True], ids=["default", "override"])
def test_docker_work_dir_resolution(
    override: bool,
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry run and real run both place the work dir under the resolved root.

    Without the flag the root is ``cli.DEFAULT_DOCKER_WORK_DIR`` (patched to a tmp
    path so the test never writes into the checkout); with it, the flag's path,
    a relative one resolved against the cwd.
    """
    default_root = tmp_path / "default-root"
    monkeypatch.setattr(cli, "DEFAULT_DOCKER_WORK_DIR", default_root)
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "override" if override else default_root
    flags = ["--docker-work-dir", "override"] if override else []
    base = ["--vep-cache-dir", str(complete_cache), "--vep-fasta", str(fasta)]

    code, out, _ = run([*flags, "--dry-run", *base, str(test_dir)], capsys)
    assert code == 0
    assert f"{root}/bless-XXXXXX:{vep.WORK_MOUNT}" in out

    _fake_docker(monkeypatch)
    probed: list[list[Path]] = []

    def refuse(_docker: str, _image: str, paths: list[Path]) -> None:
        probed.append(paths)
        raise BlessError("stop before VEP")

    monkeypatch.setattr(vep, "require_mountable", refuse)
    code, _, err = run([*flags, *base, str(test_dir)], capsys)
    assert code == 1 and "stop before VEP" in err
    work = probed[0][-1]
    assert work.parent == root and work.name.startswith("bless-")
    assert not work.exists()  # removed on failure


def test_unshared_docker_work_dir_gets_the_mount_error(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unmountable --docker-work-dir fails with require_mountable's own message."""
    _fake_docker(monkeypatch)
    unshared = tmp_path / "unshared"

    def fake_run(argv: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        host = Path(argv[argv.index("-v") + 1].rsplit(":", 2)[0])
        if host.is_relative_to(unshared):
            return subprocess.CompletedProcess(
                argv,
                125,
                "",
                "docker: Error response from daemon: Mounts denied: \n"
                f"The path {host} is not shared from the host and is not known "
                "to Docker.\n",
            )
        return subprocess.CompletedProcess(
            argv, 0, "\n".join(p.name for p in host.iterdir()), ""
        )

    monkeypatch.setattr(vep.subprocess, "run", fake_run)
    code, _, err = run(
        [
            "--docker-work-dir",
            str(unshared),
            "--vep-cache-dir",
            str(complete_cache),
            "--vep-fasta",
            str(fasta),
            str(test_dir),
        ],
        capsys,
    )
    assert code == 1
    assert err.startswith(f"bless: docker cannot mount {unshared}/bless-")
    assert "is not shared from the host" in err
    assert "Settings > Resources > File sharing" in err


PINNED: Final[str] = f"{vep.IMAGE_REPO}@sha256:{'a' * 64}"


def _fake_vep_container(
    monkeypatch: pytest.MonkeyPatch, calls: list[list[str]]
) -> None:
    """Fake docker end to end: record each ``docker run`` argv, write an oracle.

    The fake container writes ``expected_output.vcf`` with :data:`BODY` into the
    bind-mounted work directory, so a bless or reproduce completes without docker.
    """
    _fake_docker(monkeypatch)
    monkeypatch.setattr(vep, "require_mountable", lambda _d, _i, _p: None)

    def fake_run(argv: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        work = next(
            Path(v.removesuffix(f":{vep.WORK_MOUNT}"))
            for v in argv
            if v.endswith(f":{vep.WORK_MOUNT}")
        )
        (work / testdir.ORACLE_NAME).write_text("##VEP=v116\n#CHROM\n" + BODY)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(vep.subprocess, "run", fake_run)


def _vep_part(argv: list[str]) -> list[str]:
    """The VEP argv inside a ``docker run`` argv (everything after the image)."""
    return argv[argv.index(PINNED) + 1 :]


def _set_vep(test_dir: Path, command: str) -> None:
    """Give ``test_dir`` a pinned image and the recorded ``command``."""
    toml = test_dir / "test.toml"
    toml.write_text(
        testdir.set_keys(
            toml.read_text(), {"vep": {"image": PINNED, "command": command}}
        )
    )


def test_vep_flag_recorded_in_command(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bless with --vep-flag records exactly the argv VEP ran with."""
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    code, _, err = run(
        [
            "--docker-work-dir",
            str(tmp_path / "w"),
            "--vep-cache-dir",
            str(complete_cache),
            "--vep-fasta",
            str(fasta),
            "--vep-flag=--check_existing",
            str(test_dir),
        ],
        capsys,
    )
    assert code == 0, err
    ran = _vep_part(calls[-1])
    assert ran == [*vep.VEP_ARGV, "--check_existing"]
    recorded = tomllib.loads((test_dir / "test.toml").read_text())["vep"]["command"]
    assert recorded == shlex.join(ran)


def _set_flags(test_dir: Path, flags: list[str], command: str | None = None) -> None:
    """Record ``flags`` as ``[vep] extra_flags`` and a matching (or given) command."""
    toml = test_dir / "test.toml"
    toml.write_text(
        testdir.set_keys(
            toml.read_text(),
            {
                "vep": {
                    "extra_flags": flags,
                    "image": PINNED,
                    "command": vep.vep_command(tuple(flags))
                    if command is None
                    else command,
                }
            },
        )
    )


def _bless_argv(test_dir: Path, cache: Path, fasta: Path, tmp_path: Path) -> list[str]:
    """Arguments of a real (faked-docker) bless of ``test_dir``."""
    return [
        "--docker-work-dir",
        str(tmp_path / "w"),
        "--vep-cache-dir",
        str(cache),
        "--vep-fasta",
        str(fasta),
        str(test_dir),
    ]


def test_bless_records_extra_flags_list(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--vep-flag= writes [vep] extra_flags as a list; no flag writes no key."""
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    argv = _bless_argv(test_dir, complete_cache, fasta, tmp_path)
    code, _, err = run(argv, capsys)
    assert code == 0, err
    assert (
        "extra_flags" not in tomllib.loads((test_dir / "test.toml").read_text())["vep"]
    )
    code, _, err = run(["--vep-flag=--check_existing", *argv], capsys)
    assert code == 0, err
    text = (test_dir / "test.toml").read_text()
    assert 'extra_flags = ["--check_existing"]\n' in text
    recorded = tomllib.loads(text)["vep"]
    assert recorded["extra_flags"] == ["--check_existing"]
    assert recorded["command"] == vep.vep_command(("--check_existing",))
    assert _vep_part(calls[-1]) == [*vep.VEP_ARGV, "--check_existing"]


def test_reproduce_reads_extra_flags_list(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--check --reproduce passes [vep] extra_flags, in order, to docker."""
    _set_flags(test_dir, ["--check_existing"])
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    code, _, err = run(
        [
            "--check",
            "--reproduce",
            *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
        ],
        capsys,
    )
    assert code == 0, err
    assert _vep_part(calls[-1]) == [*vep.VEP_ARGV, "--check_existing"]


def test_reproduce_catches_drift(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--check --reproduce runs the drift check first, before any docker call."""
    _set_vep(test_dir, vep.vep_command(()))
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    argv = [
        "--check",
        "--reproduce",
        *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
    ]
    code, _, err = run(argv, capsys)
    assert code == 0, err
    assert len(calls) == 1
    calls.clear()
    with (test_dir / testdir.ORACLE_NAME).open("a") as oracle:
        oracle.write("x")
    code, _, err = run(argv, capsys)
    assert code == 1
    assert "drifted" in err
    assert calls == []


def _override_vep_run(
    monkeypatch: pytest.MonkeyPatch, returncode: int, body: str | None
) -> None:
    """Replace the fake container's run: exit ``returncode``, write ``body`` if given.

    Call after :func:`_fake_vep_container`, whose docker and mount fakes stay.
    """

    def fake_run(argv: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
        if body is not None:
            work = next(
                Path(v.removesuffix(f":{vep.WORK_MOUNT}"))
                for v in argv
                if v.endswith(f":{vep.WORK_MOUNT}")
            )
            (work / testdir.ORACLE_NAME).write_text("##VEP=v116\n#CHROM\n" + body)
        return subprocess.CompletedProcess(argv, returncode, "", "")

    monkeypatch.setattr(vep.subprocess, "run", fake_run)


def _reproduce_failure(
    returncode: int,
    body: str | None,
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Run ``--check --reproduce`` against a faulty fake container; return stderr.

    The committed oracle is untouched, so the drift check passes first and the
    failure comes from the reproduce step itself. Asserts exit code 1.
    """
    _set_vep(test_dir, vep.vep_command(()))
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    _override_vep_run(monkeypatch, returncode, body)
    code, _, err = run(
        [
            "--check",
            "--reproduce",
            *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
        ],
        capsys,
    )
    assert code == 1, err
    return err


def test_reproduce_fails_on_md5_mismatch(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh body whose md5 differs from [compare] body_md5 exits 1."""
    err = _reproduce_failure(
        0, BODY + "x", complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
    )
    assert "reproduction failed" in err


def test_reproduce_fails_on_vep_nonzero_exit(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A VEP container exiting non-zero fails the reproduce with its exit code."""
    err = _reproduce_failure(
        3, BODY, complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
    )
    assert "exited with 3" in err


def test_reproduce_fails_on_missing_output(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A VEP container exiting 0 without writing the oracle fails the reproduce."""
    err = _reproduce_failure(
        0, None, complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
    )
    assert "wrote no expected_output.vcf" in err


def test_reproduce_refuses_tampered_command(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A [vep] command not generated from [vep] extra_flags exits 1, no run.

    The image is validly pinned and extra_flags is allowed, so the command is
    the only cause; the untampered file exits 0.
    """
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    argv = [
        "--check",
        "--reproduce",
        *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
    ]
    for command in (
        vep.vep_command(),  # valid-looking, but drops the listed flag
        "vep --offline --cache --fa /x",  # not the fixed prefix
        vep.vep_command().replace("--offline ", "") + " --check_existing",
        vep.vep_command() + " '--check_existing'",  # quoted: not canonical
        vep.vep_command() + "  --check_existing",  # extra whitespace
        vep.vep_command(("--check_existing",)) + " ",  # trailing whitespace
    ):
        _set_flags(test_dir, ["--check_existing"], command)
        code, _, err = run(argv, capsys)
        assert code == 1, command
        assert "test.toml" in err and "[vep] command" in err, err
    assert calls == []
    _set_flags(test_dir, ["--check_existing"])
    code, _, err = run(argv, capsys)
    assert code == 0, err


def test_reproduce_refuses_unlisted_extra_flag(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """extra_flags outside the allowlist, repeated or mistyped exits 1, no run.

    ``command`` is regenerated to match, so the list is the only cause.
    """
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    argv = [
        "--check",
        "--reproduce",
        *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
    ]
    for flags in (["--fa"], ["--check_existing", "--check_existing"]):
        _set_flags(test_dir, flags)
        code, _, err = run(argv, capsys)
        assert code == 1, flags
        assert "test.toml" in err and "[vep] extra_flags" in err, err
    toml = test_dir / "test.toml"
    good = toml.read_text().replace(
        'extra_flags = ["--check_existing", "--check_existing"]',
        'extra_flags = ["--check_existing"]',
    )
    for bad in ('extra_flags = "--check_existing"', "extra_flags = [1]"):
        toml.write_text(good.replace('extra_flags = ["--check_existing"]', bad))
        code, _, err = run(argv, capsys)
        assert code == 1 and "must be an array of strings" in err, err
    assert calls == []
    _set_flags(test_dir, ["--check_existing"])
    code, _, err = run(argv, capsys)
    assert code == 0, err


def test_cli_flag_and_list_must_agree(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A re-bless uses the recorded list; a typed flag must equal it."""
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    argv = _bless_argv(test_dir, complete_cache, fasta, tmp_path)
    code, _, err = run(["--vep-flag=--check_existing", *argv], capsys)
    assert code == 0, err
    assert tomllib.loads((test_dir / "test.toml").read_text())["vep"][
        "extra_flags"
    ] == ["--check_existing"]
    code, _, err = run(["--vep-flag=--check_existing", *argv], capsys)
    assert code == 0, err
    code, _, err = run(argv, capsys)  # no CLI flag: the list is used
    assert code == 0, err
    assert _vep_part(calls[-1]) == [*vep.VEP_ARGV, "--check_existing"]
    toml = test_dir / "test.toml"
    # A second allowed flag, so that the recorded list is valid but differs.
    monkeypatch.setattr(vep, "ALLOWED_VEP_FLAGS", ("--check_existing", "--x"))
    _set_flags(test_dir, ["--x"])
    before = toml.read_bytes()
    code, _, err = run(["--vep-flag=--check_existing", *argv], capsys)
    assert code == 1, err
    assert "--vep-flag=" in err and "[vep] extra_flags" in err, err
    assert toml.read_bytes() == before


def test_check_refuses_typed_flags(
    test_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--vep-flag with --check (plain or --reproduce) exits 1, naming the option.

    The image is validly pinned and the command is the fixed one, so the typed
    flag is the only reason for the refusal.
    """
    _set_vep(test_dir, vep.vep_command())
    base = ["--vep-flag=--check_existing", str(test_dir)]
    for mode in (["--check"], ["--check", "--reproduce", "--vep-cache-dir", "/c"]):
        code, _, err = run([*mode, "--vep-fasta", "/x.fa", *base], capsys)
        assert code == 1 and err.startswith("bless: --vep-flag is refused with --check")
    code, _, err = run(["--check", str(test_dir)], capsys)
    assert code == 0, err


def test_vep_flag_rejections(capsys: pytest.CaptureFixture[str]) -> None:
    """parse_extra accepts only exact allowlist tokens, each once.

    Also: every allowlist entry is a boolean ``--name`` flag, and the bare
    ``--vep-flag <flag>`` form is refused with a message naming the ``=`` form.
    """
    for allowed in vep.ALLOWED_VEP_FLAGS:
        assert allowed.startswith("--"), allowed
        assert "=" not in allowed and not any(c.isspace() for c in allowed), allowed
    with pytest.raises(SystemExit) as stop:
        cli.main(["--dry-run", "--vep-flag", "--check_existing", "x"])
    assert stop.value.code == 2
    assert "--vep-flag needs the = form" in capsys.readouterr().err
    assert vep.parse_extra(["--check_existing"]) == ("--check_existing",)
    assert vep.parse_extra([]) == ()
    for flag in (
        "--fa",
        "--input_f",
        "-i",
        "--input_file",
        "--output_file",
        "--fasta",
        "--force",
        "--vcf",
        "--not_a_vep_flag",
        "--check_existing=1",
    ):
        with pytest.raises(BlessError) as exc:
            vep.parse_extra([flag])
        assert str(exc.value) == (
            f"--vep-flag: {flag} is not allowed; allowed: --check_existing, --merged"
        )
    with pytest.raises(BlessError, match="given twice"):
        vep.parse_extra(["--check_existing", "--check_existing"])


# --- One mode: --everything (#143) ----------------------------------------------

README_SECTION: Final[str] = "## One mode: --everything"
README_ROW: Final[re.Pattern[str]] = re.compile(
    r"^\| `(?P<flag>--[a-z_]+)` \| `(?P<key>[a-z_]+) = (?P<value>true|false)` \|"
)


def _readme_mapping_rows() -> set[tuple[str, str, bool]]:
    """The ``(VEP flag, [vepyr] key, value)`` rows of the README mapping table."""
    text = (REPO / "README.md").read_text(encoding="utf-8")
    assert text.count(f"\n{README_SECTION}\n") == 1, "README lacks the section"
    section = text.split(f"\n{README_SECTION}\n", 1)[1].split("\n## ", 1)[0]
    return {
        (m["flag"], m["key"], m["value"] == "true")
        for line in section.splitlines()
        if (m := README_ROW.match(line))
    }


def test_everything_mode_mapping_agrees_with_argv_and_readme() -> None:
    """tools/vep_flags.toml, VEP_ARGV and the README table state the same mapping."""
    rows = vep.mode_mapping()
    flags = [row.vep_flag for row in rows]
    assert "--everything" in flags and "--fasta" in flags
    assert len(set(flags)) == len(flags), "a VEP flag is mapped twice"
    for row in rows:
        assert row.vep_flag in vep.VEP_ARGV, f"{row.vep_flag} is not in VEP_ARGV"
        assert row.vep_flag in shlex.split(vep.vep_command())
    assert _readme_mapping_rows() == {
        (row.vep_flag, row.vepyr_key, row.vepyr_value) for row in rows
    }


def test_everything_mode_mapping_keys_are_loader_keys() -> None:
    """Every mapped setting is a boolean in the Python fixture schema."""
    from run_tests.fixtures import VEPYR_FIELDS

    for row in vep.mode_mapping():
        assert VEPYR_FIELDS[row.vepyr_key] is bool
    assert "fields" not in VEPYR_FIELDS


def test_everything_mode_malformed_mapping_is_refused(tmp_path: Path) -> None:
    """An empty mapping or a row missing a key is a BlessError, not a silent pass."""
    empty = tmp_path / "empty.toml"
    empty.write_text("")
    with pytest.raises(BlessError, match="at least one row"):
        vep.mode_mapping(empty)
    partial = tmp_path / "partial.toml"
    partial.write_text('[[mapping]]\nvep_flag = "--everything"\n')
    with pytest.raises(BlessError, match="malformed"):
        vep.mode_mapping(partial)


@pytest.mark.parametrize(
    ("edit", "named"),
    [
        (("everything = true", "everything = false"), "everything = false"),
        (("reference_fasta = true", "reference_fasta = false"), "reference_fasta"),
        (("[vep]\n", "[[vepyr_run]]\neverything = false\n\n[vep]\n"), "everything"),
        ((VEPYR_TABLE, ""), "[vepyr] is missing"),
    ],
)
def test_everything_mode_bless_refuses_other_modes(
    edit: tuple[str, str],
    named: str,
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bless of a test.toml outside the one mode exits 1 before running VEP."""
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    toml = test_dir / "test.toml"
    before = toml.read_text()
    assert edit[0] in before
    toml.write_text(before.replace(edit[0], edit[1], 1))
    code, _, err = run(_bless_argv(test_dir, complete_cache, fasta, tmp_path), capsys)
    assert code == 1
    assert named in err
    assert calls == [], "VEP ran for an unsupported mode"


def test_everything_mode_reproduce_refuses_old_command(
    complete_cache: Path,
    fasta: Path,
    test_dir: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--check --reproduce refuses a [vep] command made without --everything."""
    calls: list[list[str]] = []
    _fake_vep_container(monkeypatch, calls)
    old = shlex.join(arg for arg in vep.VEP_ARGV if arg != "--everything")
    _set_vep(test_dir, old)
    code, _, err = run(
        [
            "--check",
            "--reproduce",
            *_bless_argv(test_dir, complete_cache, fasta, tmp_path),
        ],
        capsys,
    )
    assert code == 1
    assert "not the canonical command" in err
    assert calls == []


@pytest.mark.parametrize("flavour,flags", [("merged", ()), ("ensembl", ("--merged",))])
def test_cache_flavour_mismatch_is_rejected(flavour, flags):
    with pytest.raises(BlessError, match="cache flavour mismatch"):
        vep.require_cache_mode({"vepyr": {"flavour": flavour}}, flags, where="case")


def test_merged_cache_requires_merged_layout(complete_cache):
    assert ensembl.missing_cache_parts(complete_cache, merged=True) == [
        "homo_sapiens_merged/116_GRCh38/"
    ]
    (complete_cache / "homo_sapiens").rename(complete_cache / "homo_sapiens_merged")
    assert ensembl.missing_cache_parts(complete_cache, merged=True) == []
    assert ensembl.missing_cache_parts(complete_cache) == ["homo_sapiens/116_GRCh38/"]


def test_merged_override_cannot_change_cache():
    config = {"vepyr": {"flavour": "merged"}, "vepyr_run": [{"flavour": "ensembl"}]}
    with pytest.raises(BlessError, match="cache flavour mismatch"):
        vep.require_cache_mode(config, ("--merged",), where="case")


def test_merged_provenance_does_not_reuse_ensembl_receipt(complete_cache):
    (complete_cache / ensembl.SOURCE_RECORD).write_text(
        'source = "ensembl-archive"\nchecksum = "ensembl-sha"\n'
    )
    prov = ensembl.cache_provenance(complete_cache, merged=True)
    assert prov.source.endswith("/homo_sapiens_merged_vep_116_GRCh38.tar.gz")
    assert prov.checksum == "unverified"


@pytest.mark.parametrize("flavour,flags", [("merged", []), ("ensembl", ["--merged"])])
def test_cli_refuses_cache_flavour_mismatch(
    flavour, flags, complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
):
    """Both blessing paths reject a mismatch before any container runs."""
    # Both layouts exist, so missing-cache rejection cannot hide a missing guard.
    shutil.copytree(
        complete_cache / "homo_sapiens", complete_cache / "homo_sapiens_merged"
    )
    _set_flags(test_dir, flags)
    toml = test_dir / "test.toml"
    toml.write_text(testdir.set_keys(toml.read_text(), {"vepyr": {"flavour": flavour}}))
    calls = []
    _fake_vep_container(monkeypatch, calls)
    argv = _bless_argv(test_dir, complete_cache, fasta, tmp_path)
    before = {p.name: p.read_bytes() for p in test_dir.iterdir()}
    for mode in ([], ["--check", "--reproduce"]):
        code, _, err = run([*mode, *argv], capsys)
        assert code == 1 and "cache flavour mismatch" in err
        assert calls == []
        assert {p.name: p.read_bytes() for p in test_dir.iterdir()} == before


def test_cli_blesses_merged_layout_and_records_merged_provenance(
    complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
):
    """The CLI selects the merged layout and does not adopt an Ensembl receipt."""
    (complete_cache / "homo_sapiens").rename(complete_cache / "homo_sapiens_merged")
    (complete_cache / ensembl.SOURCE_RECORD).write_text(
        'source = "ensembl-archive"\nchecksum = "ensembl-sha"\n'
    )
    _set_flags(test_dir, ["--merged"])
    toml = test_dir / "test.toml"
    toml.write_text(
        testdir.set_keys(toml.read_text(), {"vepyr": {"flavour": "merged"}})
    )
    calls = []
    _fake_vep_container(monkeypatch, calls)
    argv = _bless_argv(test_dir, complete_cache, fasta, tmp_path)
    code, _, err = run(argv, capsys)
    assert code == 0, err
    recorded = tomllib.loads(toml.read_text())
    assert recorded["vep"]["command"] == vep.vep_command(("--merged",))
    assert recorded["vep"]["cache_source"].startswith(
        "https://huggingface.co/datasets/biodatageeks/vepyr_116_GRCh38_merged/tree/"
    )
    assert (
        recorded["vep"]["vep_cache"]
        == ensembl.cache_provenance(complete_cache, merged=True).source
    )
    assert recorded["vep"]["vep_cache_checksum"] == "unverified"
    assert _vep_part(calls[-1]) == [*vep.VEP_ARGV, "--merged"]
    assert run(["--check", "--reproduce", *argv], capsys)[0] == 0
    assert len(calls) == 2


def test_cli_refuses_merged_cache_override(
    complete_cache, fasta, test_dir, tmp_path, capsys, monkeypatch
):
    """An explicit run override cannot silently compare different cache flavours."""
    _set_flags(test_dir, ["--merged"])
    toml = test_dir / "test.toml"
    toml.write_text(
        testdir.set_keys(toml.read_text(), {"vepyr": {"flavour": "merged"}})
        + '\n[[vepyr_run]]\nflavour = "ensembl"\n'
    )
    calls = []
    _fake_vep_container(monkeypatch, calls)
    code, _, err = run(_bless_argv(test_dir, complete_cache, fasta, tmp_path), capsys)
    assert code == 1 and "cache flavour mismatch" in err
    assert calls == []


def test_docker_copy_must_match_normalized_input(
    test_dir, complete_cache, fasta, tmp_path, monkeypatch
):
    """A changed Docker input must fail before the oracle process can run."""
    import shutil

    original = shutil.copyfile
    called = []

    def corrupt_copy(src, dst):
        original(src, dst)
        Path(dst).write_bytes(Path(dst).read_bytes() + b"changed\n")

    monkeypatch.setattr(cli.shutil, "copyfile", corrupt_copy)
    monkeypatch.setattr(vep, "require_docker", lambda: "docker")
    monkeypatch.setattr(vep, "resolve_digest", lambda *args: "image@sha256:abc")
    monkeypatch.setattr(vep, "require_mountable", lambda *args: None)
    monkeypatch.setattr(ensembl, "ensure_fai", lambda *args: None)
    monkeypatch.setattr(vep, "run", lambda *args: called.append(args))
    with pytest.raises(BlessError, match="Docker input copy differs"):
        cli._run_vep(
            testdir.load(test_dir),
            cli.Source(path=complete_cache, flag="--vep-cache-dir", download=False),
            cli.Source(path=fasta, flag="--vep-fasta", download=False),
            image=None,
            extra=(),
            work_root=tmp_path / "work",
            dry_run=False,
        )
    assert not called
