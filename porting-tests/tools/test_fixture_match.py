"""Tests for ``tools/fixture_match.py`` (#171).

Fixtures are small literal VCFs written to ``tmp_path``; URL cases use a local
``http.server`` thread or an unreachable local port, never the network.
"""

from __future__ import annotations

import functools
import gzip
import http.server
import shutil
import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pytest

from fixture_match import Exit, InputError, main, rust_const_vcf

REPO: Final[Path] = Path(__file__).resolve().parent.parent
HEADER: Final[str] = (
    "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)
ROWS: Final[tuple[str, ...]] = (
    "21\t100\trs1\tA\tC\t.\t.\t.\n",
    "21\t200\t.\tG\tT\t.\t.\t.\n",
    "21\t300\trs3\tCA\tC\t.\t.\t.\n",
)
UNREACHABLE: Final[str] = "http://127.0.0.1:9/x.vcf"
NEGATIVE_LINE: Final[str] = (
    "PASS fixture-match-negative: mutated copy -> FAIL fixture-match first 1: "
    "record 1: input ('21', 100, 'rs1', 'A', 'CA') "
    "!= fixture ('21', 100, 'rs1', 'A', 'C')"
)


def vcf(tmp: Path, name: str, *rows: str) -> Path:
    """Write ``HEADER`` + ``rows`` to ``tmp/name`` and return the path."""
    path = tmp / name
    path.write_text(HEADER + "".join(rows), encoding="utf-8")
    return path


@pytest.fixture
def inp(tmp_path: Path) -> Path:
    """An input VCF with the three ``ROWS``."""
    return vcf(tmp_path, "input.vcf", *ROWS)


def run(capsys: pytest.CaptureFixture[str], *argv: str | Path) -> tuple[int, list[str]]:
    """``main(argv)`` -> (exit code, stdout lines)."""
    code = main([str(a) for a in argv])
    return code, capsys.readouterr().out.splitlines()


def test_identical_fixture_passes(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    fx = vcf(tmp_path, "fx.vcf", *ROWS)
    code, out = run(capsys, "--input", inp, "--fixture", fx, "--records", "3")
    assert (code, out) == (
        Exit.OK,
        ["PASS fixture-match first 3: CHROM,POS,ID,REF,ALT identical"],
    )


@pytest.mark.parametrize(
    ("row", "what"),
    [
        ("21\t300\trs3\tCA\tCT\t.\t.\t.\n", "ALT"),
        ("21\t300\trsX\tCA\tC\t.\t.\t.\n", "ID"),
        ("21\t301\trs3\tCA\tC\t.\t.\t.\n", "POS"),
        ("21\t300\trs3\tGA\tC\t.\t.\t.\n", "REF"),
    ],
)
def test_mutated_column_fails_naming_the_record(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path, row: str, what: str
) -> None:
    fx = vcf(tmp_path, f"fx_{what}.vcf", *ROWS[:2], row)
    code, out = run(capsys, "--input", inp, "--fixture", fx, "--records", "3")
    assert code == Exit.FAIL, what
    assert out[0].startswith(
        "FAIL fixture-match first 3: record 3: input ('21', 300, 'rs3', 'CA', 'C') != "
    )


@pytest.mark.parametrize(("fixture_rows", "n"), [(ROWS, 4), (ROWS[:2], 3)])
def test_too_few_records_fails(
    capsys: pytest.CaptureFixture[str],
    inp: Path,
    tmp_path: Path,
    fixture_rows: tuple[str, ...],
    n: int,
) -> None:
    fx = vcf(tmp_path, "fx.vcf", *fixture_rows)
    code, out = run(capsys, "--input", inp, "--fixture", fx, "--records", str(n))
    assert code == Exit.FAIL
    want = f"input has 3, fixture {len(fixture_rows)} records"
    assert out == [f"FAIL fixture-match first {n}: {want}"]


def test_by_pos_skips_extra_fixture_rows_positional_does_not(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    fx = vcf(
        tmp_path,
        "fx.vcf",
        "21\t1\t.\tA\tC\t.\t.\t.\n",
        ROWS[0],
        "21\t150\t.\tT\tG\t.\t.\t.\n",
        *ROWS[1:],
    )
    assert (
        run(capsys, "--input", inp, "--fixture", fx, "--records", "3")[0] == Exit.FAIL
    )
    assert (
        run(capsys, "--input", inp, "--fixture", fx, "--records", "3", "--by-pos")[0]
        == Exit.OK
    )


def test_by_pos_absent_position_fails(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    fx = vcf(tmp_path, "fx.vcf", *ROWS[:2])
    code, out = run(
        capsys, "--input", inp, "--fixture", fx, "--records", "3", "--by-pos"
    )
    assert code == Exit.FAIL
    assert "fixture ('21', 300, '<absent>', '', '')" in out[0]


def test_gzip_input_and_data_test_directory(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    plain = vcf(tmp_path, "plain.vcf", *ROWS)
    gz = tmp_path / "input.vcf.bin"  # detected by magic bytes, not by the name
    gz.write_bytes(gzip.compress(plain.read_bytes()))
    d = tmp_path / "some_test"
    d.mkdir()
    shutil.copy(plain, d / "input.vcf")
    for target in (gz, d):
        assert (
            run(capsys, "--input", target, "--fixture", plain, "--records", "3")[0]
            == Exit.OK
        )


def test_negative_control_prints_three_pass_lines(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    fx = vcf(tmp_path, "fx.vcf", *ROWS)
    code, out = run(
        capsys, "--input", inp, "--fixture", fx, "--records", "1", "--negative-control"
    )
    assert code == Exit.OK
    assert out == [
        "PASS fixture-match first 1: CHROM,POS,ID,REF,ALT identical",
        NEGATIVE_LINE,
        "PASS summary: 2/2 checks passed",
    ]


def test_negative_control_not_run_when_positive_fails(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    fx = vcf(tmp_path, "fx.vcf", "21\t100\trs1\tA\tG\t.\t.\t.\n")
    code, out = run(
        capsys, "--input", inp, "--fixture", fx, "--records", "1", "--negative-control"
    )
    assert code == Exit.FAIL
    assert (
        out[1] == "FAIL fixture-match-negative: not run: "
        "positive check 'fixture-match first 1' failed"
    )
    assert out[2].startswith("FAIL summary: 0/2 checks passed")


@pytest.mark.parametrize(
    "argv_tail",
    [
        ("--records", "0"),
        ("--records", "x"),
    ],
)
def test_records_below_one_is_usage(inp: Path, argv_tail: tuple[str, ...]) -> None:
    with pytest.raises(SystemExit) as e:
        main(["--input", str(inp), "--fixture", str(inp), *argv_tail])
    assert e.value.code == Exit.USAGE


@pytest.mark.parametrize(
    "spec", ["git:x:y:z", "git:https://example.org/r.git:main:a.vcf"]
)
def test_git_spec_is_rejected(inp: Path, spec: str) -> None:
    assert (
        main(["--input", str(inp), "--fixture", spec, "--records", "1"]) == Exit.USAGE
    )


def test_missing_file_and_non_vcf_text_are_usage(inp: Path, tmp_path: Path) -> None:
    junk = tmp_path / "junk.txt"
    junk.write_text("hello world\n", encoding="utf-8")
    for fx in (tmp_path / "missing.vcf", junk):
        assert (
            main(["--input", str(inp), "--fixture", str(fx), "--records", "1"])
            == Exit.USAGE
        )
    assert (
        main(
            ["--input", str(tmp_path / "nope"), "--fixture", str(inp), "--records", "1"]
        )
        == Exit.USAGE
    )


def test_unreachable_url_is_exit3(
    capsys: pytest.CaptureFixture[str], inp: Path
) -> None:
    assert (
        main(["--input", str(inp), "--fixture", UNREACHABLE, "--records", "1"])
        == Exit.ERROR
    )
    assert "cannot fetch" in capsys.readouterr().err


@pytest.fixture
def served(tmp_path: Path) -> Iterator[str]:
    """Base URL of a local HTTP server over ``tmp_path/www``."""
    www = tmp_path / "www"
    www.mkdir()
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(www)
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def test_url_fixture(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path, served: str
) -> None:
    vcf(tmp_path / "www", "fx.vcf", *ROWS)
    assert (
        run(capsys, "--input", inp, "--fixture", f"{served}/fx.vcf", "--records", "3")[
            0
        ]
        == Exit.OK
    )
    assert (
        main(
            [
                "--input",
                str(inp),
                "--fixture",
                f"{served}/missing.vcf",
                "--records",
                "1",
            ]
        )
        == Exit.ERROR
    )


RUST_SOURCE: Final[str] = r"""
#[cfg(test)]
mod tests {
    const OTHER: &str = "not this";
    const VCF: &str = "##fileformat=VCFv4.2\n\
        #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n\
        21\t100\trs1\tA\tC\t.\t.\tNOTE=\"q\\x\"\n\
        21\t200\t.\tG\tT\t.\t.\t.\n";
    const RAW: &str = r#"21	100	rs1	A	C"#;
}
"""


def test_rust_const_escapes_and_continuations() -> None:
    assert rust_const_vcf(RUST_SOURCE, "VCF") == (
        HEADER
        + '21\t100\trs1\tA\tC\t.\t.\tNOTE="q\\x"\n'
        + "21\t200\t.\tG\tT\t.\t.\t.\n"
    )
    assert rust_const_vcf('const S: &\'static str = "a\\\n   b";', "S") == "ab"


def test_rust_const_fixture_matches(
    capsys: pytest.CaptureFixture[str], inp: Path, tmp_path: Path
) -> None:
    src = tmp_path / "test.rs"
    src.write_text(RUST_SOURCE, encoding="utf-8")
    code, out = run(
        capsys,
        "--input",
        inp,
        "--fixture",
        src,
        "--records",
        "2",
        "--rust-const",
        "VCF",
    )
    assert (code, out) == (
        Exit.OK,
        ["PASS fixture-match first 2: CHROM,POS,ID,REF,ALT identical"],
    )


@pytest.mark.parametrize(
    ("name", "why"), [("MISSING", "no `const MISSING"), ("RAW", "raw string")]
)
def test_rust_const_missing_or_raw_is_usage(
    tmp_path: Path, inp: Path, name: str, why: str
) -> None:
    with pytest.raises(InputError, match=why):
        rust_const_vcf(RUST_SOURCE, name)
    src = tmp_path / "test.rs"
    src.write_text(RUST_SOURCE, encoding="utf-8")
    assert (
        main(
            [
                "--input",
                str(inp),
                "--fixture",
                str(src),
                "--records",
                "1",
                "--rust-const",
                name,
            ]
        )
        == Exit.USAGE
    )


def test_launcher_runs_the_module(inp: Path) -> None:
    proc = subprocess.run(
        [
            REPO / "tools" / "fixture_match",
            "--input",
            inp,
            "--fixture",
            inp,
            "--records",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (proc.returncode, proc.stdout) == (
        0,
        "PASS fixture-match first 1: CHROM,POS,ID,REF,ALT identical\n",
    )
