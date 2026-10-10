"""ci/ci_helpers.py: the logic the workflows rely on, tested off-CI."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ci"))
import ci_helpers  # noqa: E402

X86 = "vepyr-0.9.2-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
ARM = "vepyr-0.9.2-cp310-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"


def touch(directory: Path, name: str) -> Path:
    path = directory / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


def test_find_wheel_returns_the_single_x86_64_wheel(tmp_path):
    expected = touch(tmp_path / "wheels-manylinux-x86_64", X86)
    touch(tmp_path, ARM)
    assert ci_helpers.find_wheel(tmp_path) == expected


@pytest.mark.parametrize("names", [[], [ARM], [X86, X86.replace("0.9.2", "0.9.3")]])
def test_find_wheel_refuses_zero_or_ambiguous(tmp_path, names):
    for name in names:
        touch(tmp_path, name)
    with pytest.raises(ci_helpers.CiError, match="exactly one"):
        ci_helpers.find_wheel(tmp_path)


def test_wheel_version_reads_the_file_name():
    assert ci_helpers.wheel_version(Path(X86)) == "0.9.2"


def test_wheel_version_rejects_other_files():
    with pytest.raises(ci_helpers.CiError):
        ci_helpers.wheel_version(Path("polars-1.0-py3-none-any.whl"))


@pytest.mark.parametrize(
    ("outcome", "conclusion"),
    [
        ("success", "success"),
        ("failure", "failure"),
        ("cancelled", "error"),
        ("skipped", "error"),
        ("", "error"),
    ],
)
def test_result_record_maps_step_outcomes(outcome, conclusion):
    record = ci_helpers.result_record("vep-parity", outcome, "s")
    assert record == {"name": "vep-parity", "conclusion": conclusion, "summary": "s"}


def test_cli_result_writes_json(tmp_path):
    out = tmp_path / "result-vep-parity.json"
    code = ci_helpers.main(
        [
            "result",
            "--name",
            "vep-parity",
            "--outcome",
            "success",
            "--summary",
            "204 pass",
            "--out",
            str(out),
        ]
    )
    assert code == 0
    assert json.loads(out.read_text())["conclusion"] == "success"


def test_cli_error_exits_nonzero(tmp_path, capsys):
    assert ci_helpers.main(["find-wheel", str(tmp_path)]) == 1
    assert "exactly one" in capsys.readouterr().err


def test_find_wheel_returns_absolute_path_for_relative_dir(tmp_path, monkeypatch):
    touch(tmp_path / "dist", X86)
    monkeypatch.chdir(tmp_path)
    found = ci_helpers.find_wheel(Path("dist"))
    assert found.is_absolute()
    assert found.is_file()


def test_profiles_reads_manifest_keys_sorted(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"profiles": {"refseq": {}, "ensembl": {}, "merged": {}}})
    )
    assert ci_helpers.profiles(manifest) == ["ensembl", "merged", "refseq"]


@pytest.mark.parametrize("body", [{}, {"profiles": {}}])
def test_profiles_refuses_empty(tmp_path, body):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(body))
    with pytest.raises(ci_helpers.CiError, match="no profiles"):
        ci_helpers.profiles(manifest)


def test_profiles_cli_prints_json(tmp_path, capsys):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"profiles": {"merged": {}}}))
    assert ci_helpers.main(["profiles", str(manifest)]) == 0
    assert json.loads(capsys.readouterr().out) == ["merged"]


def test_profiles_matches_the_committed_manifest():
    manifest = (
        Path(__file__).resolve().parents[1]
        / "e2e-testing/golden/116/chr22/manifest.json"
    )
    assert len(ci_helpers.profiles(manifest)) == 10


ENV_YML = """---
# yaml-language-server: $schema=https://example/schema.json
channels:
  - conda-forge
  - bioconda
dependencies:
  # renovate: datasource=conda depName=bioconda/htslib
  - bioconda::htslib=1.24
  # renovate: datasource=conda depName=bioconda/vepyr
  - bioconda::vepyr=0.9.0
"""


def test_conda_specs_drops_vepyr_and_channels(tmp_path):
    env = tmp_path / "environment.yml"
    env.write_text(ENV_YML)
    assert ci_helpers.conda_specs(env) == ["bioconda::htslib=1.24"]


def test_conda_specs_of_the_committed_module():
    env = (
        Path(__file__).resolve().parents[1]
        / "nf-core-module/modules/nf-core/vepyr/annotate/environment.yml"
    )
    specs = ci_helpers.conda_specs(env)
    assert specs and not any("vepyr" in s for s in specs)


SNAP = {
    "homo_sapiens - vcf": {
        "content": [
            {
                "vcf": ["test_vepyr.vcf.gz,variantsMD5:fdddbf25"],
                "versions_vepyr": [["VEPYR_ANNOTATE", "vepyr", "0.9.0"]],
            }
        ],
        "meta": {"nf-test": "0.9.2", "nextflow": "25.04.6"},
    },
    "stub": {
        "content": [
            {
                "versions_vepyr": [["VEPYR_ANNOTATE", "vepyr", "0.9.0"]],
                "versions_bcftools": [["BCFTOOLS_NORM", "bcftools", "1.22"]],
            }
        ]
    },
}


def test_set_snapshot_version_rewrites_only_vepyr():
    out = json.loads(ci_helpers.set_snapshot_version(json.dumps(SNAP), "0.9.3"))
    first = out["homo_sapiens - vcf"]["content"][0]
    assert first["versions_vepyr"] == [["VEPYR_ANNOTATE", "vepyr", "0.9.3"]]
    assert out["stub"]["content"][0]["versions_bcftools"] == [
        ["BCFTOOLS_NORM", "bcftools", "1.22"]
    ]
    assert first["vcf"] == ["test_vepyr.vcf.gz,variantsMD5:fdddbf25"]
    assert out["homo_sapiens - vcf"]["meta"] == SNAP["homo_sapiens - vcf"]["meta"]


def test_set_snapshot_version_fails_when_nothing_matches():
    with pytest.raises(ci_helpers.CiError, match="no .*vepyr"):
        ci_helpers.set_snapshot_version(json.dumps({"t": {"content": [1]}}), "0.9.3")


def test_snap_version_cli_rewrites_in_place(tmp_path):
    snap = tmp_path / "main.nf.test.snap"
    snap.write_text(json.dumps(SNAP))
    assert (
        ci_helpers.main(["snap-version", "--snap", str(snap), "--version", "1.0.0"])
        == 0
    )
    assert '"1.0.0"' in snap.read_text() and '"0.9.0"' not in snap.read_text()


def write_result(
    directory: Path, name: str, conclusion: str, summary: str = ""
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    safe = name.replace("/", "-")
    (directory / f"result-{safe}.json").write_text(
        json.dumps({"name": name, "conclusion": conclusion, "summary": summary})
    )


OK = {"vep-parity": "success", "integration": "success"}


def as_dict(rows):
    return {ctx: (state, desc) for ctx, state, desc in rows}


def test_statuses_all_green(tmp_path):
    write_result(tmp_path / "a", "vep-parity", "success", "204 pass")
    for p in ("ensembl", "merged"):
        write_result(tmp_path / p, f"integration/{p}", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"], OK))
    assert rows["parity/vep-parity"][0] == "success"
    assert rows["parity/integration"][0] == "success"
    assert rows["parity/integration/merged"][0] == "success"


def test_statuses_failed_profile_fails_aggregate_and_is_named(tmp_path):
    write_result(tmp_path, "vep-parity", "success")
    write_result(tmp_path, "integration/ensembl", "success")
    write_result(tmp_path, "integration/merged", "failure", "md5 strict: MISMATCH")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"], OK))
    assert rows["parity/integration/merged"] == ("failure", "md5 strict: MISMATCH")
    assert rows["parity/integration"][0] == "failure"
    assert "merged" in rows["parity/integration"][1]


def test_statuses_missing_result_is_error_never_success(tmp_path):
    write_result(tmp_path, "integration/ensembl", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["ensembl", "merged"], OK))
    assert rows["parity/vep-parity"][0] == "error"
    assert rows["parity/integration/merged"][0] == "error"
    assert rows["parity/integration"][0] != "success"


def test_statuses_without_profile_list_is_error(tmp_path):
    write_result(tmp_path, "vep-parity", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, [], OK))
    assert rows["parity/integration"][0] == "error"


def test_statuses_descriptions_fit_github_limit(tmp_path):
    write_result(tmp_path, "vep-parity", "failure", "x" * 500)
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"], OK))
    assert all(len(desc) <= 140 for _, desc in rows.values())


def test_statuses_cli_tab_separated(tmp_path, capsys):
    write_result(tmp_path, "vep-parity", "success")
    write_result(tmp_path, "integration/merged", "success")
    assert (
        ci_helpers.main(
            [
                "statuses",
                "--results-dir",
                str(tmp_path),
                "--profiles",
                '["merged"]',
                "--vep-parity-job",
                "success",
                "--integration-job",
                "success",
            ]
        )
        == 0
    )
    lines = capsys.readouterr().out.strip().splitlines()
    assert {line.split("\t")[0] for line in lines} == {
        "parity/vep-parity",
        "parity/integration/merged",
        "parity/integration",
    }


def cli_statuses(tmp_path, profiles, vep_parity="success", integration="success"):
    return ci_helpers.main(
        [
            "statuses",
            "--results-dir",
            str(tmp_path),
            "--profiles",
            profiles,
            "--vep-parity-job",
            vep_parity,
            "--integration-job",
            integration,
        ]
    )


def test_statuses_cli_empty_profiles_is_error(tmp_path, capsys):
    write_result(tmp_path, "vep-parity", "success")
    assert cli_statuses(tmp_path, "") == 0
    out = {
        ln.split("\t")[0]: ln.split("\t")[1]
        for ln in capsys.readouterr().out.splitlines()
    }
    assert out["parity/integration"] == "error"


@pytest.mark.parametrize(
    "bad", ['"merged"', '{"a": 1}', "not json", '["a\\tb"]', '["a\\nb"]', '[""]', "[1]"]
)
def test_statuses_cli_rejects_bad_profiles(tmp_path, bad):
    assert cli_statuses(tmp_path, bad) == 1


def test_statuses_rejects_forged_profile_name(tmp_path):
    with pytest.raises(ci_helpers.CiError):
        ci_helpers.statuses(tmp_path, ["a\tsuccess\tx\nparity/vep-parity"], OK)


def test_statuses_unknown_conclusion_is_error(tmp_path):
    write_result(tmp_path, "vep-parity", "skipped")
    write_result(tmp_path, "integration/a", "neutral")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"], OK))
    assert rows["parity/vep-parity"][0] == "error"
    assert rows["parity/integration/a"][0] == "error"
    assert rows["parity/integration"][0] != "success"


def test_statuses_malformed_results_do_not_crash(tmp_path):
    write_result(tmp_path, "integration/a", "success")
    (tmp_path / "result-bad.json").write_text("{not json")
    (tmp_path / "result-noname.json").write_text(json.dumps({"conclusion": "success"}))
    (tmp_path / "result-nocon.json").write_text(json.dumps({"name": "vep-parity"}))
    (tmp_path / "result-list.json").write_text("[1]")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"], OK))
    assert rows["parity/vep-parity"][0] == "error"
    assert rows["parity/integration"][0] == "success"


def test_statuses_duplicate_result_is_error(tmp_path):
    write_result(tmp_path / "x", "vep-parity", "success")
    write_result(tmp_path / "y", "vep-parity", "success")
    write_result(tmp_path, "integration/a", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"], OK))
    assert rows["parity/vep-parity"] == ("error", "duplicate result for vep-parity")


def test_statuses_job_result_must_confirm_success(tmp_path):
    write_result(tmp_path, "vep-parity", "success")
    write_result(tmp_path, "integration/a", "success")
    rows = as_dict(
        ci_helpers.statuses(
            tmp_path, ["a"], {"vep-parity": "cancelled", "integration": "failure"}
        )
    )
    assert rows["parity/vep-parity"] == (
        "error",
        "job result cancelled contradicts verdict",
    )
    assert rows["parity/integration"] == (
        "error",
        "job result failure contradicts verdict",
    )
    assert (
        rows["parity/integration/a"][0] == "success"
    )  # per-profile stays record-based


def test_statuses_failure_verdict_survives_job_mismatch(tmp_path):
    write_result(tmp_path, "vep-parity", "failure", "3 fail")
    write_result(tmp_path, "integration/a", "failure")
    rows = as_dict(
        ci_helpers.statuses(
            tmp_path, ["a"], {"vep-parity": "failure", "integration": "failure"}
        )
    )
    assert rows["parity/vep-parity"] == ("failure", "3 fail")
    assert rows["parity/integration"][0] == "failure"


def test_statuses_missing_job_results_never_success(tmp_path):
    write_result(tmp_path, "vep-parity", "success")
    write_result(tmp_path, "integration/a", "success")
    rows = as_dict(ci_helpers.statuses(tmp_path, ["a"], {}))
    assert rows["parity/vep-parity"][0] == "error"
    assert rows["parity/integration"][0] == "error"


@pytest.mark.parametrize("text", ["/parity", " /parity", "/parity\n", "\t/parity \r\n"])
def test_is_parity_command_accepts_exactly_the_command(text):
    assert ci_helpers.is_parity_command(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "",
        "   ",
        "/parity please",
        "/parityx",
        "/Parity",
        "/PARITY",
        "parity",
        "> /parity",
        "`/parity`",
        '"/parity"',
        "/parity\nand more",
        "lgtm\n/parity",
        "/ parity",
        "/parity ",
    ],
)
def test_is_parity_command_rejects_anything_else(text):
    assert ci_helpers.is_parity_command(text) is False


def test_main_is_parity_command_from_env(monkeypatch):
    monkeypatch.setenv("COMMENT_BODY", "/parity\n")
    assert ci_helpers.main(["is-parity-command", "--env", "COMMENT_BODY"]) == 0
    monkeypatch.setenv("COMMENT_BODY", "/parity now")
    assert ci_helpers.main(["is-parity-command", "--env", "COMMENT_BODY"]) == 1
    monkeypatch.delenv("COMMENT_BODY")
    assert ci_helpers.main(["is-parity-command", "--env", "COMMENT_BODY"]) == 1


def test_main_is_parity_command_from_stdin(monkeypatch):
    import io

    monkeypatch.setattr(sys, "stdin", io.StringIO("/parity"))
    assert ci_helpers.main(["is-parity-command"]) == 0
    monkeypatch.setattr(sys, "stdin", io.StringIO("/parity?"))
    assert ci_helpers.main(["is-parity-command"]) == 1
