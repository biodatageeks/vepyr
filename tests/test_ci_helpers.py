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


def test_profiles_reads_core_and_clinvar_profiles_in_stable_order(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "profiles": {"refseq": {}, "ensembl": {}, "merged": {}},
                "clinvar_profiles": {"clinvar_merged": {}},
            }
        )
    )
    assert ci_helpers.profiles(manifest) == [
        "merged",
        "ensembl",
        "refseq",
        "clinvar_merged",
    ]


@pytest.mark.parametrize("body", [{}, {"profiles": {}}])
def test_profiles_refuses_empty(tmp_path, body):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(body))
    with pytest.raises(ci_helpers.CiError, match="no profiles"):
        ci_helpers.profiles(manifest)


@pytest.mark.parametrize("bad", ["a b", "x;rm", "", "../up", "ok\n"])
def test_profiles_refuses_invalid_names(tmp_path, bad):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"profiles": {"good": {}, bad: {}}}))
    with pytest.raises(ci_helpers.CiError, match="invalid profile name"):
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


def test_profiles_includes_clinvar_from_the_wgs_manifest():
    manifest = (
        Path(__file__).resolve().parents[1] / "e2e-testing/golden/wgs-manifest.json"
    )
    assert ci_helpers.profiles(manifest) == [
        "merged",
        "ensembl",
        "refseq",
        "clinvar_merged",
    ]


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


@pytest.mark.parametrize("text", ["/parity", "/parity\n", "/parity \r\n", "/parity\t "])
def test_is_parity_command_accepts_exactly_the_command(text):
    assert ci_helpers.is_parity_command(text) is True


@pytest.mark.parametrize(
    "text",
    [
        "",
        "   ",
        " /parity",
        "\t/parity",
        "\n/parity",
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
        "/parity\u00a0",  # NBSP is not stripped
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


@pytest.mark.parametrize(
    "text",
    [
        "/parity-wgs",
        "/parity-wgs\n",
        "/parity-wgs \r\n",
        "/parity-wgs 3bba0d5",
        "/parity-wgs " + "a" * 40,
    ],
)
def test_is_parity_wgs_command_accepts(text):
    assert ci_helpers.is_parity_wgs_command(text)


@pytest.mark.parametrize(
    "text",
    [
        "",
        "/parity",
        " /parity-wgs",
        "/parity-wgs 115",
        "/parity-wgsx",
        "`/parity-wgs`",
        "/parity-wgs\nplease",
        "/parity-wgs 3bba0d",  # too short
        "/parity-wgs 3BBA0D5",  # not lowercase
        "/parity-wgs  3bba0d5",
        "/parity-wgs 3bba0d5 extra",
        "/parity-wgs " + "a" * 41,
        "/parity-wgs main",
    ],
)
def test_is_parity_wgs_command_rejects(text):
    assert not ci_helpers.is_parity_wgs_command(text)
    assert ci_helpers.parity_wgs_pin(text) is None


def test_parity_wgs_pin_values():
    assert ci_helpers.parity_wgs_pin("/parity-wgs") == ""
    assert ci_helpers.parity_wgs_pin("/parity-wgs 3bba0d5\n") == "3bba0d5"


def test_parity_wgs_pin_cli(monkeypatch, capsys):
    monkeypatch.setenv("BODY", "/parity-wgs 3bba0d5")
    assert ci_helpers.main(["parity-wgs-pin", "--env", "BODY"]) == 0
    assert capsys.readouterr().out == "3bba0d5\n"
    monkeypatch.setenv("BODY", "/parity-wgs")
    assert ci_helpers.main(["parity-wgs-pin", "--env", "BODY"]) == 0
    assert capsys.readouterr().out == "\n"
    monkeypatch.setenv("BODY", "/parity-wgs now")
    assert ci_helpers.main(["parity-wgs-pin", "--env", "BODY"]) == 1


def test_parity_and_wgs_commands_do_not_overlap():
    assert not ci_helpers.is_parity_command("/parity-wgs")
    assert not ci_helpers.is_parity_wgs_command("/parity")


def test_wgs_statuses_all_green(tmp_path):
    profiles = ["merged", "ensembl", "refseq", "clinvar_merged"]
    for p in profiles:
        write_result(tmp_path, f"wgs/{p}", "success", "md5 strict: body MATCH")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, profiles, "success"))
    assert rows["parity/wgs"] == ("success", "4 profiles pass")
    assert rows["parity/wgs/merged"][0] == "success"
    assert rows["parity/wgs/clinvar_merged"][0] == "success"


def test_wgs_statuses_missing_record_is_error(tmp_path):
    write_result(tmp_path, "wgs/ensembl", "success")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "failure"))
    assert rows["parity/wgs/merged"][0] == "error"
    assert rows["parity/wgs"][0] != "success"


def test_wgs_statuses_cancelled_job_never_success(tmp_path):
    for p in ("ensembl", "merged"):
        write_result(tmp_path, f"wgs/{p}", "success")
    rows = as_dict(
        ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "cancelled")
    )
    assert rows["parity/wgs"][0] == "error"


def test_wgs_statuses_error_beats_failure(tmp_path):
    # Stale data on one profile must not read as a parity mismatch.
    write_result(tmp_path, "wgs/ensembl", "failure", "FAIL: body digest differs")
    write_result(tmp_path, "wgs/merged", "error", "staged data differs")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "failure"))
    assert rows["parity/wgs"] == ("error", "1 of 2 profiles errored: merged")
    assert rows["parity/wgs/ensembl"][0] == "failure"


def test_wgs_statuses_mismatch_is_failure(tmp_path):
    write_result(tmp_path, "wgs/ensembl", "success")
    write_result(tmp_path, "wgs/merged", "failure", "FAIL: body digest differs")
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, ["ensembl", "merged"], "failure"))
    assert rows["parity/wgs"][0] == "failure"


def test_parity_statuses_keep_failure_aggregate(tmp_path):
    # error_wins is /parity-wgs only: the /parity integration aggregate is unchanged.
    rows = ci_helpers._tier_rows(
        {"integration/a": {"conclusion": "error", "summary": "x"}},
        set(),
        context="parity/integration",
        record_prefix="integration/",
        profiles=["a"],
        job_result="failure",
    )
    assert rows[-1][1] == "failure"


def test_wgs_statuses_empty_profiles_is_error(tmp_path):
    rows = as_dict(ci_helpers.wgs_statuses(tmp_path, [], "success"))
    assert rows["parity/wgs"][0] == "error"


def test_wgs_statuses_cli(tmp_path, capsys):
    write_result(tmp_path, "wgs/merged", "success")
    assert (
        ci_helpers.main(
            [
                "wgs-statuses",
                "--results-dir",
                str(tmp_path),
                "--profiles",
                '["merged"]',
                "--job",
                "success",
            ]
        )
        == 0
    )
    ctxs = {
        line.split("\t")[0] for line in capsys.readouterr().out.strip().splitlines()
    }
    assert ctxs == {"parity/wgs/merged", "parity/wgs"}


# ---- /parity-caches ------------------------------------------------------


@pytest.mark.parametrize(
    "text, parsed",
    [
        ("/parity-caches", ("", [])),
        ("/parity-caches\n", ("", [])),
        ("/parity-caches \r\n", ("", [])),
        ("/parity-caches 3bba0d5", ("3bba0d5", [])),
        ("/parity-caches " + "a" * 40, ("a" * 40, [])),
        ("/parity-caches core", ("", ["core"])),
        ("/parity-caches plugins merged", ("", ["plugins", "merged"])),
        ("/parity-caches 3bba0d5 clinvar cadd", ("3bba0d5", ["clinvar", "cadd"])),
        ("/parity-caches plugin_clinvar", ("", ["plugin_clinvar"])),
        # cadd is hex but too short for a pin, so it is a selector
        ("/parity-caches cadd", ("", ["cadd"])),
    ],
)
def test_parity_caches_command_accepts(text, parsed):
    assert ci_helpers.parity_caches_command(text) == parsed


@pytest.mark.parametrize(
    "text",
    [
        "",
        "/parity",
        "/parity-wgs",
        " /parity-caches",
        "/parity-cachesx",
        "/parity-caches-now",
        "`/parity-caches`",
        "/parity-caches\nclinvar",
        "/parity-caches  clinvar",  # two spaces
        "/parity-caches\tclinvar",
        "/parity-caches Clinvar",
        "/parity-caches clinvar;id",
        "/parity-caches $(id)",
        "/parity-caches 3BBA0D5",  # not lowercase, and not a selector
        "/parity-caches 115",
        "/parity-caches core 3bba0d5",  # the pin comes first
        "/parity-caches " + " ".join(["core"] * 17),
    ],
)
def test_parity_caches_command_rejects(text):
    assert ci_helpers.parity_caches_command(text) is None


def test_parity_commands_do_not_overlap_with_caches():
    assert not ci_helpers.is_parity_command("/parity-caches")
    assert not ci_helpers.is_parity_wgs_command("/parity-caches")
    assert ci_helpers.parity_caches_command("/parity") is None
    assert ci_helpers.parity_caches_command("/parity-wgs 3bba0d5") is None


def test_parity_caches_command_cli(monkeypatch, capsys):
    monkeypatch.setenv("BODY", "/parity-caches 3bba0d5 core clinvar")
    assert ci_helpers.main(["parity-caches-command", "--env", "BODY"]) == 0
    assert capsys.readouterr().out == "pin=3bba0d5\nselect=core clinvar\n"
    monkeypatch.setenv("BODY", "/parity-caches")
    assert ci_helpers.main(["parity-caches-command", "--env", "BODY"]) == 0
    assert capsys.readouterr().out == "pin=\nselect=\n"
    monkeypatch.setenv("BODY", "/parity-caches now!")
    assert ci_helpers.main(["parity-caches-command", "--env", "BODY"]) == 1


REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLISHED_PY = REPO_ROOT / "src/vepyr/_published.py"
CACHE_MANIFEST = REPO_ROOT / "e2e-testing/golden/cache-parity-manifest.json"
CACHE_PINS = {
    (116, "GRCh38", "ensembl"): "1" * 40,
    (116, "GRCh38", "merged"): "2" * 40,
    (116, "GRCh38", "plugin_big"): "3" * 40,
    (116, "GRCh38", "plugin_small"): "4" * 40,
    (115, "GRCh38", "refseq"): "5" * 40,
}
CACHE_CONFIG = {
    "release": 116,
    "assembly": "GRCh38",
    "limits": {
        "core": {"timeout_minutes": 480, "memory": "22g"},
        "plugin": {"timeout_minutes": 120, "memory": "20g"},
    },
    "core": {"ensembl": {}, "merged": {"memory": "24g"}},
    "plugins": {"small": {}, "big": {"opt_in": "large", "timeout_minutes": 900}},
}


def test_published_pins_reads_the_committed_file_without_importing_vepyr():
    pins = ci_helpers.published_pins(PUBLISHED_PY)
    namespace: dict = {}
    exec(compile(PUBLISHED_PY.read_text(), str(PUBLISHED_PY), "exec"), namespace)
    assert pins == namespace["PINS"]
    assert (116, "GRCh38", "merged") in pins


@pytest.mark.parametrize(
    "body",
    [
        "X = 1\n",
        "PINS = build()\n",
        "PINS = {(116, 'GRCh38'): 'a'}\n",
        "PINS = {(116, 'GRCh38', 'merged'): 'main'}\n",
        "PINS = [1]\n",
    ],
)
def test_published_pins_refuses_anything_else(tmp_path, body):
    path = tmp_path / "_published.py"
    path.write_text(body)
    with pytest.raises(ci_helpers.CiError):
        ci_helpers.published_pins(path)


def test_cache_targets_default_is_everything_but_opt_in():
    selection = ci_helpers.cache_targets(CACHE_PINS, CACHE_CONFIG, [])
    assert selection["targets"] == ["ensembl", "merged", "plugin_small"]
    assert selection["aggregate"] is True
    assert selection["matrix"] == {
        "include": [
            {"target": "ensembl", "timeout": 480, "memory": "22g"},
            {"target": "merged", "timeout": 480, "memory": "24g"},
            {"target": "plugin_small", "timeout": 120, "memory": "20g"},
        ]
    }


@pytest.mark.parametrize(
    "selectors, targets, aggregate",
    [
        (["core"], ["ensembl", "merged"], False),
        (["plugins"], ["plugin_small"], False),
        (["core", "plugins"], ["ensembl", "merged", "plugin_small"], True),
        (["merged"], ["merged"], False),
        (["small"], ["plugin_small"], False),
        (["plugin_small", "small"], ["plugin_small"], False),
        (["big"], ["plugin_big"], False),  # naming an opt-in plugin opts in
        (["plugins", "big"], ["plugin_big", "plugin_small"], False),
        (
            ["core", "plugins", "big"],
            ["ensembl", "merged", "plugin_big", "plugin_small"],
            True,
        ),
    ],
)
def test_cache_targets_selectors(selectors, targets, aggregate):
    selection = ci_helpers.cache_targets(CACHE_PINS, CACHE_CONFIG, selectors)
    assert selection["targets"] == targets
    assert selection["aggregate"] is aggregate
    limits = {row["target"]: row["timeout"] for row in selection["matrix"]["include"]}
    assert limits.get("plugin_big", 900) == 900


@pytest.mark.parametrize(
    "selectors, message",
    [
        (["refseq"], "unknown selector 'refseq'; use one of: core, plugins, ensembl"),
        (["nope"], "unknown selector 'nope'"),
        (["Big"], "invalid selector"),
        (["a;b"], "invalid selector"),
    ],
)
def test_cache_targets_refuses_unknown_selectors(selectors, message):
    with pytest.raises(ci_helpers.CiError, match=message):
        ci_helpers.cache_targets(CACHE_PINS, CACHE_CONFIG, selectors)


def test_cache_targets_refuses_bad_manifests():
    with pytest.raises(ci_helpers.CiError, match="no cache is pinned"):
        ci_helpers.cache_targets(CACHE_PINS, {**CACHE_CONFIG, "release": 999}, [])
    no_limits = {**CACHE_CONFIG, "limits": {}}
    with pytest.raises(ci_helpers.CiError, match="timeout_minutes must be"):
        ci_helpers.cache_targets(CACHE_PINS, no_limits, ["ensembl"])
    bad_memory = {**CACHE_CONFIG, "core": {"ensembl": {"memory": "22g --privileged"}}}
    with pytest.raises(ci_helpers.CiError, match="memory must look like"):
        ci_helpers.cache_targets(CACHE_PINS, bad_memory, ["ensembl"])
    only_opt_in = {
        **CACHE_CONFIG,
        "plugins": {"small": {"opt_in": "x"}, "big": {"opt_in": "y"}},
    }
    with pytest.raises(ci_helpers.CiError, match="selection is empty"):
        ci_helpers.cache_targets(CACHE_PINS, only_opt_in, ["plugins"])
    forged = {**CACHE_PINS, (116, "GRCh38", "plugin_a b"): "6" * 40}
    with pytest.raises(ci_helpers.CiError, match="invalid cache name"):
        ci_helpers.cache_targets(forged, CACHE_CONFIG, [])


def test_committed_cache_manifest_covers_every_pin():
    """A new pin must be given a raw cache path or a plugin entry on purpose."""
    pins = ci_helpers.published_pins(PUBLISHED_PY)
    manifest = json.loads(CACHE_MANIFEST.read_text())
    release, assembly = manifest["release"], manifest["assembly"]
    names = {n for (r, a, n) in pins if (r, a) == (release, assembly)}
    core = {n for n in names if not n.startswith("plugin_")}
    plugins = {n.removeprefix("plugin_") for n in names - core}
    assert set(manifest["core"]) == core
    assert all(manifest["core"][c]["raw"] for c in core)
    assert set(manifest["plugins"]) == plugins
    assert manifest["plugin_tiering_cache"] in core
    selection = ci_helpers.cache_targets(pins, manifest, [])
    assert selection["targets"] == [
        "ensembl",
        "merged",
        "refseq",
        "plugin_alphamissense",
        "plugin_clinvar",
        "plugin_phenotypeorthologous",
    ]
    everything = ci_helpers.cache_targets(
        pins, manifest, ["core", "plugins", "cadd", "spliceai"]
    )
    assert set(everything["targets"]) == names


def test_cache_targets_cli(capsys):
    args = [
        "cache-targets",
        "--published",
        str(PUBLISHED_PY),
        "--manifest",
        str(CACHE_MANIFEST),
    ]
    assert ci_helpers.main([*args, "--select", "clinvar  merged"]) == 0
    out = dict(line.split("=", 1) for line in capsys.readouterr().out.splitlines())
    assert json.loads(out["targets"]) == ["merged", "plugin_clinvar"]
    assert out["aggregate"] == "false"
    assert json.loads(out["matrix"])["include"][0] == {
        "target": "merged",
        "timeout": 480,
        "memory": "22g",
    }
    assert ci_helpers.main(args) == 0
    assert "aggregate=true" in capsys.readouterr().out
    assert ci_helpers.main([*args, "--select", "dbnsfp"]) == 1
    assert "unknown selector 'dbnsfp'" in capsys.readouterr().err


# ---- /parity-caches inputs on a runner host (check.sh) --------------------

INPUTS_CONFIG = {
    "release": 116,
    "assembly": "GRCh38",
    "core": {
        "ensembl": {"raw": "vep-cache/homo_sapiens/116_GRCh38", "scratch_gb": 60},
        "merged": {"raw": "vep-cache/homo_sapiens_merged/116_GRCh38"},
    },
    "plugin_tiering_cache": "merged",
    "plugins": {"small": {}, "big": {"opt_in": "large", "scratch_gb": 100}},
}


def stage_inputs(data: Path) -> None:
    """Everything the jobs for ensembl, merged, plugin_small and plugin_big read."""
    for cache_type in ("ensembl", "merged"):
        cache = data / f"cache/116_GRCh38_{cache_type}"
        cache.mkdir(parents=True)
        marker = {
            "repo_id": f"org/vepyr_116_GRCh38_{cache_type}",
            "revision": CACHE_PINS[(116, "GRCh38", cache_type)],
        }
        (cache / ".revision").write_text(json.dumps(marker))
        touch(data / INPUTS_CONFIG["core"][cache_type]["raw"], "info.txt")
    for plugin in ("small", "big"):
        cache = data / f"plugin-cache/116_GRCh38/plugin/{plugin}"
        cache.mkdir(parents=True)
        marker = {
            "repo_id": f"org/vepyr_116_GRCh38_plugin_{plugin}",
            "revision": CACHE_PINS[(116, "GRCh38", f"plugin_{plugin}")],
        }
        (cache / ".revision").write_text(json.dumps(marker))
        sources = [{"file": "in.vcf.gz", "index": {"file": "in.vcf.gz.tbi"}}]
        (cache / "manifest.json").write_text(json.dumps({"sources": sources}))
        touch(data / f"plugin-source/{plugin}", "in.vcf.gz")
        touch(data / f"plugin-source/{plugin}", "in.vcf.gz.tbi")


def inputs(data: Path, targets: list[str], free: float | None = 500.0):
    return ci_helpers.cache_inputs(
        data, "org", CACHE_PINS, INPUTS_CONFIG, targets, free
    )


ALL_INPUT_TARGETS = ["ensembl", "merged", "plugin_small", "plugin_big"]


def test_cache_inputs_complete_host_has_no_problem(tmp_path):
    stage_inputs(tmp_path)
    assert inputs(tmp_path, ALL_INPUT_TARGETS) == ([], [])


def test_cache_inputs_empty_host_names_every_missing_input(tmp_path):
    problems, warnings = inputs(tmp_path, ["ensembl", "plugin_small"])
    text = "\n".join(problems)
    assert (
        f"{tmp_path / 'cache' / '116_GRCh38_ensembl'} (the published cache to compare with) is not staged at org/vepyr_116_GRCh38_ensembl@{'1' * 40}"
        in text
    )
    assert (
        f"raw Ensembl VEP cache missing: {tmp_path / 'vep-cache' / 'homo_sapiens' / '116_GRCh38' / 'info.txt'}"
        in text
    )
    # A plugin job reads its tiers from the merged cache.
    assert (
        f"{tmp_path / 'cache' / '116_GRCh38_merged'} (tiers of plugin caches) is not staged at"
        in text
    )
    assert (
        f"no staged source file for small under {tmp_path / 'plugin-source' / 'small'}"
        in text
    )
    assert len(problems) == 4
    # The job downloads a published plugin cache it does not find staged.
    assert warnings == [
        f"published small cache is not staged at org/vepyr_116_GRCh38_plugin_small@{'4' * 40}: "
        "the job downloads it; re-run ci/wgs-runner/stage-cache-inputs.sh to avoid that"
    ]


def test_cache_inputs_stale_marker_missing_source_and_scratch(tmp_path):
    stage_inputs(tmp_path)
    stale = {"repo_id": "org/vepyr_116_GRCh38_merged", "revision": "9" * 40}
    (tmp_path / "cache/116_GRCh38_merged/.revision").write_text(json.dumps(stale))
    (tmp_path / "plugin-source/big/in.vcf.gz.tbi").unlink()
    problems, warnings = inputs(tmp_path, ALL_INPUT_TARGETS, free=70.0)
    assert warnings == []
    # 70 GiB are enough for ensembl (60) and not for plugin_big (100).
    assert problems == [
        f"{tmp_path / 'cache' / '116_GRCh38_merged'} (the published cache to compare with) is not "
        f"staged at org/vepyr_116_GRCh38_merged@{'2' * 40}: re-run ci/wgs-runner/stage-data.sh",
        f"{tmp_path / 'cache' / '116_GRCh38_merged'} (tiers of plugin caches) is not staged at "
        f"org/vepyr_116_GRCh38_merged@{'2' * 40}: re-run ci/wgs-runner/stage-data.sh",
        f"staged plugin source missing: {tmp_path / 'plugin-source' / 'big' / 'in.vcf.gz.tbi'}; "
        "re-run ci/wgs-runner/stage-cache-inputs.sh",
        "plugin_big needs 100 GiB of scratch; 70 GiB are free",
    ]
    # Free space is not checked without a scratch directory.
    assert "scratch" not in "\n".join(inputs(tmp_path, ["plugin_big"], free=None)[0])


def test_cache_inputs_staged_plugin_cache_without_sources_is_a_problem(tmp_path):
    stage_inputs(tmp_path)
    (tmp_path / "plugin-cache/116_GRCh38/plugin/small/manifest.json").write_text("{}")
    problems, _ = inputs(tmp_path, ["plugin_small"])
    assert problems == [
        f"{tmp_path / 'plugin-cache' / '116_GRCh38' / 'plugin' / 'small' / 'manifest.json'} lists no sources; "
        "re-run ci/wgs-runner/stage-cache-inputs.sh"
    ]


def test_published_org_is_the_one_vepyr_uses(tmp_path):
    namespace: dict = {}
    exec(PUBLISHED_PY.read_text(), namespace)  # noqa: S102 - the repository's own file
    org = ci_helpers.published_org(PUBLISHED_PY)
    assert org == namespace["HF_ORG"]
    # The repository names cache_inputs builds are the ones vepyr downloads from.
    assert (
        namespace["cache_repo_id"](116, "GRCh38", "merged")
        == f"{org}/vepyr_116_GRCh38_merged"
    )
    assert (
        namespace["plugin_repo_id"](116, "GRCh38", "clinvar")
        == f"{org}/vepyr_116_GRCh38_plugin_clinvar"
    )
    other = tmp_path / "_published.py"
    other.write_text("HF_ORG = some_call()\n")
    with pytest.raises(ci_helpers.CiError, match="no HF_ORG string"):
        ci_helpers.published_org(other)


def test_cache_inputs_cli(tmp_path, capsys):
    args = [
        "cache-inputs",
        "--data",
        str(tmp_path),
        "--published",
        str(PUBLISHED_PY),
        "--manifest",
        str(CACHE_MANIFEST),
    ]
    assert ci_helpers.main([*args, "--select", "merged clinvar"]) == 1
    err = capsys.readouterr().err
    assert "cache parity input(s) missing for merged, plugin_clinvar:" in err
    assert "- raw Ensembl VEP cache missing:" in err
    assert (
        "WARNING: published clinvar cache is not staged at biodatageeks/vepyr_116_GRCh38_plugin_clinvar@"
        in err
    )
    # A complete host, for the committed pins and manifest.
    pins = ci_helpers.published_pins(PUBLISHED_PY)
    manifest = json.loads(CACHE_MANIFEST.read_text())
    cache = tmp_path / "cache/116_GRCh38_merged"
    cache.mkdir(parents=True)
    marker = {
        "repo_id": "biodatageeks/vepyr_116_GRCh38_merged",
        "revision": pins[(116, "GRCh38", "merged")],
    }
    (cache / ".revision").write_text(json.dumps(marker))
    touch(tmp_path / manifest["core"]["merged"]["raw"], "info.txt")
    assert ci_helpers.main([*args, "--select", "merged"]) == 0
    assert capsys.readouterr().out == "cache parity inputs present for: merged\n"
    assert (
        ci_helpers.main(
            [*args, "--select", "merged", "--scratch", str(tmp_path / "none")]
        )
        == 1
    )
    assert "cannot read" in capsys.readouterr().err


def test_caches_statuses_all_green(tmp_path):
    targets = ["merged", "plugin_clinvar"]
    for t in targets:
        write_result(tmp_path, f"caches/{t}", "success", "PASS: 25 of 25 shards")
    rows = as_dict(ci_helpers.caches_statuses(tmp_path, targets, "success", True))
    assert rows["parity/caches"] == ("success", "2 targets pass")
    assert rows["parity/caches/plugin_clinvar"] == ("success", "PASS: 25 of 25 shards")


def test_caches_statuses_mismatch_is_failure_and_error_wins(tmp_path):
    write_result(tmp_path, "caches/merged", "success")
    write_result(
        tmp_path, "caches/plugin_clinvar", "failure", "FAIL: 25 of 25 shards differ"
    )
    targets = ["merged", "plugin_clinvar", "refseq"]
    rows = as_dict(ci_helpers.caches_statuses(tmp_path, targets[:2], "failure", True))
    assert rows["parity/caches"][0] == "failure"
    assert "plugin_clinvar" in rows["parity/caches"][1]
    rows = as_dict(ci_helpers.caches_statuses(tmp_path, targets, "failure", True))
    assert rows["parity/caches/refseq"][0] == "error"
    assert rows["parity/caches"][0] == "error"


def test_caches_statuses_selected_targets_have_no_aggregate(tmp_path):
    write_result(tmp_path, "caches/plugin_clinvar", "success", "PASS")
    rows = ci_helpers.caches_statuses(tmp_path, ["plugin_clinvar"], "success", False)
    assert rows == [("parity/caches/plugin_clinvar", "success", "PASS")]
    # A forged success is not believed when the job itself did not succeed.
    rows = ci_helpers.caches_statuses(tmp_path, ["plugin_clinvar"], "failure", False)
    assert rows == [
        (
            "parity/caches/plugin_clinvar",
            "error",
            "job result failure contradicts verdict",
        )
    ]
    write_result(tmp_path, "caches/merged", "failure", "FAIL")
    rows = as_dict(
        ci_helpers.caches_statuses(
            tmp_path, ["merged", "plugin_clinvar"], "failure", False
        )
    )
    assert rows == {
        "parity/caches/merged": ("failure", "FAIL"),
        "parity/caches/plugin_clinvar": ("success", "PASS"),
    }


def test_caches_statuses_cancelled_job_never_success(tmp_path):
    write_result(tmp_path, "caches/merged", "success")
    rows = as_dict(ci_helpers.caches_statuses(tmp_path, ["merged"], "cancelled", True))
    assert rows["parity/caches"][0] == "error"
    assert as_dict(ci_helpers.caches_statuses(tmp_path, [], "success", False)) == {
        "parity/caches": ("error", "profile list missing (profiles job failed)")
    }


def test_caches_statuses_cli(tmp_path, capsys):
    write_result(tmp_path, "caches/merged", "success")
    args = [
        "caches-statuses",
        "--results-dir",
        str(tmp_path),
        "--targets",
        '["merged"]',
    ]
    assert ci_helpers.main([*args, "--job", "success", "--aggregate", "true"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert [line.split("\t")[0] for line in lines] == [
        "parity/caches/merged",
        "parity/caches",
    ]
    assert ci_helpers.main([*args, "--job", "success", "--aggregate", "false"]) == 0
    assert capsys.readouterr().out.count("\n") == 1
    assert (
        ci_helpers.main([*args[:-1], '["a b"]', "--job", "x", "--aggregate", "true"])
        == 1
    )
