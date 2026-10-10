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
    record = ci_helpers.result_record("porting", outcome, "s")
    assert record == {"name": "porting", "conclusion": conclusion, "summary": "s"}


def test_cli_result_writes_json(tmp_path):
    out = tmp_path / "result-porting.json"
    code = ci_helpers.main(
        [
            "result",
            "--name",
            "porting",
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
