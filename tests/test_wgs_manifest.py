import hashlib
import json
import sys
from pathlib import Path

import pytest
from vepyr import _published

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "e2e-testing" / "scripts"))
import wgs_manifest  # noqa: E402


def make_layout(root: Path) -> None:
    (root / "output/116").mkdir(parents=True)
    for name in ("vep", "merged", "refseq"):
        p = root / f"output/116/HG002_annotated_wgs_everything_hgvs_{name}.vcf.gz"
        p.write_bytes(f"golden-{name}".encode())
        (p.parent / (p.name + ".tbi")).write_bytes(b"tbi")
    (root / "input").mkdir()
    (root / "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz").write_bytes(b"vcf")
    (root / "input/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi").write_bytes(b"vtbi")
    for flavour in ("ensembl", "merged", "refseq"):
        d = root / f"cache/116_GRCh38_{flavour}/variation"
        d.mkdir(parents=True)
        (d / "chr1.parquet").write_bytes(b"x" * 10)


@pytest.fixture
def chr22_manifest(tmp_path):
    m = tmp_path / "chr22.json"
    m.write_text(
        json.dumps(
            {
                "caches": {
                    f: {
                        "repo_id": f"biodatageeks/vepyr_116_GRCh38_{f}",
                        "revision": f"rev-{f}",
                        "files": {},
                    }
                    for f in ("ensembl", "merged", "refseq")
                }
            }
        ),
        encoding="utf-8",
    )
    return m


PROFILES = {"ensembl": "vep", "merged": "merged", "refseq": "refseq"}


def test_build_manifest_records_goldens_and_caches(tmp_path, chr22_manifest):
    make_layout(tmp_path / "data")
    m = wgs_manifest.build_manifest(tmp_path / "data", chr22_manifest, PROFILES)
    assert set(m["profiles"]) == {"ensembl", "merged", "refseq"}
    g = m["profiles"]["merged"]
    assert g["golden"] == "output/116/HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    assert g["sha256"] == hashlib.sha256(b"golden-merged").hexdigest()
    assert m["caches"]["refseq"] == {
        "repo_id": "biodatageeks/vepyr_116_GRCh38_refseq",
        "revision": "rev-refseq",
    }


def test_check_passes_on_fresh_staging(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    assert wgs_manifest.check_staged(data, m, staged) == []


def test_check_detects_missing_truncated_and_revision_change(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    (data / "output/116/HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz").unlink()
    (data / "cache/116_GRCh38_merged/variation/chr1.parquet").write_bytes(b"short")
    m2 = json.loads(json.dumps(m))
    m2["caches"]["ensembl"]["revision"] = "rev-new"
    problems = wgs_manifest.check_staged(data, m2, staged)
    text = "\n".join(problems)
    assert "refseq.vcf.gz" in text
    assert "chr1.parquet" in text
    assert "manifest changed since staging" in text or "ensembl" in text


def test_check_detects_wrong_golden_digest(tmp_path, chr22_manifest):
    data = tmp_path / "data"
    make_layout(data)
    m = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, m)
    m["profiles"]["merged"]["sha256"] = "0" * 64
    assert any("merged" in p for p in wgs_manifest.check_staged(data, m, staged))


def test_cli_check_exit_codes(tmp_path, chr22_manifest, capsys):
    data = tmp_path / "data"
    make_layout(data)
    mpath = tmp_path / "wgs.json"
    assert (
        wgs_manifest.main(
            [
                "prepare",
                "--data",
                str(data),
                "--chr22-manifest",
                str(chr22_manifest),
                "--out",
                str(mpath),
            ]
        )
        == 0
    )
    assert (
        wgs_manifest.main(
            [
                "record-staged",
                "--data",
                str(data),
                "--manifest",
                str(mpath),
                "--out",
                str(data / "STAGED.json"),
            ]
        )
        == 0
    )
    assert (
        wgs_manifest.main(["check", "--data", str(data), "--manifest", str(mpath)]) == 0
    )
    (data / "STAGED.json").unlink()
    assert (
        wgs_manifest.main(["check", "--data", str(data), "--manifest", str(mpath)]) == 3
    )
    assert "stage-data.sh" in capsys.readouterr().err


def test_prepare_preserves_clinvar_profiles_in_existing_manifest(
    tmp_path, chr22_manifest
):
    data = tmp_path / "data"
    make_layout(data)
    out = tmp_path / "wgs.json"
    clinvar_profiles = {
        "clinvar_merged": {
            "cache": {"revision": _published.PINS[(116, "GRCh38", "merged")]}
        }
    }
    out.write_text(json.dumps({"clinvar_profiles": clinvar_profiles}), encoding="utf-8")

    assert (
        wgs_manifest.main(
            [
                "prepare",
                "--data",
                str(data),
                "--chr22-manifest",
                str(chr22_manifest),
                "--out",
                str(out),
            ]
        )
        == 0
    )

    prepared = json.loads(out.read_text(encoding="utf-8"))
    assert prepared["clinvar_profiles"] == clinvar_profiles


def test_clinvar_profiles_do_not_invalidate_existing_wgs_staging(
    tmp_path, chr22_manifest
):
    data = tmp_path / "data"
    make_layout(data)
    manifest = wgs_manifest.build_manifest(data, chr22_manifest, PROFILES)
    staged = wgs_manifest.record_staged(data, manifest)
    with_clinvar = {**manifest, "clinvar_profiles": {"clinvar_merged": {}}}

    assert wgs_manifest.check_staged(data, with_clinvar, staged) == []
