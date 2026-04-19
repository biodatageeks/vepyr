import importlib.util
from pathlib import Path


def load_run_annotation_fast():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "e2e-testing"
        / "scripts"
        / "run_annotation_fast.py"
    )
    spec = importlib.util.spec_from_file_location("run_annotation_fast", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_extract_chrom_from_vep_force_refreshes_cached_slice(tmp_path):
    module = load_run_annotation_fast()
    vep_vcf = tmp_path / "vep.vcf"
    vep_vcf.write_text(
        "\n".join(
            [
                "##fileformat=VCFv4.2",
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
                "chr8\t10\t.\tA\tG\t50\tPASS\tCSQ=first",
                "chr1\t20\t.\tC\tT\t50\tPASS\tCSQ=other",
            ]
        )
        + "\n"
    )

    out_path = Path(module.extract_chrom_from_vep(str(vep_vcf), "chr8", str(tmp_path)))
    assert out_path.read_text().count("chr8\t") == 1

    vep_vcf.write_text(
        "\n".join(
            [
                "##fileformat=VCFv4.2",
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
                "chr8\t10\t.\tA\tG\t50\tPASS\tCSQ=first",
                "chr8\t11\t.\tT\tC\t50\tPASS\tCSQ=second",
                "chr1\t20\t.\tC\tT\t50\tPASS\tCSQ=other",
            ]
        )
        + "\n"
    )

    cached_path = Path(
        module.extract_chrom_from_vep(str(vep_vcf), "chr8", str(tmp_path), force=False)
    )
    assert cached_path.read_text().count("chr8\t") == 1

    refreshed_path = Path(
        module.extract_chrom_from_vep(str(vep_vcf), "chr8", str(tmp_path), force=True)
    )
    assert refreshed_path.read_text().count("chr8\t") == 2
