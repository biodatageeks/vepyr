import importlib.util
import types
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


def load_run_annotation_fast_all():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "e2e-testing"
        / "scripts"
        / "run_annotation_fast_all.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_annotation_fast_all", script_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_refseq_cache_profile_uses_data_vepyr_paths():
    module = load_run_annotation_fast()
    profile = module._CACHE_PROFILES["refseq"]

    assert profile["cache_dir"].endswith("data_vepyr/115_GRCh38_refseq")
    assert profile["vep_vcf"].endswith(
        "data_vepyr/HG002_annotated_wgs_everything_hgvs_refseq.vcf"
    )


def test_parse_args_accepts_forks_and_workers(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_annotation_fast.py",
            "chr1",
            "--backend",
            "indexed_parquet",
            "--forks",
            "2",
            "--workers",
            "4",
        ],
    )

    args = module.parse_args()

    assert args.forks == 2
    assert args.workers == 4


def test_parse_args_defaults_to_indexed_parquet(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr("sys.argv", ["run_annotation_fast.py", "chr1"])

    args = module.parse_args()

    assert args.backend == "indexed_parquet"


def test_parse_args_accepts_legacy_fjall(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        ["run_annotation_fast.py", "chr1", "--backend", "legacy_fjall"],
    )

    args = module.parse_args()

    assert args.backend == "legacy_fjall"


def test_main_preserves_requested_forks_and_workers_for_single_chrom(
    monkeypatch, tmp_path
):
    module = load_run_annotation_fast()
    seen = {}

    def fake_annotate(*_args, **kwargs):
        seen["forks"] = kwargs["forks"]
        seen["workers"] = kwargs["workers"]
        Path(kwargs["output_vcf"]).write_text(
            "\n".join(
                [
                    "##fileformat=VCFv4.2",
                    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
                    "chr1\t1\t.\tA\tC\t.\tPASS\t.",
                ]
            )
            + "\n"
        )

    monkeypatch.setattr(module.vepyr, "annotate", fake_annotate)
    monkeypatch.setattr(
        module,
        "extract_chrom_from_vcf",
        lambda *_args, **_kwargs: str(tmp_path / "input_chr1.vcf.gz"),
    )
    monkeypatch.setattr(
        module.subprocess, "check_output", lambda *_args, **_kwargs: b"1"
    )
    monkeypatch.setattr(module, "count_data_lines", lambda _path: 1)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_annotation_fast.py",
            "chr1",
            "--backend",
            "indexed_parquet",
            "--forks",
            "4",
            "--workers",
            "1",
            "--vcf",
            str(tmp_path / "input.vcf.gz"),
            "--cache-dir",
            str(tmp_path / "cache"),
            "--fasta",
            str(tmp_path / "ref.fa"),
            "--no-normalize",
            "--skip-compare",
            "--force",
        ],
    )

    module.main()

    assert seen == {"forks": 4, "workers": 1}


def test_parse_args_accepts_skip_comparison_alias(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_annotation_fast.py",
            "chr1",
            "--skip-comparison",
        ],
    )

    args = module.parse_args()

    assert args.skip_compare is True


def test_fast_all_parse_args_accepts_skip_comparison(monkeypatch):
    module = load_run_annotation_fast_all()
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_annotation_fast_all.py",
            "--skip-comparison",
        ],
    )

    args = module.parse_args()

    assert args.skip_comparison is True


def test_fast_all_run_chromosome_forwards_skip_compare(monkeypatch):
    module = load_run_annotation_fast_all()
    seen = {}

    def fake_run(cmd, cwd=None):
        seen["cmd"] = cmd
        seen["cwd"] = cwd
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    assert module.run_chromosome(22, skip_comparison=True) is True
    assert "--skip-compare" in seen["cmd"]
    assert "--workers" in seen["cmd"]
    assert "--chrom-parallelism" not in seen["cmd"]


def test_fast_all_parse_args_defaults_to_indexed_parquet(monkeypatch):
    module = load_run_annotation_fast_all()
    monkeypatch.setattr("sys.argv", ["run_annotation_fast_all.py"])

    args = module.parse_args()

    assert args.backend == "indexed_parquet"


def test_fast_all_parse_args_accepts_legacy_fjall(monkeypatch):
    module = load_run_annotation_fast_all()
    monkeypatch.setattr(
        "sys.argv",
        ["run_annotation_fast_all.py", "--backend", "legacy_fjall"],
    )

    args = module.parse_args()

    assert args.backend == "legacy_fjall"


def test_parse_args_allows_parallel_indexed_parquet(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_annotation_fast.py",
            "chr1",
            "--backend",
            "indexed_parquet",
            "--forks",
            "2",
        ],
    )

    args = module.parse_args()

    assert args.forks == 2
    assert args.backend == "indexed_parquet"


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


def write_csq_vcf(path, csq):
    path.write_text(
        "\n".join(
            [
                "##fileformat=VCFv4.2",
                '##INFO=<ID=CSQ,Number=.,Type=String,Description="Format: Allele|Consequence|Feature|Gene">',
                "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO",
                f"chr1\t10\t.\tA\tG\t50\tPASS\tCSQ={csq};DP=1",
            ]
        )
        + "\n"
    )


def test_compare_vcfs_can_ignore_vep_hash_order_pick_csq_order(tmp_path):
    module = load_run_annotation_fast()
    vepyr_vcf = tmp_path / "vepyr.vcf"
    vep_vcf = tmp_path / "vep.vcf"
    write_csq_vcf(
        vepyr_vcf,
        "G|intron_variant|ENST0001|GENE1,G|intron_variant|ENST0002|GENE2",
    )
    write_csq_vcf(
        vep_vcf,
        "G|intron_variant|ENST0002|GENE2,G|intron_variant|ENST0001|GENE1",
    )

    strict = module.compare_vcfs(str(vepyr_vcf), str(vep_vcf), "chr1")
    assert strict["csq_order_mismatch"] == 1
    assert strict["csq_order_ignored"] == 0
    assert strict["field_mismatch_counts"] == {}

    hash_order_pick = module.compare_vcfs(
        str(vepyr_vcf),
        str(vep_vcf),
        "chr1",
        ignore_csq_order=True,
    )
    assert hash_order_pick["csq_order_mismatch"] == 0
    assert hash_order_pick["csq_order_ignored"] == 1
    assert hash_order_pick["field_mismatch_counts"] == {}
    assert "per_gene" in hash_order_pick["csq_order_ignore_reason"]
    assert "pick_allele_gene" in hash_order_pick["csq_order_ignore_reason"]
