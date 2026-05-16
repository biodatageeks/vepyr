import importlib.util
import sys
from types import SimpleNamespace
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


def test_run_annotation_fast_defaults_to_fjall_backend(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(sys, "argv", ["run_annotation_fast.py", "chr22"])

    args = module.parse_args()

    assert args.backend == "fjall"


def test_run_annotation_fast_accepts_parquet_backend(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        sys, "argv", ["run_annotation_fast.py", "chr22", "--backend", "parquet"]
    )

    args = module.parse_args()

    assert args.backend == "parquet"


def test_run_annotation_fast_accepts_redb_backend(monkeypatch):
    module = load_run_annotation_fast()
    monkeypatch.setattr(
        sys, "argv", ["run_annotation_fast.py", "chr22", "--backend", "redb"]
    )

    args = module.parse_args()

    assert args.backend == "redb"


def test_run_annotation_fast_all_forwards_backend(monkeypatch):
    module = load_run_annotation_fast_all()
    calls = []

    def fake_run(cmd, cwd):
        calls.append((cmd, cwd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    assert module.run_chromosome(3, cache="merged", backend="parquet", force=True)

    cmd, cwd = calls[0]
    assert cwd == module.SCRIPT_DIR
    assert "--backend" in cmd
    assert cmd[cmd.index("--backend") + 1] == "parquet"


def test_run_annotation_fast_all_forwards_redb_backend(monkeypatch):
    module = load_run_annotation_fast_all()
    calls = []

    def fake_run(cmd, cwd):
        calls.append((cmd, cwd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    assert module.run_chromosome(3, cache="merged", backend="redb", force=True)

    cmd, cwd = calls[0]
    assert cwd == module.SCRIPT_DIR
    assert "--backend" in cmd
    assert cmd[cmd.index("--backend") + 1] == "redb"


def test_run_annotation_fast_all_defaults_to_sequential(monkeypatch):
    module = load_run_annotation_fast_all()
    monkeypatch.setattr(sys, "argv", ["run_annotation_fast_all.py"])

    args = module.parse_args()

    assert args.parallel == 1


def test_run_annotation_fast_all_accepts_parallel(monkeypatch):
    module = load_run_annotation_fast_all()
    monkeypatch.setattr(sys, "argv", ["run_annotation_fast_all.py", "--parallel", "4"])

    args = module.parse_args()

    assert args.parallel == 4


def test_run_annotation_fast_all_runs_chromosomes_with_requested_parallelism(
    monkeypatch,
):
    module = load_run_annotation_fast_all()
    workers = []
    submitted = []
    calls = []

    class FakeFuture:
        def __init__(self, result):
            self._result = result

        def result(self):
            return self._result

    class FakeExecutor:
        def __init__(self, max_workers):
            workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            submitted.append((fn, args, kwargs))
            return FakeFuture(fn(*args, **kwargs))

    def fake_run_chromosome(chrom_num, cache, backend, force):
        calls.append((chrom_num, cache, backend, force))
        return True

    monkeypatch.setattr(module, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(module, "as_completed", lambda futures: list(futures))
    monkeypatch.setattr(module, "run_chromosome", fake_run_chromosome)

    failed = module.run_chromosomes(
        [1, 2, 3, 4], cache="merged", backend="parquet", force=True, parallel=3
    )

    assert workers == [3]
    assert len(submitted) == 4
    assert calls == [
        (1, "merged", "parquet", True),
        (2, "merged", "parquet", True),
        (3, "merged", "parquet", True),
        (4, "merged", "parquet", True),
    ]
    assert failed == []


def test_run_annotation_fast_all_report_uses_selected_backend():
    module = load_run_annotation_fast_all()
    reports = [
        {
            "chrom": "chr1",
            "input_variants": 10,
            "annotation": {"time_s": 2.0, "backend": "parquet"},
            "comparison": {},
        }
    ]
    agg = {
        "all_fields": {"Consequence"},
        "field_mm": {},
        "field_examples": {},
        "total_compared": 0,
        "total_csq_match": 0,
        "total_csq_mismatch": 0,
        "total_only_vepyr": 0,
        "total_only_vep": 0,
    }

    md = module.generate_report(
        reports, agg, csq_classes={}, old_mm=None, build_info={}, backend="parquet"
    )

    assert "# Fast Annotation Report: chr1-22 (parquet)" in md
    assert "**Backend:** parquet only" in md


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
