# Merged e2e Comparison Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `run_annotation_fast.py` and `run_annotation_fast_all.py` with a single `run_comparison.py` backed by a `comparison/` package that handles one or all contigs, accepts plain and block-gzipped VCFs on both sides, and models the Ensembl release explicitly.

**Architecture:** A thin entry point delegates to `comparison.cli`. Six modules split by responsibility: `vcfio` (compression and indexing), `profiles` (profile x release matrix), `compare` (CSQ comparison, pure), `annotate` (the only `vepyr` importer), `report` (aggregation and Markdown, pure), `cli` (argparse and orchestration). Only `annotate` needs a built native extension, so everything else unit-tests in milliseconds.

**Tech Stack:** Python 3.10+, pytest 8, argparse, subprocess wrapping `bcftools` / `bgzip` / `tabix`, ruff for lint and format.

**Spec:** `docs/superpowers/specs/2026-07-28-merge-comparison-runners-design.md`

## Global Constraints

- Releases are **strings** everywhere (`"115"`, `"116"`), never ints — `115.2` is a directory name and future releases may not be numeric.
- `RELEASE_DIRS = {"115": "115.2", "116": "116"}` — the on-disk directory name is not derivable from the release number.
- Profile `suffix` values are stored **without** a leading underscore (`"merged"`, not `"_merged"`); filename templates add the separators.
- `--release` is **required**, no default. `--profile` defaults to `"merged"`.
- `--force` re-annotates; reuse is the default.
- Every intermediate lives under `results/{release}/`. Nothing under `results/{release}/` is ever read by a run of a different release.
- Reports stay flat in `reports/` with the release in the filename, so the 408 existing report JSONs remain loadable.
- Default input paths resolve `$DATA/input/{name}` first, then fall back to `$DATA/{name}` with a warning.
- Parquet caches resolve `$DATA/cache/{release}_GRCh38_{flavour}` first, then fall back to `$DATA/{release}_GRCh38_{flavour}` with a warning.
- `$DATA` = `$DATA_VEPYR_DIR` or `~/workspace/data_vepyr`.
- Only `comparison/annotate.py` may import `vepyr`. Every other module must import cleanly without the native extension built.
- Run anything that imports `vepyr` via `uv run`; plain `python` will not find the extension.
- Pre-commit runs `ruff check` and `ruff format` on commit; run them before committing to avoid a failed hook.

---

### Task 1: Package scaffold, test bootstrap, and compression primitives

**Files:**
- Create: `e2e-testing/scripts/comparison/__init__.py`
- Create: `e2e-testing/scripts/comparison/vcfio.py`
- Create: `tests/conftest.py`
- Test: `tests/test_comparison_vcfio.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `vcfio.open_text(path) -> TextIO`, `vcfio.is_bgzf(path) -> bool`, `vcfio.count_data_lines(path) -> int`, `vcfio.ensure_bgzf(path, out_dir) -> str`, `vcfio.ensure_tabix_index(vcf_gz) -> None`.

- [ ] **Step 1: Write the failing test**

Create `tests/conftest.py`:

```python
"""Put e2e-testing/scripts on sys.path so `comparison` imports as a package."""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "e2e-testing" / "scripts"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
```

Create `tests/test_comparison_vcfio.py`:

```python
import subprocess

import pytest

from comparison import vcfio

VCF_BODY = """##fileformat=VCFv4.2
##contig=<ID=chr1>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|missense_variant|ENST01
chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|ENST01
"""


@pytest.fixture
def plain_vcf(tmp_path):
    p = tmp_path / "sample.vcf"
    p.write_text(VCF_BODY)
    return p


@pytest.fixture
def bgzf_vcf(tmp_path, plain_vcf):
    out = tmp_path / "sample_bgzf.vcf.gz"
    with open(out, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain_vcf)], stdout=fh, check=True)
    return out


def test_is_bgzf_distinguishes_plain_from_block_gzip(plain_vcf, bgzf_vcf):
    assert vcfio.is_bgzf(str(bgzf_vcf)) is True
    assert vcfio.is_bgzf(str(plain_vcf)) is False


def test_open_text_reads_plain_and_bgzf_identically(plain_vcf, bgzf_vcf):
    with vcfio.open_text(str(plain_vcf)) as fh:
        plain = fh.read()
    with vcfio.open_text(str(bgzf_vcf)) as fh:
        compressed = fh.read()
    assert plain == compressed == VCF_BODY


def test_count_data_lines_ignores_headers(plain_vcf, bgzf_vcf):
    assert vcfio.count_data_lines(str(plain_vcf)) == 2
    assert vcfio.count_data_lines(str(bgzf_vcf)) == 2


def test_ensure_bgzf_compresses_a_plain_vcf(plain_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    result = vcfio.ensure_bgzf(str(plain_vcf), str(out_dir))
    assert result.endswith(".vcf.gz")
    assert vcfio.is_bgzf(result)
    assert vcfio.count_data_lines(result) == 2


def test_ensure_bgzf_returns_an_already_compressed_file_unchanged(bgzf_vcf, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    assert vcfio.ensure_bgzf(str(bgzf_vcf), str(out_dir)) == str(bgzf_vcf)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_vcfio.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison'`

- [ ] **Step 3: Write the minimal implementation**

Create `e2e-testing/scripts/comparison/__init__.py`:

```python
"""vepyr vs Ensembl VEP parity comparison harness."""
```

Create `e2e-testing/scripts/comparison/vcfio.py`:

```python
"""Compression- and index-aware VCF helpers.

This module knows about gzip framing, bgzf, and tabix. It knows nothing about
CSQ fields or annotation profiles, and must import without the vepyr native
extension present.
"""

import gzip
import os
import subprocess

GZIP_SUFFIXES = (".gz", ".bgz", ".bgzf")


def open_text(path):
    """Open a VCF for text reading, transparently handling .gz (bgzf or plain gzip)."""
    if path.endswith(GZIP_SUFFIXES):
        return gzip.open(path, "rt")
    return open(path)


def is_bgzf(path):
    """Return True if `path` is BGZF (block-gzip): gzip magic plus a 'BC' subfield."""
    with open(path, "rb") as f:
        head = f.read(18)
    # gzip magic 1f 8b, deflate (08), FLG.FEXTRA set (bit 2), then an extra
    # field carrying the "BC" subfield id that marks bgzf blocks.
    return (
        len(head) >= 18
        and head[0:3] == b"\x1f\x8b\x08"
        and bool(head[3] & 0x04)
        and head[12:14] == b"BC"
    )


def count_data_lines(path):
    """Count non-header lines in a VCF (plain or .gz)."""
    n = 0
    with open_text(path) as f:
        for line in f:
            if not line.startswith("#"):
                n += 1
    return n


def ensure_tabix_index(vcf_gz):
    """Create a tabix index for `vcf_gz` if one is missing."""
    tbi = vcf_gz + ".tbi"
    if os.path.exists(tbi):
        return
    print(f"  Indexing (tabix) {os.path.basename(vcf_gz)} ...")
    subprocess.run(["tabix", "-p", "vcf", vcf_gz], check=True)


def ensure_bgzf(path, out_dir):
    """Return a bgzf-compressed, tabix-indexed copy of `path`.

    Already-compressed inputs are returned untouched. A plain VCF is copied into
    `out_dir` and block-gzipped there, so a read-only source directory is never
    written to. This is what lets --no-normalize accept an uncompressed VCF.
    """
    if path.endswith(GZIP_SUFFIXES):
        ensure_tabix_index(path)
        return path

    os.makedirs(out_dir, exist_ok=True)
    target = os.path.join(out_dir, os.path.basename(path))
    target_gz = target + ".gz"
    if os.path.exists(target_gz):
        ensure_tabix_index(target_gz)
        return target_gz

    print(f"  Input is plain text, block-gzipping {os.path.basename(path)} ...")
    with open(target_gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", path], stdout=fh, check=True)
    ensure_tabix_index(target_gz)
    return target_gz
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_vcfio.py -v`
Expected: PASS, 5 tests

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/conftest.py tests/test_comparison_vcfio.py
uv run ruff format e2e-testing/scripts/comparison tests/conftest.py tests/test_comparison_vcfio.py
git add e2e-testing/scripts/comparison tests/conftest.py tests/test_comparison_vcfio.py
git commit -m "feat(e2e): add comparison package with compression primitives"
```

---

### Task 2: Contig detection, normalization, and slicing

**Files:**
- Modify: `e2e-testing/scripts/comparison/vcfio.py`
- Test: `tests/test_comparison_vcfio.py`

**Interfaces:**
- Consumes: `vcfio.open_text`, `vcfio.is_bgzf`, `vcfio.ensure_tabix_index`, `vcfio.ensure_bgzf` from Task 1.
- Produces: `vcfio.detect_contigs(vcf) -> list[str]`, `vcfio.normalize_vcf(vcf, out_dir) -> str`, `vcfio.slice_contig(vcf_gz, chrom, out_dir) -> str`, `vcfio.slice_vep(vep_vcf, chrom, out_dir, suffix, force=False) -> str`.

Contig detection must read the **index, not the header**. On the real reference files `tabix -l` returns 22 contigs in coordinate order while the header lists 195 (the whole GRCh38 primary assembly plus scaffolds and alts), so header parsing would launch 173 empty contigs.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_comparison_vcfio.py`:

```python
import json


MULTI_CONTIG_BODY = """##fileformat=VCFv4.2
##contig=<ID=chr1>
##contig=<ID=chr2>
##contig=<ID=chr3>
##contig=<ID=chrUn_scaffold99>
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
chr1\t100\t.\tA\tT\t50\tPASS\t.
chr2\t100\t.\tG\tC\t50\tPASS\t.
chr1\t300\t.\tC\tG\t50\tPASS\t.
"""


@pytest.fixture
def indexed_multi_contig(tmp_path):
    """Header lists 4 contigs; only chr1 and chr2 carry records."""
    plain = tmp_path / "multi.vcf"
    # tabix requires coordinate-sorted input
    rows = sorted(
        [ln for ln in MULTI_CONTIG_BODY.splitlines() if not ln.startswith("#")],
        key=lambda ln: (ln.split("\t")[0], int(ln.split("\t")[1])),
    )
    header = [ln for ln in MULTI_CONTIG_BODY.splitlines() if ln.startswith("#")]
    plain.write_text("\n".join(header + rows) + "\n")
    gz = tmp_path / "multi.vcf.gz"
    with open(gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain)], stdout=fh, check=True)
    subprocess.run(["tabix", "-p", "vcf", str(gz)], check=True)
    return gz


def test_detect_contigs_uses_the_index_not_the_header(indexed_multi_contig):
    """The header lists 4 contigs but only 2 have records; detection must find 2."""
    assert vcfio.detect_contigs(str(indexed_multi_contig)) == ["chr1", "chr2"]


def test_detect_contigs_returns_empty_for_an_unindexed_file(plain_vcf):
    assert vcfio.detect_contigs(str(plain_vcf)) == []


def test_slice_contig_extracts_only_the_requested_contig(indexed_multi_contig, tmp_path):
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    sliced = vcfio.slice_contig(str(indexed_multi_contig), "chr1", str(out_dir))
    assert vcfio.is_bgzf(sliced)
    assert vcfio.count_data_lines(sliced) == 2
    with vcfio.open_text(sliced) as fh:
        chroms = {ln.split("\t")[0] for ln in fh if not ln.startswith("#")}
    assert chroms == {"chr1"}


def test_slice_vep_reads_a_bgzf_reference(indexed_multi_contig, tmp_path):
    """Regression: extract_chrom_from_vep used bare open() and raised UnicodeDecodeError."""
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    out = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(out_dir), "merged")
    assert vcfio.count_data_lines(out) == 2


def test_slice_vep_tabix_and_linear_paths_agree(indexed_multi_contig, tmp_path):
    """The indexed fast path and the plain linear scan must produce identical records."""
    gz_dir = tmp_path / "gz"
    gz_dir.mkdir()
    via_tabix = vcfio.slice_vep(str(indexed_multi_contig), "chr1", str(gz_dir), "a")

    plain = tmp_path / "plain.vcf"
    with vcfio.open_text(str(indexed_multi_contig)) as fh:
        plain.write_text(fh.read())
    plain_dir = tmp_path / "plain_out"
    plain_dir.mkdir()
    via_scan = vcfio.slice_vep(str(plain), "chr1", str(plain_dir), "a")

    def records(path):
        with vcfio.open_text(path) as fh:
            return [ln for ln in fh if not ln.startswith("#")]

    assert records(via_tabix) == records(via_scan)


def test_slice_vep_matches_contig_without_chr_prefix(tmp_path):
    """VEP output may use bare contig names; chr22 must still match a '22' record."""
    src = tmp_path / "bare.vcf"
    src.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        "22\t100\t.\tA\tT\t50\tPASS\t.\n"
    )
    out_dir = tmp_path / "work"
    out_dir.mkdir()
    out = vcfio.slice_vep(str(src), "chr22", str(out_dir), "merged")
    assert vcfio.count_data_lines(out) == 1


def test_normalize_vcf_records_its_source(indexed_multi_contig, tmp_path):
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    norm = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    sidecar = json.loads((tmp_path / "shared" / "normalized.source.json").read_text())
    assert sidecar["path"] == str(indexed_multi_contig)
    assert sidecar["size"] == indexed_multi_contig.stat().st_size
    assert vcfio.is_bgzf(norm)


def test_normalize_vcf_reuses_output_for_the_same_source(indexed_multi_contig, tmp_path):
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    first = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    marker = os.path.join(str(out_dir), "marker")
    open(marker, "w").close()
    second = vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))
    assert first == second
    assert os.path.exists(marker), "reuse must not wipe the shared directory"


def test_normalize_vcf_reruns_when_the_source_changes(indexed_multi_contig, tmp_path):
    """A different --vcf at the same release must not silently reuse a stale decomposition."""
    out_dir = tmp_path / "shared"
    out_dir.mkdir()
    vcfio.normalize_vcf(str(indexed_multi_contig), str(out_dir))

    other = tmp_path / "other.vcf.gz"
    other.write_bytes(indexed_multi_contig.read_bytes())
    subprocess.run(["tabix", "-f", "-p", "vcf", str(other)], check=True)
    vcfio.normalize_vcf(str(other), str(out_dir))

    sidecar = json.loads((out_dir / "normalized.source.json").read_text())
    assert sidecar["path"] == str(other)
```

Add `import os` to the test module imports if not already present.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_vcfio.py -v`
Expected: FAIL — `AttributeError: module 'comparison.vcfio' has no attribute 'detect_contigs'`

- [ ] **Step 3: Write the implementation**

Append to `e2e-testing/scripts/comparison/vcfio.py`:

```python
import json
import sys


def detect_contigs(vcf):
    """Return contigs that actually carry records, in index (coordinate) order.

    Reads the tabix index, never the ##contig headers: on the real HG002 VEP
    outputs the index lists 22 contigs while the header lists 195 (the whole
    GRCh38 primary assembly plus scaffolds and alts). Returns [] when the file
    has no usable index, so callers can fall back.
    """
    if not vcf.endswith(GZIP_SUFFIXES) or not os.path.exists(vcf + ".tbi"):
        return []
    result = subprocess.run(["tabix", "-l", vcf], capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def contig_aliases(chrom):
    """Return {chrom, with-prefix, without-prefix} so 'chr22' also matches '22'."""
    bare = chrom[3:] if chrom.startswith("chr") else chrom
    return {chrom, bare, f"chr{bare}"}


def normalize_vcf(vcf, out_dir):
    """Normalize with `bcftools norm -m -both`, then bgzip and index.

    Output is shared by every contig of one release. A `normalized.source.json`
    sidecar records the input path, size, and mtime; a mismatch forces a re-run
    so changing --vcf cannot silently reuse a stale decomposition.
    """
    os.makedirs(out_dir, exist_ok=True)
    norm_vcf = os.path.join(out_dir, "normalized.vcf")
    norm_vcf_gz = norm_vcf + ".gz"
    sidecar_path = os.path.join(out_dir, "normalized.source.json")

    stat = os.stat(vcf)
    source = {"path": os.path.abspath(vcf), "size": stat.st_size, "mtime": stat.st_mtime}

    if os.path.exists(norm_vcf_gz) and os.path.exists(sidecar_path):
        with open(sidecar_path) as f:
            previous = json.load(f)
        if previous == source:
            print(f"  Using existing {norm_vcf_gz}")
            ensure_tabix_index(norm_vcf_gz)
            return norm_vcf_gz
        print(
            f"  Source changed ({previous.get('path')} -> {source['path']}), re-normalizing"
        )

    print(f"  Normalizing {os.path.basename(vcf)} (bcftools norm -m -both) ...")
    result = subprocess.run(
        ["bcftools", "norm", "-m", "-both", "-o", norm_vcf, vcf],
        capture_output=True,
        text=True,
    )
    if result.stderr.strip():
        print(result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(f"bcftools norm failed: {result.stderr}")

    subprocess.run(["bgzip", "-f", norm_vcf], check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", norm_vcf_gz], check=True)
    with open(sidecar_path, "w") as f:
        json.dump(source, f, indent=2)
    print(f"  Created {norm_vcf_gz}")
    return norm_vcf_gz


def slice_contig(vcf_gz, chrom, out_dir):
    """Extract one contig from an indexed VCF into a bgzf + tabix-indexed slice."""
    os.makedirs(out_dir, exist_ok=True)
    out_vcf = os.path.join(out_dir, f"input_{chrom}.vcf")
    out_gz = out_vcf + ".gz"
    if os.path.exists(out_gz) and os.path.exists(out_gz + ".tbi"):
        print(f"  Using existing {out_gz}")
        return out_gz

    ensure_tabix_index(vcf_gz)
    header = subprocess.run(
        ["tabix", "-H", vcf_gz], capture_output=True, check=True
    ).stdout

    body = b""
    for candidate in contig_aliases(chrom):
        result = subprocess.run(["tabix", vcf_gz, candidate], capture_output=True)
        if result.returncode == 0 and result.stdout:
            body = result.stdout
            break
    if not body:
        raise SystemExit(
            f"Error: tabix found no records for {chrom} in {vcf_gz}. "
            f"Available contigs: {', '.join(detect_contigs(vcf_gz)) or '(none)'}"
        )

    with open(out_vcf, "wb") as f:
        f.write(header)
        f.write(body)
    subprocess.run(["bgzip", "-f", out_vcf], check=True)
    subprocess.run(["tabix", "-f", "-p", "vcf", out_gz], check=True)
    print(f"  Created {out_gz}")
    return out_gz


def slice_vep(vep_vcf, chrom, out_dir, suffix, force=False):
    """Extract one contig from a VEP reference VCF, plain or block-gzipped.

    Uses `tabix` when the reference is indexed, which turns a full scan of a
    multi-gigabyte reference into a seek. Falls back to a streaming linear scan
    through open_text() otherwise -- the previous implementation used a bare
    open() here and raised UnicodeDecodeError on any block-gzipped reference.
    """
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"vep_{chrom}_{suffix}.vcf")
    if os.path.exists(out_path) and not force:
        print(f"  Using existing {out_path}")
        return out_path

    indexed = vep_vcf.endswith(GZIP_SUFFIXES) and os.path.exists(vep_vcf + ".tbi")
    if indexed:
        print(f"  Extracting {chrom} from VEP reference via tabix ...")
        header = subprocess.run(
            ["tabix", "-H", vep_vcf], capture_output=True, check=True
        ).stdout
        body = b""
        for candidate in contig_aliases(chrom):
            result = subprocess.run(["tabix", vep_vcf, candidate], capture_output=True)
            if result.returncode == 0 and result.stdout:
                body = result.stdout
                break
        with open(out_path, "wb") as f:
            f.write(header)
            f.write(body)
        n = body.count(b"\n")
    else:
        if vep_vcf.endswith(GZIP_SUFFIXES):
            print(
                f"  Note: {os.path.basename(vep_vcf)} is compressed but unindexed; "
                "streaming instead of seeking",
                file=sys.stderr,
            )
        print(f"  Extracting {chrom} from VEP reference by scan ...")
        targets = contig_aliases(chrom)
        n = 0
        with open_text(vep_vcf) as fin, open(out_path, "w") as fout:
            for line in fin:
                if line.startswith("#"):
                    fout.write(line)
                elif line.split("\t", 1)[0] in targets:
                    fout.write(line)
                    n += 1

    print(f"  Extracted {n:,} VEP records for {chrom}")
    return out_path
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_vcfio.py -v`
Expected: PASS, 14 tests

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/test_comparison_vcfio.py
uv run ruff format e2e-testing/scripts/comparison tests/test_comparison_vcfio.py
git add e2e-testing/scripts/comparison/vcfio.py tests/test_comparison_vcfio.py
git commit -m "feat(e2e): index-based contig detection, bgzf-safe VEP slicing, source-keyed normalization"
```

---

### Task 3: Profile x release matrix

**Files:**
- Create: `e2e-testing/scripts/comparison/profiles.py`
- Test: `tests/test_comparison_profiles.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `profiles.PROFILES` (dict), `profiles.RELEASES` (tuple), `profiles.DEFAULT_PROFILE` (str), `profiles.Resolved` (dataclass with fields `profile`, `release`, `cache_dir`, `vep_vcf`, `annotate_kwargs`, `suffix`, `ignore_csq_order`), `profiles.resolve(profile, release, cache_dir=None, vep_vcf=None) -> Resolved`, `profiles.availability_table() -> str`, `profiles.default_input(name) -> str`, `profiles.data_dir() -> str`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_comparison_profiles.py`:

```python
import pytest

from comparison import profiles


def test_suffixes_have_no_leading_underscore():
    """Filename templates add separators, so stored suffixes must not."""
    for name, profile in profiles.PROFILES.items():
        assert not profile.suffix.startswith("_"), name


def test_default_profile_is_merged():
    assert profiles.DEFAULT_PROFILE == "merged"


def test_releases_are_strings():
    assert all(isinstance(r, str) for r in profiles.RELEASES)


def test_release_dirs_map_115_to_the_dotted_directory():
    assert profiles.RELEASE_DIRS["115"] == "115.2"
    assert profiles.RELEASE_DIRS["116"] == "116"


def test_resolve_derives_cache_and_reference_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "115_GRCh38_merged").mkdir(parents=True)
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    ref = ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    ref.write_text("")

    resolved = profiles.resolve("merged", "115")
    assert resolved.cache_dir == str(tmp_path / "cache" / "115_GRCh38_merged")
    assert resolved.vep_vcf == str(ref)
    assert resolved.suffix == "merged"


def test_cache_dir_falls_back_to_the_data_root(tmp_path, monkeypatch, capsys):
    """Legacy layout: caches sit at $DATA/ rather than $DATA/cache/."""
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    legacy = tmp_path / "115_GRCh38_merged"
    legacy.mkdir()
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz").write_text("")

    resolved = profiles.resolve("merged", "115")
    assert resolved.cache_dir == str(legacy)
    assert "cache/" in capsys.readouterr().err


def test_cache_dir_prefers_the_cache_subdirectory(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "115_GRCh38_merged").mkdir()
    preferred = tmp_path / "cache" / "115_GRCh38_merged"
    preferred.mkdir(parents=True)
    assert profiles.cache_dir_for("merged", "115") == str(preferred)


def test_resolve_prefers_bgzf_over_plain_reference(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "115_GRCh38_merged").mkdir(parents=True)
    ref_dir = tmp_path / "output" / "115.2"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf").write_text("")
    gz = ref_dir / "HG002_annotated_wgs_everything_hgvs_merged.vcf.gz"
    gz.write_text("")
    assert profiles.resolve("merged", "115").vep_vcf == str(gz)


def test_resolve_reports_what_is_available_when_the_cache_is_missing(  # noqa: E501
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    ref_dir = tmp_path / "output" / "116"
    ref_dir.mkdir(parents=True)
    (ref_dir / "HG002_annotated_wgs_everything_hgvs_refseq.vcf.gz").write_text("")

    with pytest.raises(profiles.ProfileUnavailable) as excinfo:
        profiles.resolve("refseq", "116")
    message = str(excinfo.value)
    assert "116_GRCh38_refseq" in message
    assert "Available" in message


def test_resolve_accepts_explicit_overrides(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    cache = tmp_path / "custom_cache"
    cache.mkdir()
    ref = tmp_path / "custom.vcf.gz"
    ref.write_text("")
    resolved = profiles.resolve("merged", "116", cache_dir=str(cache), vep_vcf=str(ref))
    assert resolved.cache_dir == str(cache)
    assert resolved.vep_vcf == str(ref)


def test_hash_order_profiles_ignore_csq_order():
    assert profiles.PROFILES["merged_per_gene"].ignore_csq_order is True
    assert profiles.PROFILES["merged_pick_allele_gene"].ignore_csq_order is True
    assert profiles.PROFILES["merged"].ignore_csq_order is False


def test_default_input_prefers_the_input_subdirectory(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "input").mkdir()
    preferred = tmp_path / "input" / "ref.fa"
    preferred.write_text("")
    (tmp_path / "ref.fa").write_text("")
    assert profiles.default_input("ref.fa") == str(preferred)


def test_default_input_falls_back_to_the_data_root(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    legacy = tmp_path / "ref.fa"
    legacy.write_text("")
    assert profiles.default_input("ref.fa") == str(legacy)
    assert "input/" in capsys.readouterr().err


def test_default_input_returns_the_preferred_path_when_neither_exists(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    assert profiles.default_input("ref.fa") == str(tmp_path / "input" / "ref.fa")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_profiles.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison.profiles'`

- [ ] **Step 3: Write the implementation**

Create `e2e-testing/scripts/comparison/profiles.py`:

```python
"""Annotation profile x Ensembl release matrix and path derivation.

A profile selects a cache flavour, a VEP reference, and the pick-mode flags
passed to vepyr.annotate(). The release selects which build of both to use.
Both paths are derived rather than hardcoded, so adding a release means adding
one RELEASE_DIRS entry.
"""

import os
import sys
from dataclasses import dataclass, field

VEP_PICK_ORDER = "biotype,rank,mane_select,tsl,canonical,appris,ccds,length"
BACKEND = "parquet"

# Releases are strings: "115.2" is a directory name, and future releases may
# not be purely numeric.
RELEASES = ("115", "116")
RELEASE_DIRS = {"115": "115.2", "116": "116"}

DEFAULT_PROFILE = "merged"

DEFAULT_VCF_NAME = "HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
DEFAULT_FASTA_NAME = "Homo_sapiens.GRCh38.dna.primary_assembly.fa"


class ProfileUnavailable(RuntimeError):
    """Raised when a profile x release combination has no cache or no reference."""


@dataclass(frozen=True)
class Profile:
    """One comparison scenario.

    `flavour` picks the Parquet cache ({release}_GRCh38_{flavour}). `vep_basename`
    is the reference filename inside output/{release_dir}/, without extension.
    `suffix` is stored without a leading underscore; filename templates add the
    separators. `ignore_csq_order` marks profiles where Ensembl VEP emits already
    selected CSQ entries in Perl hash order, which carries no meaning.
    """

    flavour: str
    vep_basename: str
    suffix: str
    annotate_kwargs: dict = field(default_factory=dict)
    ignore_csq_order: bool = False


_PICK = {"pick_order": VEP_PICK_ORDER}

PROFILES = {
    "ensembl": Profile(
        flavour="ensembl",
        vep_basename="HG002_annotated_wgs_everything_hgvs_vep",
        suffix="ensembl",
    ),
    "merged": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged",
        suffix="merged",
    ),
    "refseq": Profile(
        flavour="refseq",
        vep_basename="HG002_annotated_wgs_everything_hgvs_refseq",
        suffix="refseq",
    ),
    "merged_flag_pick": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_flag_pick",
        suffix="merged_flag_pick",
        annotate_kwargs={"flag_pick": True, **_PICK},
    ),
    "merged_flag_pick_allele": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele",
        suffix="merged_flag_pick_allele",
        annotate_kwargs={"flag_pick_allele": True, **_PICK},
    ),
    # This local VEP artifact is misnamed: chr16 validation shows it is the
    # flag_pick_allele_gene reference, with unfiltered CSQs and PICK.
    "merged_flag_pick_allele_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick",
        suffix="merged_flag_pick_allele_gene",
        annotate_kwargs={"flag_pick_allele_gene": True, **_PICK},
    ),
    "merged_pick_filter": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_filter",
        suffix="merged_pick_filter",
        annotate_kwargs={"pick": True, **_PICK},
    ),
    "merged_pick_allele": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_allele",
        suffix="merged_pick_allele",
        annotate_kwargs={"pick_allele": True, **_PICK},
    ),
    "merged_per_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_per_gene",
        suffix="merged_per_gene",
        annotate_kwargs={"per_gene": True, **_PICK},
        ignore_csq_order=True,
    ),
    "merged_pick_allele_gene": Profile(
        flavour="merged",
        vep_basename="HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene",
        suffix="merged_pick_allele_gene",
        annotate_kwargs={"pick_allele_gene": True, **_PICK},
        ignore_csq_order=True,
    ),
}


@dataclass(frozen=True)
class Resolved:
    profile: str
    release: str
    cache_dir: str
    vep_vcf: str
    annotate_kwargs: dict
    suffix: str
    ignore_csq_order: bool


def data_dir():
    return os.path.expanduser(
        os.path.expandvars(os.environ.get("DATA_VEPYR_DIR", "$HOME/workspace/data_vepyr"))
    )


def _resolve_with_legacy_fallback(subdir, name, exists):
    """Prefer $DATA/{subdir}/{name}, fall back to $DATA/{name} with a warning.

    Returns the preferred path when neither exists, so the caller's own existence
    check produces the error message rather than this helper inventing one.
    """
    preferred = os.path.join(data_dir(), subdir, name)
    if exists(preferred):
        return preferred
    legacy = os.path.join(data_dir(), name)
    if exists(legacy):
        print(
            f"  Note: using legacy location {legacy}; move it under {subdir}/ "
            "(see docs/superpowers/specs/2026-07-28-merge-comparison-runners-design.md)",
            file=sys.stderr,
        )
        return legacy
    return preferred


def default_input(name):
    """Resolve a default input file, preferring $DATA/input/ over the legacy root."""
    return _resolve_with_legacy_fallback("input", name, os.path.exists)


def cache_dir_for(profile_name, release):
    """Resolve a Parquet cache, preferring $DATA/cache/ over the legacy root."""
    name = f"{release}_GRCh38_{PROFILES[profile_name].flavour}"
    return _resolve_with_legacy_fallback("cache", name, os.path.isdir)


def vep_vcf_for(profile_name, release):
    """Return the reference path, preferring .vcf.gz, or None if neither exists."""
    base = os.path.join(
        data_dir(),
        "output",
        RELEASE_DIRS[release],
        PROFILES[profile_name].vep_basename,
    )
    for candidate in (base + ".vcf.gz", base + ".vcf"):
        if os.path.exists(candidate):
            return candidate
    return None


def availability_table():
    """Render which profile x release combinations have both a cache and a reference."""
    lines = [f"{'profile':<32} " + " ".join(f"{r:>12}" for r in RELEASES)]
    for name in sorted(PROFILES):
        cells = []
        for release in RELEASES:
            has_cache = os.path.isdir(cache_dir_for(name, release))
            has_ref = vep_vcf_for(name, release) is not None
            if has_cache and has_ref:
                cells.append("ok")
            elif has_cache:
                cells.append("no reference")
            elif has_ref:
                cells.append("no cache")
            else:
                cells.append("-")
        lines.append(f"{name:<32} " + " ".join(f"{c:>12}" for c in cells))
    return "\n".join(lines)


def resolve(profile_name, release, cache_dir=None, vep_vcf=None):
    """Resolve a profile and release to concrete paths, or raise ProfileUnavailable.

    Runs before any other work so a bad combination fails in milliseconds rather
    than after a normalization pass. Explicit cache_dir / vep_vcf override the
    derived paths and skip their existence checks.
    """
    if profile_name not in PROFILES:
        raise ProfileUnavailable(
            f"Unknown profile {profile_name!r}. Known: {', '.join(sorted(PROFILES))}"
        )
    if release not in RELEASE_DIRS:
        raise ProfileUnavailable(
            f"Unknown release {release!r}. Known: {', '.join(RELEASES)}"
        )

    profile = PROFILES[profile_name]
    resolved_cache = cache_dir or cache_dir_for(profile_name, release)
    resolved_ref = vep_vcf or vep_vcf_for(profile_name, release)

    problems = []
    if not os.path.isdir(resolved_cache):
        problems.append(f"no Parquet cache at {resolved_cache}")
    if resolved_ref is None:
        problems.append(
            f"no VEP reference {profile.vep_basename}.vcf[.gz] under "
            f"output/{RELEASE_DIRS[release]}/"
        )
    if problems:
        raise ProfileUnavailable(
            f"Profile {profile_name!r} at release {release}: "
            + "; ".join(problems)
            + "\n\nAvailable combinations:\n"
            + availability_table()
        )

    return Resolved(
        profile=profile_name,
        release=release,
        cache_dir=resolved_cache,
        vep_vcf=resolved_ref,
        annotate_kwargs=dict(profile.annotate_kwargs),
        suffix=profile.suffix,
        ignore_csq_order=profile.ignore_csq_order,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_profiles.py -v`
Expected: PASS, 14 tests

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/test_comparison_profiles.py
uv run ruff format e2e-testing/scripts/comparison tests/test_comparison_profiles.py
git add e2e-testing/scripts/comparison/profiles.py tests/test_comparison_profiles.py
git commit -m "feat(e2e): profile x release matrix with fail-fast availability check"
```

---

### Task 4: CSQ comparison engine

**Files:**
- Create: `e2e-testing/scripts/comparison/compare.py`
- Test: `tests/test_comparison_compare.py`

**Interfaces:**
- Consumes: `vcfio.open_text`, `vcfio.count_data_lines` from Task 1.
- Produces: `compare.compare_vcfs(vepyr_vcf, vep_vcf, label, ignore_csq_order=False) -> dict`, `compare.VEP_HASH_ORDER_PICK_IGNORE_REASON` (str).

This is a port. Copy `compare_vcfs` verbatim from `run_annotation_fast.py:384-672` and `VEP_HASH_ORDER_PICK_IGNORE_REASON` from `:373-381`, then apply exactly these changes:

1. Replace the module-level `import re` usage by adding `import re` at the top of the new file.
2. Replace bare `open_text(...)` and `count_data_lines(...)` calls with `vcfio.open_text(...)` and `vcfio.count_data_lines(...)`, importing `from . import vcfio`.
3. Replace `BACKEND` in the banner `print` with a `backend="parquet"` keyword argument defaulting to `"parquet"`.
4. Do **not** copy `VEP_HASH_ORDER_PICK_CACHES` — that set is replaced by the `ignore_csq_order` field on `Profile` from Task 3.
5. Add no other behaviour changes. The returned dict keys must stay byte-identical, because `report.py` and the 408 existing report JSONs depend on them.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_comparison_compare.py`:

```python
import itertools
import subprocess

import pytest

from comparison import compare

HEADER = (
    '##fileformat=VCFv4.2\n'
    '##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence annotations. '
    'Format: Allele|Consequence|IMPACT|Feature">\n'
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
)

MATCHING = HEADER + (
    "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|missense_variant|MODERATE|ENST01\n"
    "chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|LOW|ENST01\n"
)

DIFFERING = HEADER + (
    "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|stop_gained|HIGH|ENST01\n"
    "chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|synonymous_variant|LOW|ENST01\n"
)


def _write(tmp_path, name, body, compressed):
    plain = tmp_path / name
    plain.write_text(body)
    if not compressed:
        return str(plain)
    gz = tmp_path / (name + ".gz")
    with open(gz, "wb") as fh:
        subprocess.run(["bgzip", "-c", str(plain)], stdout=fh, check=True)
    return str(gz)


@pytest.mark.parametrize(
    "vepyr_gz,vep_gz", list(itertools.product([False, True], repeat=2))
)
def test_compare_is_identical_across_all_compression_combinations(
    tmp_path, vepyr_gz, vep_gz
):
    """Regression for the bare open() that raised UnicodeDecodeError on bgzf refs."""
    a = _write(tmp_path / "a" if False else tmp_path, "vepyr.vcf", MATCHING, vepyr_gz)
    b = _write(tmp_path, "vep.vcf", MATCHING, vep_gz)
    result = compare.compare_vcfs(a, b, "combo")
    assert result["variants_compared"] == 2
    assert result["variants_only_in_vepyr"] == 0
    assert result["variants_only_in_vep"] == 0
    assert result["field_mismatch_counts"] == {}
    assert result["field_match_rates"]["Consequence"] == 100.0


def test_compare_counts_field_mismatches(tmp_path):
    a = _write(tmp_path, "vepyr.vcf", MATCHING, False)
    b = _write(tmp_path, "vep.vcf", DIFFERING, False)
    result = compare.compare_vcfs(a, b, "diff")
    assert result["field_mismatch_counts"]["Consequence"] == 1
    assert result["field_mismatch_counts"]["IMPACT"] == 1
    assert result["field_mismatch_examples"]["Consequence"][0]["vepyr"] == (
        "missense_variant"
    )


def test_compare_can_ignore_vep_hash_order_csq_order(tmp_path):
    two_entries = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\t"
        "CSQ=T|missense_variant|MODERATE|ENST01,T|intron_variant|MODIFIER|ENST02\n"
    )
    reordered = HEADER + (
        "chr1\t100\t.\tA\tT\t50\tPASS\t"
        "CSQ=T|intron_variant|MODIFIER|ENST02,T|missense_variant|MODERATE|ENST01\n"
    )
    a = _write(tmp_path, "vepyr.vcf", two_entries, False)
    b = _write(tmp_path, "vep.vcf", reordered, False)

    strict = compare.compare_vcfs(a, b, "strict")
    assert strict["csq_order_mismatch"] == 1
    assert strict["csq_order_ignored"] == 0

    lenient = compare.compare_vcfs(a, b, "lenient", ignore_csq_order=True)
    assert lenient["csq_order_mismatch"] == 0
    assert lenient["csq_order_ignored"] == 1
    assert lenient["csq_order_ignore_reason"] == (
        compare.VEP_HASH_ORDER_PICK_IGNORE_REASON
    )


def test_compare_reports_variants_present_in_only_one_side(tmp_path):
    extra = MATCHING + "chr1\t300\t.\tT\tA\t50\tPASS\tCSQ=A|intron_variant|MODIFIER|ENST01\n"
    a = _write(tmp_path, "vepyr.vcf", extra, False)
    b = _write(tmp_path, "vep.vcf", MATCHING, False)
    result = compare.compare_vcfs(a, b, "extra")
    assert result["variants_only_in_vepyr"] == 1
    assert result["variants_only_in_vep"] == 0
```

Simplify the first parametrized test's `a = ...` line to:

```python
    a = _write(tmp_path, "vepyr.vcf", MATCHING, vepyr_gz)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_compare.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison.compare'`

- [ ] **Step 3: Port the implementation**

Create `e2e-testing/scripts/comparison/compare.py` starting with:

```python
"""Field-by-field CSQ comparison between a vepyr output and a VEP reference.

Pure with respect to the rest of the harness: it takes two paths and returns a
dict. It does not import vepyr, does not parse argv, and does not format
Markdown, so it unit-tests without a built native extension.
"""

import re

from . import vcfio

VEP_HASH_ORDER_PICK_IGNORE_REASON = (
    "CSQ entry order is ignored for per_gene and pick_allele_gene because "
    "Ensembl VEP selects the representative consequences, then emits those "
    "winners by iterating Perl hashes (`keys %by_gene`; for pick_allele_gene "
    "also `keys %by_allele`). The comma order of those already-selected CSQ "
    "entries has no biological or interpretation meaning; it is not a severity, "
    "transcript-priority, genomic, MANE, or canonical ranking. The meaningful "
    "checks are the selected CSQ entries, entry counts, and field values."
)


def compare_vcfs(vepyr_vcf, vep_vcf, label, ignore_csq_order=False, backend="parquet"):
    """Field-by-field CSQ comparison between vepyr and VEP output."""
```

Then paste the body of `compare_vcfs` from `run_annotation_fast.py:391-672` verbatim, applying the five changes listed above this step. The two local helper reads become:

```python
    n_vepyr = vcfio.count_data_lines(vepyr_vcf)
    n_vep = vcfio.count_data_lines(vep_vcf)
```

and both `with open_text(path) as f:` occurrences (inside `get_csq_fields` and `extract_keyed_csq`) become `with vcfio.open_text(path) as f:`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_compare.py -v`
Expected: PASS, 7 tests (4 parametrized combinations plus 3)

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/test_comparison_compare.py
uv run ruff format e2e-testing/scripts/comparison tests/test_comparison_compare.py
git add e2e-testing/scripts/comparison/compare.py tests/test_comparison_compare.py
git commit -m "feat(e2e): extract CSQ comparison engine into a pure module"
```

---

### Task 5: Aggregation, classification, and Markdown reporting

**Files:**
- Create: `e2e-testing/scripts/comparison/report.py`
- Test: `tests/test_comparison_report.py`

**Interfaces:**
- Consumes: nothing at runtime; consumes the dict shape produced by `compare.compare_vcfs` in Task 4.
- Produces: `report.ISSUES` (dict), `report.REPO` (str), `report.report_json_path(report_dir, chrom, suffix, release) -> str`, `report.load_reports(report_dir, chroms, suffix, release) -> list[dict]`, `report.contig_span(chroms) -> str`, `report.aggregate_mismatches(reports) -> dict`, `report.classify_consequence_mismatches(examples) -> dict`, `report.load_old_benchmark(report_dir, backend="parquet") -> dict | None`, `report.get_build_info() -> dict`, `report.generate_markdown(reports, agg, csq_classes, old_mm, build_info, release, profile, backend="parquet") -> str`.

Port `ISSUES`, `REPO`, `aggregate_mismatches`, `classify_consequence_mismatches`, `load_old_benchmark`, `get_build_info`, `issue_link`, `pr_link`, and `generate_report` from `run_annotation_fast_all.py` (lines 44-114, 233-394, 400-690) with these changes:

1. `generate_report` is renamed `generate_markdown` and takes `release` and `profile` as required keyword arguments.
2. Its title line becomes `f"# Fast Annotation Report: {span} ({backend}, release {release}, profile {profile})"`, where `span` comes from `contig_span([r["chrom"] for r in reports])`.
3. `load_old_benchmark` takes `report_dir` as its first argument instead of using the module-level constant.
4. `get_build_info` moves its `import re` to the module top level.
5. Every `SCRIPT_DIR` use in `get_build_info` is replaced by the module-level `REPO_ROOT` defined in the code block below. The old walk was `cwd=os.path.join(SCRIPT_DIR, "..", "..")` from `e2e-testing/scripts/`; from inside the package that is one level deeper, and `REPO_ROOT` already resolves to the repo root. The Cargo.toml path becomes `os.path.join(REPO_ROOT, "Cargo.toml")`.
6. Guard the two division sites that assume a non-zero total: `total_time` and `agg["total_compared"]`. An all-reused run has `time_s: None` for every chromosome, which makes `total_time` zero and today raises `ZeroDivisionError`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_comparison_report.py`:

```python
import json

from comparison import report


def make_chrom_report(chrom, *, time_s=1.0, consequence_mismatches=0):
    return {
        "chrom": chrom,
        "profile": "merged",
        "release": "115",
        "input_variants": 100,
        "annotation": {
            "backend": "parquet",
            "compression": "plain",
            "time_s": time_s,
            "output_variants": 100,
        },
        "comparison": {
            "variants_compared": 100,
            "variants_only_in_vepyr": 0,
            "variants_only_in_vep": 0,
            "csq_entry_count_match": 100,
            "csq_entry_count_mismatch": 0,
            "csq_order_mismatch": 0,
            "csq_order_ignored": 0,
            "field_match_rates": {"Consequence": 99.0, "IMPACT": 100.0},
            "field_mismatch_counts": (
                {"Consequence": consequence_mismatches} if consequence_mismatches else {}
            ),
            "field_mismatch_examples": (
                {
                    "Consequence": [
                        {
                            "variant": f"{chrom}\t100\tA\tT",
                            "vepyr": "stop_gained",
                            "vep": "frameshift_variant",
                        }
                    ]
                }
                if consequence_mismatches
                else {}
            ),
            "field_order_mismatch_counts": {},
            "field_order_mismatch_examples": {},
        },
    }


def test_report_json_path_includes_the_release():
    path = report.report_json_path("/reports", "chr1", "merged", "115")
    assert path.endswith("fast_chr1_merged_115_report.json")


def test_report_paths_for_two_releases_do_not_collide():
    a = report.report_json_path("/reports", "chr1", "merged", "115")
    b = report.report_json_path("/reports", "chr1", "merged", "116")
    assert a != b


def test_contig_span_summarises_a_contiguous_range():
    assert report.contig_span(["chr1", "chr2", "chr22"]) == "chr1_chr22"
    assert report.contig_span(["chr7"]) == "chr7"


def test_load_reports_prefers_the_release_qualified_name(tmp_path):
    modern = tmp_path / "fast_chr1_merged_115_report.json"
    modern.write_text(json.dumps(make_chrom_report("chr1")))
    legacy = tmp_path / "fast_chr1_merged_report.json"
    legacy.write_text(json.dumps({"chrom": "legacy"}))
    loaded = report.load_reports(str(tmp_path), ["chr1"], "merged", "115")
    assert len(loaded) == 1
    assert loaded[0]["chrom"] == "chr1"


def test_load_reports_falls_back_to_the_legacy_name(tmp_path, capsys):
    legacy = tmp_path / "fast_chr1_merged_report.json"
    legacy.write_text(json.dumps(make_chrom_report("chr1")))
    loaded = report.load_reports(str(tmp_path), ["chr1"], "merged", "115")
    assert len(loaded) == 1
    assert "legacy" in capsys.readouterr().out.lower()


def test_aggregate_sums_across_chromosomes():
    reports = [
        make_chrom_report("chr1", consequence_mismatches=2),
        make_chrom_report("chr2", consequence_mismatches=3),
    ]
    agg = report.aggregate_mismatches(reports)
    assert agg["total_compared"] == 200
    assert agg["field_mm"]["Consequence"] == 5
    assert len(agg["field_examples"]["Consequence"]) == 2
    assert {e["source_chrom"] for e in agg["field_examples"]["Consequence"]} == {
        "chr1",
        "chr2",
    }


def test_classify_routes_stop_gained_missing():
    examples = [{"vepyr": "frameshift_variant", "vep": "stop_gained&frameshift_variant"}]
    classes = report.classify_consequence_mismatches(examples)
    assert "stop_gained_missing" in classes


def test_generate_markdown_names_the_release_and_profile():
    reports = [make_chrom_report("chr1")]
    agg = report.aggregate_mismatches(reports)
    md = report.generate_markdown(
        reports,
        agg,
        report.classify_consequence_mismatches([]),
        None,
        {"branch": "main", "vepyr_rev": "abc1234", "bio_functions_rev": "def5678"},
        release="115",
        profile="merged",
    )
    assert "release 115" in md
    assert "profile merged" in md
    assert "## Per-Chromosome Performance" in md


def test_generate_markdown_survives_an_all_reused_run():
    """Every time_s is None when nothing was re-annotated; must not divide by zero."""
    reports = [make_chrom_report("chr1", time_s=None)]
    agg = report.aggregate_mismatches(reports)
    md = report.generate_markdown(
        reports,
        agg,
        {},
        None,
        {},
        release="115",
        profile="merged",
    )
    assert "Per-Chromosome Performance" in md
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_report.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison.report'`

- [ ] **Step 3: Write the implementation**

Create `e2e-testing/scripts/comparison/report.py`. Start with the new functions, then paste the ported ones:

```python
"""Aggregation, root-cause classification, and Markdown report generation.

Takes dicts and returns a string. Touches the filesystem only to load existing
per-contig report JSONs and repo metadata.
"""

import json
import os
import re
import subprocess
from collections import defaultdict
from datetime import datetime

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(PACKAGE_DIR)))


def report_json_path(report_dir, chrom, suffix, release):
    """Release-qualified per-contig report path."""
    return os.path.join(report_dir, f"fast_{chrom}_{suffix}_{release}_report.json")


def legacy_report_json_path(report_dir, chrom, suffix):
    """Pre-release-axis path, kept readable so historical reports still load."""
    return os.path.join(report_dir, f"fast_{chrom}_{suffix}_report.json")


def contig_span(chroms):
    """Summarise a contig list for a filename: single name, or first_last."""
    if not chroms:
        return "none"
    if len(chroms) == 1:
        return chroms[0]
    return f"{chroms[0]}_{chroms[-1]}"


def load_reports(report_dir, chroms, suffix, release):
    """Load per-contig reports, preferring release-qualified names.

    Falls back to the legacy unqualified name and says so, because silently
    reading a report that predates the release axis is how a 115 result ends up
    in a 116 summary.
    """
    loaded = []
    for chrom in chroms:
        modern = report_json_path(report_dir, chrom, suffix, release)
        legacy = legacy_report_json_path(report_dir, chrom, suffix)
        if os.path.exists(modern):
            path = modern
        elif os.path.exists(legacy):
            path = legacy
            print(
                f"  Using legacy report {os.path.basename(legacy)} for {chrom} "
                "(predates the release axis; release attribution unverified)"
            )
        else:
            print(f"  WARNING: no report for {chrom}, skipping")
            continue
        with open(path) as f:
            loaded.append(json.load(f))
    return loaded
```

Then append `REPO`, `ISSUES`, `aggregate_mismatches`, `classify_consequence_mismatches`, `load_old_benchmark`, `get_build_info`, `issue_link`, `pr_link`, and `generate_markdown`, ported from `run_annotation_fast_all.py` as described above this step.

The two division guards in `generate_markdown`:

```python
    total_time = sum(r["annotation"]["time_s"] or 0 for r in reports)
    ...
    if total_time > 0:
        lines.append(
            f"**Total annotation time:** {total_time:.0f}s ({total_time / 60:.1f} min)"
        )
        lines.append(f"**Aggregate rate:** {total_in / total_time:,.0f} variants/s")
    else:
        lines.append("**Total annotation time:** n/a (all output reused)")
```

and in the performance table footer:

```python
    rate_cell = f"{total_in / total_time:,.0f}" if total_time else "n/a"
    lines.append(
        f"| **TOTAL** | **{total_in:,}** | **{total_time:.1f}** | **{rate_cell}** |"
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_report.py -v`
Expected: PASS, 9 tests

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/test_comparison_report.py
uv run ruff format e2e-testing/scripts/comparison tests/test_comparison_report.py
git add e2e-testing/scripts/comparison/report.py tests/test_comparison_report.py
git commit -m "feat(e2e): release-aware reporting with legacy report fallback"
```

---

### Task 6: Annotation wrapper

**Files:**
- Create: `e2e-testing/scripts/comparison/annotate.py`
- Test: `tests/test_comparison_annotate.py`

**Interfaces:**
- Consumes: `vcfio.is_bgzf`, `vcfio.count_data_lines` from Task 1.
- Produces: `annotate.annotate_contig(chrom_vcf_gz, cache_dir, fasta, output_vcf, workers, annotate_kwargs, force=False, bgzf=False) -> tuple[float | None, int]` returning `(elapsed_seconds_or_None, output_variant_count)`.

This is the only module allowed to import `vepyr`, and it imports it lazily inside the function so the rest of the suite runs without a built extension.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_comparison_annotate.py`:

```python
import sys
import types

import pytest

from comparison import annotate


@pytest.fixture
def fake_vepyr(monkeypatch, tmp_path):
    """Install a stub vepyr module that writes a two-record VCF."""
    calls = []

    def fake_annotate(vcf, cache_dir, **kwargs):
        calls.append({"vcf": vcf, "cache_dir": cache_dir, **kwargs})
        out = kwargs["output_vcf"]
        with open(out, "w") as f:
            f.write("##fileformat=VCFv4.2\n")
            f.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
            f.write("chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|x|y\n")
            f.write("chr1\t200\t.\tG\tC\t50\tPASS\tCSQ=C|x|y\n")

    module = types.ModuleType("vepyr")
    module.annotate = fake_annotate
    monkeypatch.setitem(sys.modules, "vepyr", module)
    return calls


def test_annotate_contig_forwards_profile_kwargs(fake_vepyr, tmp_path):
    out = tmp_path / "out.vcf"
    elapsed, n = annotate.annotate_contig(
        "input.vcf.gz",
        "/cache",
        "/ref.fa",
        str(out),
        workers=4,
        annotate_kwargs={"per_gene": True, "pick_order": "rank"},
    )
    assert n == 2
    assert elapsed is not None
    call = fake_vepyr[0]
    assert call["workers"] == 4
    assert call["per_gene"] is True
    assert call["pick_order"] == "rank"
    assert call["everything"] is True
    assert call["cache_format"] == "parquet"


def test_annotate_contig_reuses_existing_output(fake_vepyr, tmp_path):
    out = tmp_path / "out.vcf"
    out.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|x|y\n" * 200
    )
    elapsed, n = annotate.annotate_contig(
        "input.vcf.gz", "/cache", "/ref.fa", str(out), workers=1, annotate_kwargs={}
    )
    assert elapsed is None
    assert n == 200
    assert fake_vepyr == [], "reuse must not call vepyr.annotate"


def test_annotate_contig_force_reannotates(fake_vepyr, tmp_path):
    out = tmp_path / "out.vcf"
    out.write_text(
        "##fileformat=VCFv4.2\n"
        "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
        + "chr1\t100\t.\tA\tT\t50\tPASS\tCSQ=T|x|y\n" * 200
    )
    annotate.annotate_contig(
        "input.vcf.gz",
        "/cache",
        "/ref.fa",
        str(out),
        workers=1,
        annotate_kwargs={},
        force=True,
    )
    assert len(fake_vepyr) == 1


def test_annotate_contig_rejects_non_bgzf_output_when_bgzf_requested(
    fake_vepyr, tmp_path
):
    """The stub writes plain text, so --bgzf validation must fail loudly."""
    out = tmp_path / "out.vcf.gz"
    with pytest.raises(SystemExit, match="not valid BGZF"):
        annotate.annotate_contig(
            "input.vcf.gz",
            "/cache",
            "/ref.fa",
            str(out),
            workers=1,
            annotate_kwargs={},
            bgzf=True,
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_annotate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison.annotate'`

- [ ] **Step 3: Write the implementation**

Create `e2e-testing/scripts/comparison/annotate.py`:

```python
"""The only module that imports vepyr.

The import is deferred into the function body so the rest of the harness -- and
its tests -- run without the native extension built.
"""

import os
import sys
import time

from . import vcfio

BACKEND = "parquet"
REUSE_MIN_BYTES = 1000


def annotate_contig(
    chrom_vcf_gz,
    cache_dir,
    fasta,
    output_vcf,
    workers,
    annotate_kwargs,
    force=False,
    bgzf=False,
):
    """Annotate one contig slice, reusing existing output unless forced.

    Returns (elapsed_seconds, output_variant_count). elapsed is None when the
    existing output was reused, which the report renders as a blank timing.
    """
    if (
        not force
        and os.path.exists(output_vcf)
        and os.path.getsize(output_vcf) > REUSE_MIN_BYTES
    ):
        n_out = vcfio.count_data_lines(output_vcf)
        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        print(f"  Reusing {output_vcf} ({n_out:,} variants, {size_mb:.0f} MB)")
        print("  Use --force to re-run")
        _validate_bgzf(output_vcf, bgzf)
        return None, n_out

    import vepyr

    t0 = time.time()
    vepyr.annotate(
        chrom_vcf_gz,
        cache_dir,
        everything=True,
        reference_fasta=fasta,
        cache_format=BACKEND,
        output_vcf=output_vcf,
        workers=workers,
        **annotate_kwargs,
    )
    elapsed = time.time() - t0

    n_out = vcfio.count_data_lines(output_vcf)
    size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
    rate = n_out / elapsed if elapsed > 0 else 0
    print(
        f"  Done: {n_out:,} variants in {elapsed:.1f}s "
        f"({rate:,.0f} variants/s), {size_mb:.0f} MB"
    )
    _validate_bgzf(output_vcf, bgzf)
    return elapsed, n_out


def _validate_bgzf(output_vcf, bgzf):
    if not bgzf:
        return
    if vcfio.is_bgzf(output_vcf):
        print("  bgzf check: output is valid block-gzip (BGZF)")
    else:
        sys.exit(f"Error: --bgzf output {output_vcf} is not valid BGZF")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_annotate.py -v`
Expected: PASS, 4 tests

- [ ] **Step 5: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts/comparison tests/test_comparison_annotate.py
uv run ruff format e2e-testing/scripts/comparison tests/test_comparison_annotate.py
git add e2e-testing/scripts/comparison/annotate.py tests/test_comparison_annotate.py
git commit -m "feat(e2e): isolate the vepyr import behind an annotation wrapper"
```

---

### Task 7: CLI, orchestration, and entry point

**Files:**
- Create: `e2e-testing/scripts/comparison/cli.py`
- Create: `e2e-testing/scripts/run_comparison.py`
- Test: `tests/test_comparison_cli.py`

**Interfaces:**
- Consumes: everything from Tasks 1-6 — `profiles.resolve`, `profiles.default_input`, `profiles.DEFAULT_PROFILE`, `profiles.RELEASES`, `profiles.PROFILES`, `vcfio.normalize_vcf`, `vcfio.ensure_bgzf`, `vcfio.detect_contigs`, `vcfio.slice_contig`, `vcfio.slice_vep`, `annotate.annotate_contig`, `compare.compare_vcfs`, `report.*`.
- Produces: `cli.parse_args(argv=None) -> argparse.Namespace`, `cli.resolve_contigs(args, resolved, input_vcf) -> list[str]`, `cli.run_contig(chrom, args, resolved, input_vcf, results_root, report_dir) -> dict`, `cli.main(argv=None) -> int`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_comparison_cli.py`:

```python
import pytest

from comparison import cli, profiles


def test_release_is_required():
    with pytest.raises(SystemExit):
        cli.parse_args([])


def test_defaults(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    args = cli.parse_args(["--release", "115"])
    assert args.release == "115"
    assert args.profile == profiles.DEFAULT_PROFILE == "merged"
    assert args.force is False
    assert args.bgzf is False
    assert args.workers == 1
    assert args.isolate is False
    assert args.no_normalize is False
    assert args.chroms is None


def test_release_must_be_known():
    with pytest.raises(SystemExit):
        cli.parse_args(["--release", "999"])


def test_removed_flags_are_rejected():
    for flag in (["--no-force"], ["--cache", "merged"], ["--backend", "lance"]):
        with pytest.raises(SystemExit):
            cli.parse_args(["--release", "115", *flag])


def test_chroms_normalises_bare_numbers():
    args = cli.parse_args(["--release", "115", "--chroms", "1", "22"])
    assert args.chroms == ["chr1", "chr22"]


def test_chroms_all_means_detect():
    args = cli.parse_args(["--release", "115", "--chroms", "all"])
    assert args.chroms is None


def test_workers_must_be_positive():
    with pytest.raises(SystemExit):
        cli.parse_args(["--release", "115", "--workers", "0"])


def test_resolve_contigs_intersects_reference_and_input(monkeypatch):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: ["chr1", "chr2", "chr3"] if path == "ref.gz" else ["chr2", "chr3", "chr4"],
    )
    args = cli.parse_args(["--release", "115"])
    resolved = _fake_resolved(vep_vcf="ref.gz")
    assert cli.resolve_contigs(args, resolved, "input.gz") == ["chr2", "chr3"]


def test_resolve_contigs_preserves_reference_order(monkeypatch):
    """tabix -l returns coordinate order; a naive sort would give chr1, chr10, chr2."""
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: ["chr1", "chr2", "chr10"],
    )
    args = cli.parse_args(["--release", "115"])
    assert cli.resolve_contigs(args, _fake_resolved(), "input.gz") == [
        "chr1",
        "chr2",
        "chr10",
    ]


def test_resolve_contigs_falls_back_to_input_when_reference_unindexed(monkeypatch, capsys):
    monkeypatch.setattr(
        cli.vcfio,
        "detect_contigs",
        lambda path: [] if path == "ref.vcf" else ["chr1", "chr2"],
    )
    args = cli.parse_args(["--release", "115"])
    resolved = _fake_resolved(vep_vcf="ref.vcf")
    assert cli.resolve_contigs(args, resolved, "input.gz") == ["chr1", "chr2"]
    assert "degraded" in capsys.readouterr().err.lower()


def test_resolve_contigs_rejects_an_explicit_contig_that_is_absent(monkeypatch):
    monkeypatch.setattr(cli.vcfio, "detect_contigs", lambda path: ["chr1", "chr2"])
    args = cli.parse_args(["--release", "115", "--chroms", "chr9"])
    with pytest.raises(SystemExit, match="chr9"):
        cli.resolve_contigs(args, _fake_resolved(), "input.gz")


def test_results_root_is_release_scoped(tmp_path):
    root = cli.results_root(str(tmp_path), "116")
    assert root.endswith("results/116") or root.endswith("results\\116")


def _fake_resolved(vep_vcf="ref.gz"):
    return profiles.Resolved(
        profile="merged",
        release="115",
        cache_dir="/cache",
        vep_vcf=vep_vcf,
        annotate_kwargs={},
        suffix="merged",
        ignore_csq_order=False,
    )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_comparison_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'comparison.cli'`

- [ ] **Step 3: Write the implementation**

Create `e2e-testing/scripts/comparison/cli.py`:

```python
"""Argument parsing and orchestration for the parity comparison runner."""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime

from . import annotate, compare, profiles, report, vcfio

BACKEND = "parquet"

DESCRIPTION = """Compare vepyr annotation against an Ensembl VEP reference.

Examples:
    run_comparison.py --release 115                        # all detected contigs
    run_comparison.py --release 115 --chroms 22            # one contig
    run_comparison.py --release 116 --profile merged --chroms 1 2 22
    run_comparison.py --release 115 --bgzf --workers 4 --force
"""


def _normalise_chrom(value):
    return value if value.startswith("chr") else f"chr{value}"


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="run_comparison.py",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--release",
        required=True,
        choices=profiles.RELEASES,
        help="Ensembl release; selects both the Parquet cache and the VEP reference",
    )
    p.add_argument(
        "--profile",
        choices=sorted(profiles.PROFILES),
        default=profiles.DEFAULT_PROFILE,
        help="Annotation scenario (default: %(default)s)",
    )
    p.add_argument(
        "--chroms",
        nargs="+",
        default=None,
        help="Contigs to process, e.g. '22', 'chr1 chr2', or 'all'. "
        "Default: detect from the VEP reference index",
    )
    p.add_argument(
        "--force", action="store_true", help="Re-annotate even if output exists"
    )
    p.add_argument(
        "--bgzf",
        action="store_true",
        help="Write block-gzipped (.vcf.gz) annotated output and validate it",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Within-contig parallel annotation pipelines (default: %(default)s)",
    )
    p.add_argument(
        "--isolate",
        action="store_true",
        help="Run each contig in its own subprocess so a native crash loses only that contig",
    )
    p.add_argument(
        "--skip-annotate",
        action="store_true",
        help="Skip annotation and comparison; only regenerate the summary from existing JSONs",
    )
    p.add_argument(
        "--skip-compare",
        "--skip-comparison",
        dest="skip_compare",
        action="store_true",
        help="Annotate only, no comparison against the VEP reference",
    )
    p.add_argument(
        "--no-normalize",
        action="store_true",
        help="Skip bcftools norm (normalization is on by default)",
    )
    p.add_argument("--vcf", default=None, help="Input VCF (default: $DATA/input/...)")
    p.add_argument("--fasta", default=None, help="Reference FASTA (default: $DATA/input/...)")
    p.add_argument("--vep", default=None, help="VEP reference VCF (default: from profile x release)")
    p.add_argument("--cache-dir", default=None, help="Parquet cache (default: from profile x release)")

    args = p.parse_args(argv)
    if args.workers <= 0:
        p.error("--workers must be a positive integer")
    if args.chroms is not None:
        if len(args.chroms) == 1 and args.chroms[0].lower() == "all":
            args.chroms = None
        else:
            args.chroms = [_normalise_chrom(c) for c in args.chroms]
    if args.vcf is None:
        args.vcf = profiles.default_input(profiles.DEFAULT_VCF_NAME)
    if args.fasta is None:
        args.fasta = profiles.default_input(profiles.DEFAULT_FASTA_NAME)
    return args


def results_root(e2e_dir, release):
    """Every intermediate for a release lives under one directory."""
    return os.path.join(e2e_dir, "results", release)


def resolve_contigs(args, resolved, input_vcf):
    """Contigs to process: the reference index intersected with the input index.

    Reads indexes, never ##contig headers -- the headers on the real references
    list 195 contigs while only 22 carry records.
    """
    input_contigs = vcfio.detect_contigs(input_vcf)

    if args.skip_compare:
        detected = input_contigs
    else:
        ref_contigs = vcfio.detect_contigs(resolved.vep_vcf)
        if not ref_contigs:
            print(
                f"  Note: {os.path.basename(resolved.vep_vcf)} has no tabix index; "
                "contig detection degraded to the input VCF",
                file=sys.stderr,
            )
            detected = input_contigs
        elif input_contigs:
            allowed = set(input_contigs)
            detected = [c for c in ref_contigs if c in allowed]
        else:
            detected = ref_contigs

    if args.chroms is None:
        if not detected:
            raise SystemExit(
                "Error: could not detect any contigs. Pass --chroms explicitly."
            )
        return detected

    if detected:
        missing = [c for c in args.chroms if c not in set(detected)]
        if missing:
            raise SystemExit(
                f"Error: requested contig(s) {', '.join(missing)} not present. "
                f"Available: {', '.join(detected)}"
            )
    return args.chroms


def run_contig(chrom, args, resolved, input_vcf, results_dir, report_dir):
    """Annotate and compare a single contig, returning its report dict."""
    work_dir = os.path.join(results_dir, f"fast_{chrom}")
    os.makedirs(work_dir, exist_ok=True)

    print(f"\n{'=' * 60}\n  {chrom} (profile={resolved.profile}, release={resolved.release})\n{'=' * 60}")

    chrom_vcf_gz = vcfio.slice_contig(input_vcf, chrom, work_dir)
    n_variants = vcfio.count_data_lines(chrom_vcf_gz)
    print(f"  Input: {n_variants:,} variants for {chrom}")

    ext = ".vcf.gz" if args.bgzf else ".vcf"
    output_vcf = os.path.join(
        work_dir, f"vepyr_{BACKEND}_{chrom}_{resolved.suffix}{ext}"
    )
    elapsed, n_out = annotate.annotate_contig(
        chrom_vcf_gz,
        resolved.cache_dir,
        args.fasta,
        output_vcf,
        workers=args.workers,
        annotate_kwargs=resolved.annotate_kwargs,
        force=args.force,
        bgzf=args.bgzf,
    )

    comparison = None
    if not args.skip_compare:
        vep_slice = vcfio.slice_vep(
            resolved.vep_vcf, chrom, work_dir, resolved.suffix, force=args.force
        )
        comparison = compare.compare_vcfs(
            output_vcf,
            vep_slice,
            chrom,
            ignore_csq_order=resolved.ignore_csq_order,
            backend=BACKEND,
        )

    result = {
        "chrom": chrom,
        "profile": resolved.profile,
        "release": resolved.release,
        "cache": resolved.profile,
        "input_variants": n_variants,
        "annotation": {
            "backend": BACKEND,
            "compression": "bgzf" if args.bgzf else "plain",
            "time_s": round(elapsed, 1) if elapsed else None,
            "output_variants": n_out,
        },
        "comparison": comparison,
    }

    path = report.report_json_path(report_dir, chrom, resolved.suffix, resolved.release)
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Report: {path}")
    return result


def _run_contig_isolated(chrom, args):
    """Re-invoke this script for one contig so a SIGSEGV loses only that contig."""
    entry = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "run_comparison.py")
    cmd = [
        sys.executable, entry,
        "--release", args.release,
        "--profile", args.profile,
        "--chroms", chrom,
        "--workers", str(args.workers),
        "--vcf", args.vcf,
        "--fasta", args.fasta,
    ]
    if args.force:
        cmd.append("--force")
    if args.bgzf:
        cmd.append("--bgzf")
    if args.skip_compare:
        cmd.append("--skip-compare")
    if args.no_normalize:
        cmd.append("--no-normalize")
    if args.vep:
        cmd += ["--vep", args.vep]
    if args.cache_dir:
        cmd += ["--cache-dir", args.cache_dir]
    return subprocess.run(cmd).returncode == 0


def main(argv=None):
    args = parse_args(argv)

    try:
        resolved = profiles.resolve(
            args.profile, args.release, cache_dir=args.cache_dir, vep_vcf=args.vep
        )
    except profiles.ProfileUnavailable as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    # .../e2e-testing/scripts/comparison/cli.py -> .../e2e-testing
    e2e_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    results_dir = results_root(e2e_dir, args.release)
    report_dir = os.path.join(e2e_dir, "reports")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)

    print("=" * 60)
    print(f"  profile:   {resolved.profile}")
    print(f"  release:   {resolved.release}")
    print(f"  cache_dir: {resolved.cache_dir}")
    print(f"  vep_vcf:   {resolved.vep_vcf}")
    print(f"  results:   {results_dir}")
    print("=" * 60)

    input_vcf = args.vcf
    if not args.skip_annotate:
        shared = os.path.join(results_dir, "_shared")
        if args.no_normalize:
            input_vcf = vcfio.ensure_bgzf(args.vcf, shared)
        else:
            input_vcf = vcfio.normalize_vcf(args.vcf, shared)

    chroms = resolve_contigs(args, resolved, input_vcf)
    print(f"  contigs:   {', '.join(chroms)}")

    failures = []
    if not args.skip_annotate:
        for chrom in chroms:
            try:
                if args.isolate:
                    if not _run_contig_isolated(chrom, args):
                        failures.append(chrom)
                else:
                    run_contig(chrom, args, resolved, input_vcf, results_dir, report_dir)
            except Exception as exc:  # noqa: BLE001 - one contig must not kill the sweep
                print(f"  ERROR: {chrom} failed: {exc}", file=sys.stderr)
                failures.append(chrom)

    if args.skip_compare:
        print("\nSkipping aggregate summary (--skip-compare)")
        return 1 if failures else 0

    reports = report.load_reports(report_dir, chroms, resolved.suffix, resolved.release)
    if not reports:
        print("No reports found.", file=sys.stderr)
        return 1

    agg = report.aggregate_mismatches(reports)
    csq_classes = report.classify_consequence_mismatches(
        agg["field_examples"].get("Consequence", [])
    )
    old_mm = report.load_old_benchmark(report_dir, BACKEND)
    build_info = report.get_build_info()

    md = report.generate_markdown(
        reports, agg, csq_classes, old_mm, build_info,
        release=resolved.release, profile=resolved.profile, backend=BACKEND,
    )
    span = report.contig_span([r["chrom"] for r in reports])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    summary_path = os.path.join(
        report_dir,
        f"fast_{span}_{resolved.suffix}_{resolved.release}_summary_{timestamp}.md",
    )
    with open(summary_path, "w") as f:
        f.write(md)

    n_perfect = len([f for f in agg["all_fields"] if agg["field_mm"].get(f, 0) == 0])
    print(f"\n{'=' * 60}")
    print(f"  Summary: {summary_path}")
    print(f"  Fields at 100%: {n_perfect}/{len(agg['all_fields'])}")
    print(f"  Total mismatches: {sum(agg['field_mm'].values()):,}")
    if failures:
        print(f"  FAILED contigs: {', '.join(failures)}")
    print("=" * 60)
    return 1 if failures else 0
```

Create `e2e-testing/scripts/run_comparison.py`:

```python
#!/usr/bin/env python3
"""Compare vepyr annotation against an Ensembl VEP reference.

Replaces run_annotation_fast.py and run_annotation_fast_all.py. See
e2e-testing/README.md for usage.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from comparison.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
```

Make it executable: `chmod +x e2e-testing/scripts/run_comparison.py`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_comparison_cli.py -v`
Expected: PASS, 12 tests

Then run the whole new suite:

Run: `uv run pytest tests/test_comparison_*.py -v`
Expected: PASS, 48 tests

- [ ] **Step 5: Smoke-test against real data**

Run: `uv run python e2e-testing/scripts/run_comparison.py --release 116 --profile refseq --chroms 22`
Expected: exit code 2, an error naming `116_GRCh38_refseq` and printing the availability table — this combination has a reference but no cache.

Run: `uv run python e2e-testing/scripts/run_comparison.py --release 115 --profile merged --chroms 22`
Expected: resolves `115_GRCh38_merged` and `output/115.2/..._hgvs_merged.vcf.gz`, detects contigs, and runs chr22 end to end.

- [ ] **Step 6: Lint and commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts tests/test_comparison_cli.py
uv run ruff format e2e-testing/scripts tests/test_comparison_cli.py
git add e2e-testing/scripts/comparison/cli.py e2e-testing/scripts/run_comparison.py tests/test_comparison_cli.py
git commit -m "feat(e2e): single run_comparison.py entry point for one or all contigs"
```

---

### Task 8: Retire the old runners and rewrite the docs

**Files:**
- Delete: `e2e-testing/scripts/run_annotation_fast.py`
- Delete: `e2e-testing/scripts/run_annotation_fast_all.py`
- Delete: `tests/test_run_annotation_fast.py`
- Modify: `e2e-testing/README.md:61-194`

**Interfaces:**
- Consumes: `run_comparison.py` from Task 7.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Confirm the old tests still pass before deleting**

Run: `uv run pytest tests/test_run_annotation_fast.py -q`
Expected: PASS, 20 tests. This is the baseline being replaced; if it already fails, stop and investigate rather than deleting evidence.

- [ ] **Step 2: Verify coverage parity before deleting**

Check that each behaviour in the old test file has an equivalent in the new suite:

| Old test | Replacement |
|---|---|
| `test_refseq_cache_profile_uses_data_vepyr_paths` | `test_resolve_derives_cache_and_reference_paths` (Task 3) |
| `test_flag_pick_profiles_use_matching_vep_references` | `test_resolve_derives_cache_and_reference_paths` (Task 3) |
| `test_fast_all_profile_suffixes_match_single_runner_profiles` | obsolete — one table now, drift is impossible |
| `test_parse_args_accepts_workers`, `test_parse_args_allows_parallel_workers` | `test_defaults`, `test_workers_must_be_positive` (Task 7) |
| `test_parse_args_defaults_to_plain_output` (both) | `test_defaults` (Task 7) |
| `test_parse_args_rejects_removed_backend_flag` (both) | `test_removed_flags_are_rejected` (Task 7) |
| `test_parse_args_accepts_profile_and_bgzf` (both) | `test_defaults` (Task 7) |
| `test_parse_args_accepts_legacy_cache_alias` (both) | `test_removed_flags_are_rejected` — the alias is intentionally dropped |
| `test_parse_args_accepts_skip_comparison_alias` (both) | `--skip-comparison` alias retained in `parse_args` (Task 7) |
| `test_main_preserves_requested_workers_for_single_chrom` | `test_annotate_contig_forwards_profile_kwargs` (Task 6) |
| `test_fast_all_run_chromosome_forwards_*` | `_run_contig_isolated` argv construction (Task 7) |
| `test_extract_chrom_from_vep_force_refreshes_cached_slice` | `test_slice_vep_reads_a_bgzf_reference` + force flag (Task 2) |
| `test_compare_vcfs_can_ignore_vep_hash_order_pick_csq_order` | `test_compare_can_ignore_vep_hash_order_csq_order` (Task 4) |

- [ ] **Step 3: Delete the old files**

```bash
cd /Users/mwiewior/research/git/vepyr
git rm e2e-testing/scripts/run_annotation_fast.py \
       e2e-testing/scripts/run_annotation_fast_all.py \
       tests/test_run_annotation_fast.py
rm -rf e2e-testing/scripts/__pycache__
```

- [ ] **Step 4: Rewrite the README section**

Replace `e2e-testing/README.md` lines 61-194 with a single section. Every example gains `--release`:

````markdown
### `run_comparison.py` -- vepyr vs Ensembl VEP parity

Annotates HG002 against a Parquet cache and compares the result field-by-field
against an Ensembl VEP reference. Handles one contig or all of them, and accepts
plain or block-gzipped VCFs on both sides.

`--release` is required: it selects both the Parquet cache and the VEP reference,
so the two can never be silently mismatched.

```bash
# All contigs detected from the reference index
uv run python run_comparison.py --release 115

# One contig
uv run python run_comparison.py --release 115 --chroms 22

# Several contigs, a different scenario and release
uv run python run_comparison.py --release 116 --profile merged --chroms 1 2 22

# Re-annotate instead of reusing existing output
uv run python run_comparison.py --release 115 --chroms 22 --force

# Block-gzipped output, validated as BGZF
uv run python run_comparison.py --release 115 --chroms 22 --bgzf

# Parallel within-contig pipelines
uv run python run_comparison.py --release 115 --chroms 22 --workers 4

# One subprocess per contig, so a native crash loses only that contig
uv run python run_comparison.py --release 115 --isolate

# Annotate only, no comparison
uv run python run_comparison.py --release 115 --chroms 22 --skip-compare

# Regenerate the summary from existing per-contig JSONs
uv run python run_comparison.py --release 115 --skip-annotate
```

Outputs:

- `results/{release}/_shared/normalized.vcf.gz` -- normalized input, shared by every
  contig of that release, re-created if `--vcf` changes
- `results/{release}/fast_{chrom}/` -- per-contig slices and annotated output
- `reports/fast_{chrom}_{profile}_{release}_report.json` -- per-contig comparison
- `reports/fast_{span}_{profile}_{release}_summary_{timestamp}.md` -- aggregate report

Defaults: `--profile merged`, reuse existing output (`--force` to re-annotate),
plain output (`--bgzf` for block-gzipped), `--workers 1`, normalization on
(`--no-normalize` to skip), contigs detected from the reference index.

Data layout under `$DATA_VEPYR_DIR` (default `~/workspace/data_vepyr`):

```
input/                                     # benchmark VCF, reference FASTA
cache/{release}_GRCh38_{flavour}/          # vepyr Parquet caches
output/{115.2,116}/                        # Ensembl VEP reference VCFs
```

Both `input/` and `cache/` fall back to the directory root with a warning, so the
runner works before and after the files are reorganised.

Run without arguments to see the full flag list, or pass an unavailable
profile/release pair to print the availability matrix.
````

- [ ] **Step 5: Verify nothing still references the deleted scripts**

Run:

```bash
cd /Users/mwiewior/research/git/vepyr
grep -rn "run_annotation_fast" --include='*.py' --include='*.md' --include='*.yml' --include='*.ipynb' . | grep -v '\.git/'
```

Expected: no output.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest tests/ -q`
Expected: PASS. The comparison tests are new; the golden and annotate tests are untouched by this change.

- [ ] **Step 7: Commit**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run ruff check e2e-testing/scripts tests/
uv run ruff format e2e-testing/scripts tests/
git add -A e2e-testing tests
git commit -m "refactor(e2e)!: replace the two comparison runners with run_comparison.py

BREAKING CHANGE: run_annotation_fast.py and run_annotation_fast_all.py are
removed. Use run_comparison.py, which requires --release and defaults to
--profile merged. --no-force and the --cache alias are dropped."
```

---

## Deferred: the `$DATA` reorganisation

**Not part of this plan.** The code prefers `input/` and `cache/` and falls back to the
legacy root with a warning, so it works before, during, and after the move.

The two halves have different safety constraints, so do them separately.

**The Parquet caches can move any time no vepyr annotation is running.** No container
mounts them — the VEP containers mount `homo_sapiens_*`, which stays put.

```bash
cd ~/workspace/data_vepyr
mkdir -p cache
mv 115_GRCh38_ensembl 115_GRCh38_merged 115_GRCh38_refseq 116_GRCh38_merged cache/
```

**The inputs must wait until no VEP container holds them.** At the time of writing an
Ensembl-116 run has `$DATA` bind-mounted as both `/fasta` and `/work` and is reading the
FASTA and `HG002_normalized.vcf.gz` by path; moving them mid-run risks killing a
multi-hour job.

```bash
docker ps          # must show no VEP container before proceeding

cd ~/workspace/data_vepyr
mkdir -p input
mv HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz* input/
mv HG002_normalized.vcf.gz* input/
mv Homo_sapiens.GRCh38.dna.primary_assembly.fa* input/
```

Then update the Docker `-v` paths and `--input_file` / `--fasta` arguments in
`docs/testing-vep.md` and `e2e-testing/vep-docker.md`, and rebuild the docs with
`uv run mkdocs build --strict`.

After both moves, confirm the fallback warnings stop appearing:

```bash
uv run python e2e-testing/scripts/run_comparison.py --release 115 --chroms 22 --skip-annotate
```

## Deferred: unavailable profile/release combinations

`merged_flag_pick` and `merged_pick_filter` have a cache at both releases but no VEP
reference was ever generated, so they fail fast under the new runner. Release 116 has
only `merged` fully available. Generating those references and building the missing 116
caches is separate work; see the availability matrix in the design document.
