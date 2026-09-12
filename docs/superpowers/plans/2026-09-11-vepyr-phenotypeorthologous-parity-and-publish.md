# vepyr: PhenotypeOrthologous parity, oracle references and publish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pin vepyr to the engine with `gff` + `interval` support, add the `merged_phenotypeorthologous` comparison profile with its VEP 116 oracle scripts, prove parity on chr22/chr1 and the whole normalized HG002 sample, document it, and publish the 25-contig cache to Hugging Face.

**Architecture:** One vepyr PR on branch `feat/gff-plugin-source-phenotypeorthologous` (already created; holds the spec and these plans). Oracle references are produced by two new bash scripts modelled on the five-plugin pair, writing BGZF directly. The comparison reuses `run_comparison.py` / `md5_concordance.py` unchanged through a new profile. Publishing is a manual `hf upload` with a hand-written card (the cache QA tool does not know interval shards yet).

**Tech Stack:** Python 3.12 / uv / maturin, Polars, pytest, bash, docker `ensemblorg/ensembl-vep:release_116.0`, `hf` CLI.

**Spec:** `docs/superpowers/specs/2026-09-11-gff-plugin-source-and-phenotypeorthologous-design.md`.

## Global Constraints

- Repo `/Users/mwiewior/research/git/vepyr`, branch `feat/gff-plugin-source-phenotypeorthologous`. GSD is bypassed for this work (user approval 2026-09-11).
- Rebuild the extension after any pin change with `env -u CONDA_PREFIX -u VIRTUAL_ENV uv run --no-sync maturin develop --release` (both env vars are set in this shell; with them maturin refuses and the stale `.venv` extension silently keeps running).
- Lint only through pre-commit: `uv run pre-commit run ruff --all-files` and `ruff-format` (`uv run ruff` falls through to pyenv). Note the hook's `--fix` strips imports whose consumer is added in a later commit.
- `DATA=~/workspace/data_vepyr`. Inputs: `input/HG002_norm.vcf.gz` (+`.tbi`), `input/Homo_sapiens.GRCh38.dna.primary_assembly.fa`, raw merged cache `homo_sapiens_merged/116_GRCh38`, Parquet cache `cache/116_GRCh38_merged`. The HG002 benchmark VCF covers chr1–22 only.
- VEP oracle recipe (do not vary): image `release_116.0`, `--cache --cache_version 116 --dir_cache /data --offline --merged --everything --no_stats --force_overwrite --vcf --compress_output bgzip --fasta …`, plugin code pinned by sha256 `89be8f30dd464f81913bef367831e8a08258e4085207f8291c956487f47de8ac` (VEP_plugins `a7e03a5c6497e29e0598eed7b4795f953d9a1b5f`).
- Source file md5 `20e5401a198d7d3db66a982c037d3ad4`, `.tbi` md5 `1eb833a26c247418910685289c6528cc`.
- Disk: 107 GiB free on 2026-09-11 after cleanup; the whole-sample references need ~1.5 GB, the cache < 50 MB.
- Commit trailers as in the engine plan. Draft PR; the human merges.

---

### Task V0: Baseline before any engine change lands (run once, first)

Record the current five-plugin chr22 parity so the engine PRs can be shown not to regress the point path.

- [ ] **Step 1: Run the existing comparison on master's extension**

```bash
cd /Users/mwiewior/research/git/vepyr
uv run python e2e-testing/scripts/run_comparison.py --release 116 --profile merged_plugins --chroms 22 \
  --plugin-cache ~/workspace/data_vepyr/plugin_cache_v0.1.1 --workers 4 --bgzf --force
uv run python e2e-testing/scripts/md5_concordance.py \
  --pair e2e-testing/results/116/fast_chr22/vep_chr22_merged_plugins.vcf \
         e2e-testing/results/116/fast_chr22/vepyr_parquet_chr22_merged_plugins.vcf.gz --mode strict
```

Expected: report `124/124 fields at 100%`, strict concordance exit 0. Copy the two md5 lines into `e2e-testing/reports/baseline_merged_plugins_chr22_2026-09-11.txt` (untracked, reports/ is gitignored) for the re-check in Task V9.

### Task V1: Pin the engine

**Files:**
- Modify: `Cargo.toml` (the `datafusion-bio-function-vep` `rev`), `Cargo.lock`

- [ ] **Step 1: Point at the engine PR 2 head** (interim; re-pin to the master squash SHA after both engine PRs merge)

```bash
sed -i '' 's/rev = "4b0bd00b5f4c5a40a40886cd6c0d230bb91a5554"/rev = "<PR2 head sha>"/' Cargo.toml
cargo update -p datafusion-bio-function-vep
env -u CONDA_PREFIX -u VIRTUAL_ENV uv run --no-sync maturin develop --release
uv run pytest tests/test_build_plugin_cache.py tests/test_annotate.py -q -x
```

Expected: builds; existing tests pass (point-lookup behaviour unchanged).

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: pin engine with gff provider and interval lookup"
```

### Task V2: Synthetic GFF fixture and interval-plugin tests

**Files:**
- Create: `tests/data/plugin_gff/demo.gff3` (plain text, 3 lines)
- Modify: `tests/test_build_plugin_cache.py:12-67` (add `_GFF_MANIFEST` next to `_UTF8_MANIFEST`)
- Modify: `tests/test_annotate.py:158-198` (new fixture `interval_plugin_cache` after `text_plugin_cache`) and a new class `TestIntervalPlugin`

**Interfaces:**
- Consumes: `_init_full_repo(root, manifest=...)` (writes `plugins/demo/demo.source.toml`, tags `v0.1.0`), `metadata_cache_dir`, `INPUT_VCF` (golden chr1, 100 records).
- Golden facts used: variants chr1:604358 G>C and chr1:604360 T>C are intronic in genes `ENSG00000225880` (LINC00115) and `ENSG00000293331`; chr1:611317 A>G is intronic in the same two genes.

- [ ] **Step 1: Write the fixture GFF**

`tests/data/plugin_gff/demo.gff3`:

```
##gff-version 3
1	test	gene	600000	605000	.	-	.	ID=gene:ENSG00000225880;gene_id=ENSG00000225880;Rat_gene_id=ENSRNOG00000000001;Rat_Orthologous_phenotype= leading space|two, comma;Mouse_gene_id=ENSMUSG00000000001;Mouse_Orthologous_phenotype=abnormal coat/hair pigmentation|hyperactivity
1	test	gene	604360	604360	.	+	.	ID=gene:ENSG00000293331;gene_id=ENSG00000293331;Rat_gene_id=ENSRNOG00000000002;Rat_Orthologous_phenotype=rat only
```

(tab-separated; the first row's span covers 604358 and 604360 but not 611317; the second row's one-base span covers only 604360 and has no mouse attributes.)

- [ ] **Step 2: Write the manifest constant** in `tests/test_build_plugin_cache.py` after `_UTF8_MANIFEST`:

```python
# A gene-span GFF source with an interval lookup keyed by {Gene}. Mirrors
# plugins/phenotypeorthologous in vepyr-plugins, at fixture scale.
_GFF_MANIFEST = '''\
plugin_name = "demo"
coordinate_system = "1-based"
lookup = "interval"
field_order = "alphabetical"
ingest_sql = """
SELECT chrom, start, "end", gene_id,
       "Mouse_gene_id" AS mouse_gene_id, "Mouse_Orthologous_phenotype" AS mouse_phenotype,
       "Rat_gene_id" AS rat_gene_id, "Rat_Orthologous_phenotype" AS rat_phenotype
FROM plugin_demo_src
"""

[[source]]
provider = "gff"
path = "placeholder.gff3"
  [source.gff]
  attributes = ["gene_id", "Mouse_gene_id", "Mouse_Orthologous_phenotype", "Rat_gene_id", "Rat_Orthologous_phenotype"]

[[match_column]]
column = "gene_id"
template = "{Gene}"

[[value_columns]]
column = "mouse_gene_id"
csq_field = "Demo_Mouse_geneid"
type = "Utf8"
[[value_columns]]
column = "mouse_phenotype"
csq_field = "Demo_Mouse_phenotype"
type = "Utf8"
[[value_columns]]
column = "rat_gene_id"
csq_field = "Demo_Rat_geneid"
type = "Utf8"
[[value_columns]]
column = "rat_phenotype"
csq_field = "Demo_Rat_phenotype"
type = "Utf8"
'''
```

- [ ] **Step 3: Write the failing fixture and tests** in `tests/test_annotate.py`:

```python
@pytest.fixture(scope="module")
def interval_plugin_cache(metadata_cache_dir, tmp_path_factory):
    """An interval (gene-span) plugin built from tests/data/plugin_gff/demo.gff3."""
    from tests.test_build_plugin_cache import _GFF_MANIFEST, _init_full_repo

    import vepyr

    root = tmp_path_factory.mktemp("interval_plugin")
    repo = _init_full_repo(root, manifest=_GFF_MANIFEST)
    plugin_root = root / "pc"
    built = vepyr.build_plugin_cache(
        "demo",
        "v0.1.0",
        source_path=str(TESTS_DIR / "data" / "plugin_gff" / "demo.gff3"),
        cache_dir=metadata_cache_dir,
        plugin_cache_root=str(plugin_root),
        plugins_repo=str(repo),
        chroms=["1"],
    )
    # Interval shards carry no tier information: every row is cold.
    assert built == [("chr1", 2, 0, 2)]
    return str(plugin_root)


def _plugin_tails(vcf_path, n_fields):
    """{(pos, gene): plugin tail} for every CSQ entry, tail = last n_fields tokens."""
    out = {}
    for line in Path(vcf_path).read_text().splitlines():
        if line.startswith("#"):
            continue
        cols = line.split("\t")
        pos = int(cols[1])
        csq = next((f[len("CSQ=") :] for f in cols[7].split(";") if f.startswith("CSQ=")), None)
        if csq is None:
            continue
        for entry in csq.split(","):
            tokens = entry.split("|")
            gene = tokens[4]  # Allele|Consequence|IMPACT|SYMBOL|Gene|...
            out[(pos, gene)] = "|".join(tokens[-n_fields:])
    return out


class TestIntervalPlugin:
    """lookup = "interval": span overlap + {Gene}, first row in file order, VEP escaping."""

    def test_gene_span_gates_and_values_escape_like_vep(
        self, interval_plugin_cache, metadata_cache_dir, tmp_path
    ):
        import vepyr

        output = tmp_path / "interval.vcf"
        vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
            fields="core",
            plugin_cache_root=interval_plugin_cache,
            plugins=["demo"],
            output_vcf=str(output),
            show_progress=False,
        )
        header = next(l for l in output.read_text().splitlines() if l.startswith("##INFO=<ID=CSQ"))
        assert header.rstrip('">').endswith(
            "Demo_Mouse_geneid|Demo_Mouse_phenotype|Demo_Rat_geneid|Demo_Rat_phenotype"
        ), "alphabetical field order"
        tails = _plugin_tails(output, 4)
        # Inside the LINC00115 span: all four fields; VEP escaping of the
        # leading space, the comma, the pipe and the whitespace runs.
        assert tails[(604358, "ENSG00000225880")] == (
            "ENSMUSG00000000001|abnormal_coat/hair_pigmentation&hyperactivity"
            "|ENSRNOG00000000001|_leading_space&two&_comma"
        )
        # Same variant, the other gene: its one-base span starts at 604360 → miss.
        assert tails[(604358, "ENSG00000293331")] == "|||"
        # Boundary: span [604360, 604360] overlaps the variant at 604360; no mouse attrs → empty.
        assert tails[(604360, "ENSG00000293331")] == "||ENSRNOG00000000002|rat_only"
        # Gene id matches but the variant lies outside the span (VEP's flank rule) → miss.
        assert tails[(611317, "ENSG00000225880")] == "|||"

    def test_lazyframe_columns_are_per_transcript_lists(
        self, interval_plugin_cache, metadata_cache_dir
    ):
        import polars as pl
        import vepyr

        lf = vepyr.annotate(
            INPUT_VCF,
            metadata_cache_dir,
            plugin_cache_root=interval_plugin_cache,
            plugins=["demo"],
            show_progress=False,
        )
        df = lf.select("pos", "Gene", "Demo_Rat_geneid", "Demo_Rat_phenotype").collect()
        assert df.schema["Demo_Rat_geneid"] == pl.List(pl.String)
        row = df.filter(pl.col("pos") == 604358).row(0, named=True)
        genes, rat = row["Gene"], row["Demo_Rat_geneid"]
        assert rat[genes.index("ENSG00000225880")] == "ENSRNOG00000000001"
        assert rat[genes.index("ENSG00000293331")] is None
```

`Gene` is a `List(String)` column in the LazyFrame (`docs/dataframes.md:50`), aligned per transcript with `Consequence` and with every plugin column of a match-column plugin, so indexing the plugin list by the gene's position is valid.

- [ ] **Step 4: Run to verify the tests fail before the pin (or pass after Task V1)**

Run: `uv run pytest tests/test_annotate.py -k Interval -q`
Expected before V1: the build fails (`provider must be one of …` / unknown key `lookup`). After V1: PASS. If the escaping assertion fails on `_leading_space`, inspect the actual tail: the shared escaper's whitespace rule is `\s+ → _`; a difference means the GFF reader trimmed the value (fix in the engine's provider test first).

- [ ] **Step 5: Commit**

```bash
git add tests/data/plugin_gff/demo.gff3 tests/test_build_plugin_cache.py tests/test_annotate.py
git commit -m "test: interval (gene-span) plugin fixture — span gating and VEP escaping"
```

### Task V3: Comparison profile

**Files:**
- Modify: `e2e-testing/scripts/comparison/profiles.py:63-70` (constants), `:138-152` (profiles dict)
- Modify: `tests/test_comparison_profiles.py:188-196` (`_write_plugin_reference(tmp_path, chrom=22, profile="merged_plugins")`) and new tests
- Modify: `tests/test_verify_parity_gate.py:364-382` (add `merged_phenotypeorthologous` to the refusal parametrisation)
- Modify: `e2e-testing/scripts/comparison/cli.py:14-21` (`DESCRIPTION` example) and `:136-141` (`--plugin-cache` help)
- Modify: `tests/test_comparison_cli.py:350-372` (parametrise the two plugin-profile tests over both plugin profiles)

`run_comparison.py` is a 16-line shim over `comparison.cli.main`, and the CLI's `--profile` choices are `sorted(profiles.PROFILES)` (`cli.py:44-46`), so registering the profile in `profiles.py` is what exposes `--profile merged_phenotypeorthologous`; the steps below make that visible in the help text and pin it with tests.

- [ ] **Step 1: Write the failing tests** (`tests/test_comparison_profiles.py`)

```python
def test_phenotypeorthologous_profile_attaches_only_that_plugin(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_VEPYR_DIR", str(tmp_path))
    (tmp_path / "cache" / "116_GRCh38_merged").mkdir(parents=True)
    plugin_cache = tmp_path / "cache" / "plugin_cache_116"
    plugin_cache.mkdir(parents=True)
    _write_plugin_reference(tmp_path, chrom=22, profile="merged_phenotypeorthologous")

    resolved = profiles.resolve("merged_phenotypeorthologous", "116", chrom=22)
    assert resolved.annotate_kwargs["plugins"] == ["phenotypeorthologous"]
    assert resolved.vep_vcf.endswith("HG002_chr22_phenotypeorthologous_vep116.vcf.gz")
    assert "/plugins/" in resolved.vep_vcf
```

Generalise `_write_plugin_reference` to take `profile="merged_plugins"` and use `profiles.PROFILES[profile].vep_per_contig`.

In `tests/test_comparison_cli.py`, parametrise the two existing plugin-profile tests and add a choices test:

```python
@pytest.mark.parametrize("profile", ["merged_plugins", "merged_phenotypeorthologous"])
def test_main_passes_the_single_requested_contig_to_resolve(monkeypatch, profile):
    seen = {}

    def fake_resolve(*args, **kwargs):
        seen.update(kwargs)
        raise profiles.ProfileUnavailable("stop here")

    monkeypatch.setattr(profiles, "resolve", fake_resolve)
    rc = cli.main(["--release", "116", "--profile", profile, "--chroms", "22"])
    assert rc == 2
    assert seen["chrom"] == "chr22"


def test_phenotypeorthologous_profile_is_a_cli_choice(capsys):
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--release", "116", "--profile", "no_such_profile"])
    assert excinfo.value.code == 2
    assert "merged_phenotypeorthologous" in capsys.readouterr().err
```

(argparse lists the valid choices in its error, so the second test proves the new profile is selectable from `run_comparison.py`.)

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_comparison_profiles.py -k phenotypeorthologous -q`
Expected: `ProfileUnavailable: Unknown profile 'merged_phenotypeorthologous'`.

- [ ] **Step 3: Implement** (`profiles.py`)

```python
# PhenotypeOrthologous is compared on its own reference (only that plugin
# loaded), one file per contig like the five-plugin set.
_PO_REFERENCE = "HG002_annotated_wgs_everything_hgvs_merged_phenotypeorthologous"
_PO_PER_CONTIG = "HG002_chr{chrom}_phenotypeorthologous_vep116"
```

and in `PROFILES`:

```python
    "merged_phenotypeorthologous": Profile(
        flavour="merged",
        vep_basename=_PO_REFERENCE,
        suffix="merged_phenotypeorthologous",
        plugins=("phenotypeorthologous",),
        vep_per_contig=_PO_PER_CONTIG,
        vep_subdir="plugins",
    ),
```

In `tests/test_verify_parity_gate.py`, extend the plugin-refusal test's parametrisation with `"merged_phenotypeorthologous"`.

In `cli.py`, add to `DESCRIPTION`:

```
    run_comparison.py --release 116 --profile merged_phenotypeorthologous --chroms 22 \
        --plugin-cache ~/workspace/data_vepyr/plugin_cache_v0.2.0   # one plugin, its own reference
```

and change the `--plugin-cache` help to `"Plugin cache root, used only by plugin profiles (merged_plugins, merged_phenotypeorthologous); default: $DATA/cache/plugin_cache_<release>"`.

- [ ] **Step 4: Run to verify**

Run: `uv run pytest tests/test_comparison_profiles.py tests/test_comparison_cli.py tests/test_verify_parity_gate.py -q && uv run python e2e-testing/scripts/run_comparison.py --help | grep -c phenotypeorthologous`
Expected: tests PASS; the help mentions the profile at least twice (choices + example).

- [ ] **Step 5: Commit**

```bash
git add e2e-testing/scripts/comparison/profiles.py e2e-testing/scripts/comparison/cli.py tests/test_comparison_profiles.py tests/test_comparison_cli.py tests/test_verify_parity_gate.py
git commit -m "feat(e2e): merged_phenotypeorthologous profile in run_comparison.py"
```

### Task V4: Oracle scripts

**Files:**
- Create: `e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh`
- Create: `e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh`

**Interfaces:**
- Builder: `./build_vep_phenotypeorthologous_reference.sh <chrom> [workdir]`; env `DATA_VEPYR_DIR`, `VEP_NORMALIZED_VCF` (default `$DATA/input/HG002_norm.vcf.gz`), `VEP_OUTPUT_VCF` (default `$DATA/output/116/plugins/HG002_chr<N>_phenotypeorthologous_vep116.vcf.gz`), `VEP_PLUGIN_DIR` (default `$DATA/output/116/plugins/plugin_code`). Writes `<out>.vcf.gz`, `.tbi`, `.plugins` sidecar (`PLUGIN PhenotypeOrthologous <sha256>` and `SOURCE <url> md5=<md5>`).
- Driver: `./generate_vep_phenotypeorthologous_references.sh [chroms…]` (default `1..22`), env `VEP_REFERENCE_JOBS` (default 2), resumable through the same 4-field header check + sidecar diff.

- [ ] **Step 1: Write the builder**

```bash
#!/usr/bin/env bash
#
# Build the Ensembl VEP 116 PhenotypeOrthologous reference for one chromosome:
# the golden output `run_comparison.py --profile merged_phenotypeorthologous`
# compares against. Same recipe as build_vep_plugin_reference.sh (merged cache,
# --everything, normalized input), with exactly one plugin loaded and BGZF
# written by VEP itself.
#
# Usage: ./build_vep_phenotypeorthologous_reference.sh 22 [workdir]
set -euo pipefail

CHROM="${1:?usage: $0 <chrom> [workdir]}"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
WORK="${2:-$DATA/vep116_po_chr${CHROM}}"
IMAGE="ensemblorg/ensembl-vep:release_116.0"
REF_DIR="$DATA/output/116/plugins"
PLUGIN_DIR="${VEP_PLUGIN_DIR:-$REF_DIR/plugin_code}"
SRC_DIR="$DATA/plugin_input/phenotypeorthologous"
SRC_NAME="PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
SRC_URL="https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/$SRC_NAME"
SRC_MD5="20e5401a198d7d3db66a982c037d3ad4"
TBI_MD5="1eb833a26c247418910685289c6528cc"
PLUGIN_COMMIT="a7e03a5c6497e29e0598eed7b4795f953d9a1b5f"
PLUGIN_SHA256="89be8f30dd464f81913bef367831e8a08258e4085207f8291c956487f47de8ac"
OUT="${VEP_OUTPUT_VCF:-$REF_DIR/HG002_chr${CHROM}_phenotypeorthologous_vep116.vcf.gz}"

mkdir -p "$WORK/input" "$REF_DIR" "$PLUGIN_DIR" "$SRC_DIR"

md5_file() {
  if command -v md5sum >/dev/null 2>&1; then md5sum "$1" | awk '{print $1}'; else md5 -q "$1"; fi
}
sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'; else shasum -a 256 "$1" | awk '{print $1}'; fi
}

# ---------------------------------------------------------------------------
# 1. Source data and plugin code, both pinned by digest
# ---------------------------------------------------------------------------
for f in "$SRC_NAME" "$SRC_NAME.tbi"; do
  if [[ ! -s "$SRC_DIR/$f" ]]; then
    curl --fail --silent --show-error -o "$SRC_DIR/$f.partial" "$SRC_URL${f#"$SRC_NAME"}"
    mv "$SRC_DIR/$f.partial" "$SRC_DIR/$f"
  fi
done
[[ "$(md5_file "$SRC_DIR/$SRC_NAME")" == "$SRC_MD5" ]] || { echo "ERROR: $SRC_NAME md5 mismatch" >&2; exit 1; }
[[ "$(md5_file "$SRC_DIR/$SRC_NAME.tbi")" == "$TBI_MD5" ]] || { echo "ERROR: $SRC_NAME.tbi md5 mismatch" >&2; exit 1; }

if [[ ! -s "$PLUGIN_DIR/PhenotypeOrthologous.pm" ]]; then
  curl --fail --silent --show-error \
    -o "$PLUGIN_DIR/PhenotypeOrthologous.pm" \
    "https://raw.githubusercontent.com/Ensembl/VEP_plugins/$PLUGIN_COMMIT/PhenotypeOrthologous.pm"
fi
actual="$(sha256_file "$PLUGIN_DIR/PhenotypeOrthologous.pm")"
[[ "$actual" == "$PLUGIN_SHA256" ]] || { echo "ERROR: PhenotypeOrthologous.pm sha256 $actual != $PLUGIN_SHA256" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2. Normalized input slice
# ---------------------------------------------------------------------------
NORM="${VEP_NORMALIZED_VCF:-$DATA/input/HG002_norm.vcf.gz}"
[[ -s "$NORM" && -s "$NORM.tbi" ]] || { echo "ERROR: normalized input $NORM (+.tbi) missing; see build_vep_plugin_reference.sh" >&2; exit 1; }
IN="$WORK/input/HG002_norm_chr${CHROM}.vcf.gz"
{ tabix -H "$NORM"; tabix "$NORM" "chr${CHROM}"; } | bgzip -c > "$IN"
tabix -f -p vcf "$IN"

# ---------------------------------------------------------------------------
# 3. Annotate (BGZF written by VEP, --compress_output bgzip)
# ---------------------------------------------------------------------------
to_container() {  # host path under $DATA → /data/...
  local p; p="$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"
  local d; d="$(cd "$DATA" && pwd -P)"
  [[ "$p" == "$d/"* ]] || { echo "ERROR: $1 must live under $DATA" >&2; exit 1; }
  printf '/data/%s\n' "${p#"$d/"}"
}
IN_C="$(to_container "$IN")"
OUT_C="$(to_container "$OUT")"
SRC_C="$(to_container "$SRC_DIR/$SRC_NAME")"

docker run --rm --user "$(id -u):$(id -g)" \
  -v "$DATA":/data -v "$PLUGIN_DIR":/plugins:ro "$IMAGE" \
  vep --cache --cache_version 116 --dir_cache /data --offline --merged \
      --everything --no_stats --force_overwrite --vcf --compress_output bgzip \
      --fasta /data/input/Homo_sapiens.GRCh38.dna.primary_assembly.fa \
      --input_file "$IN_C" --output_file "$OUT_C" \
      --dir_plugins /plugins \
      --plugin "PhenotypeOrthologous,file=$SRC_C"

# ---------------------------------------------------------------------------
# 4. Index and check the four plugin fields landed
# ---------------------------------------------------------------------------
tabix -f -p vcf "$OUT"
n_plugin=$(tabix -H "$OUT" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -c '^PhenotypeOrthologous_')
if [[ "$n_plugin" -ne 4 ]]; then
  echo "ERROR: expected 4 PhenotypeOrthologous CSQ fields, found $n_plugin" >&2
  grep -i 'failed to instantiate' "${OUT%.vcf.gz}.vcf_warnings.txt" >&2 || true
  exit 1
fi
{
  printf 'PLUGIN PhenotypeOrthologous %s\n' "$PLUGIN_SHA256"
  printf 'SOURCE %s md5=%s\n' "$SRC_URL" "$SRC_MD5"
} > "$OUT.plugins"
echo "OK: chr${CHROM} — $(bgzip -dc "$OUT" | grep -vc '^#') records, $n_plugin plugin CSQ fields"
echo "     $OUT"
```

If VEP rejects `--compress_output` in the 116.0 image, replace that flag with a pipe: write to `"${OUT%.gz}"` and follow with `bgzip -f "${OUT%.gz}"`; note the deviation in the script header.

- [ ] **Step 2: Write the driver**

```bash
#!/usr/bin/env bash
# Resumable per-contig VEP 116 PhenotypeOrthologous references (default chr1-22).
set -euo pipefail

DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
TARGET="$DATA/output/116/plugins"
JOBS="${VEP_REFERENCE_JOBS:-2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILDER="$SCRIPT_DIR/build_vep_phenotypeorthologous_reference.sh"
WORK_ROOT="$TARGET/.work_po"
LOG_DIR="$TARGET/logs"
mkdir -p "$TARGET" "$WORK_ROOT" "$LOG_DIR"

expected_provenance() {
  sed -n 's/^PLUGIN_SHA256="\([0-9a-f]\{64\}\)"$/PLUGIN PhenotypeOrthologous \1/p' "$BUILDER"
  local url md5
  url="$(sed -n 's/^SRC_URL="\(.*\)"$/\1/p' "$BUILDER")"
  md5="$(sed -n 's/^SRC_MD5="\([0-9a-f]\{32\}\)"$/\1/p' "$BUILDER")"
  # SRC_URL is composed from $SRC_NAME in the builder; expand it the same way.
  url="${url//\$SRC_NAME/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz}"
  printf 'SOURCE %s md5=%s\n' "$url" "$md5"
}

is_complete() {
  local out="$TARGET/HG002_chr${1}_phenotypeorthologous_vep116.vcf.gz"
  [[ -s "$out" && -s "$out.tbi" && -s "$out.plugins" ]] || return 1
  local n; n=$(tabix -H "$out" | grep -m1 '^##INFO=<ID=CSQ' | tr '|' '\n' | grep -c '^PhenotypeOrthologous_')
  [[ "$n" -eq 4 ]] || return 1
  diff -q <(sort "$out.plugins") <(expected_provenance | sort) >/dev/null
}

run_one() {
  local chrom="$1" log="$LOG_DIR/po_chr${1}.log"
  if is_complete "$chrom"; then echo "SKIP chr${chrom}"; return; fi
  local work; work=$(mktemp -d "$WORK_ROOT/chr${chrom}.XXXXXX")
  echo "START chr${chrom}: log=$log"
  if "$BUILDER" "$chrom" "$work" > "$log" 2>&1 && is_complete "$chrom"; then
    rm -rf "$work"; echo "DONE chr${chrom}"
  else
    echo "ERROR chr${chrom}: see $log (work kept at $work)" >&2; return 1
  fi
}

if [[ "$#" -gt 0 ]]; then chroms=("$@"); else chroms=({1..22}); fi
fail=0; pids=(); labels=()
for chrom in "${chroms[@]}"; do
  run_one "$chrom" & pids+=("$!"); labels+=("$chrom")
  if [[ "${#pids[@]}" -eq "$JOBS" ]]; then
    for i in "${!pids[@]}"; do wait "${pids[$i]}" || { echo "FAILED chr${labels[$i]}" >&2; fail=1; }; done
    pids=(); labels=()
  fi
done
for i in "${!pids[@]}"; do wait "${pids[$i]}" || { echo "FAILED chr${labels[$i]}" >&2; fail=1; }; done
[[ "$fail" -eq 0 ]] && echo "All requested PhenotypeOrthologous references are complete under $TARGET"
exit "$fail"
```

- [ ] **Step 3: Make executable, shellcheck, run chr22**

```bash
chmod +x e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh
shellcheck e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh || true
e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh 22
```

Expected: `OK: chr22 — 50861 records, 4 plugin CSQ fields` (record count equals the five-plugin chr22 reference) and the file under `output/116/plugins/`.

- [ ] **Step 4: Confirm the gene-id assumption on the oracle** (spec §11)

```bash
OUT=~/workspace/data_vepyr/output/116/plugins/HG002_chr22_phenotypeorthologous_vep116.vcf.gz
bgzip -dc "$OUT" | grep -v '^#' | head -2000 | tr ',' '\n' | awk -F'|' '$(NF-3)!="" || $(NF-1)!=""' | cut -d'|' -f5,7,$(( $(bgzip -dc "$OUT" | grep -m1 '##INFO=<ID=CSQ' | tr '|' '\n' | wc -l) - 3 ))- | head -5
```

Expected: populated lines whose Gene column is an `ENSG…` id present in the GFF for that gene; RefSeq lines (`Feature` starting `NM_`/`NR_`/`XM_`) always empty. If a populated line shows a gene id that is **not** in the GFF row overlapping that position, stop and report: the `_gene` object diverges from the Gene column and the discriminator needs a different attribute.

- [ ] **Step 5: Commit**

```bash
git add e2e-testing/scripts/build_vep_phenotypeorthologous_reference.sh e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh
git commit -m "feat(e2e): VEP 116 PhenotypeOrthologous reference builder and resumable driver"
```

### Task V4b: End-to-end comparison runner for the profile

**Files:**
- Create: `e2e-testing/scripts/run_phenotypeorthologous_comparison.sh`

**Interfaces:**
- `./run_phenotypeorthologous_comparison.sh [chroms…]` (default `1..22`). Env: `DATA_VEPYR_DIR`, `VEPYR_PLUGIN_REPO` (default `~/research/git/vepyr-plugins`), `VEPYR_PLUGIN_REF` (git ref of the catalog manifest, default `v0.2.0`), `VEPYR_PLUGIN_CACHE` (default `$DATA/plugin_cache_po_<ref>`), `VEP_COMPARISON_WORKERS` (default 4). Per chromosome: builds the reference if missing (Task V4 builder), builds the shard through `vepyr.build_plugin_cache` with strict verification, runs `run_comparison.py --profile merged_phenotypeorthologous`, then `md5_concordance.py --mode strict`, and records `strict_chr<N>.exit` under `$DATA/output/116/plugins/po_comparison_logs/`. Exit code 1 if any chromosome fails. This is the single command behind Tasks V5 and V6.
- Result files it reads (written by `run_comparison.py`): `e2e-testing/results/116/fast_chr<N>/vep_chr<N>_merged_phenotypeorthologous.vcf` (`vcfio.slice_vep`, `vcfio.py:366`) and `…/vepyr_parquet_chr<N>_merged_phenotypeorthologous.vcf.gz` (`cli.py:394-397`: `vepyr_{BACKEND}_{chrom}_{suffix}{ext}` with `BACKEND = "parquet"`, `chrom` canonical `chrN`, `ext = .vcf.gz` under `--bgzf`).

- [ ] **Step 1: Write the runner**

```bash
#!/usr/bin/env bash
# Build the PhenotypeOrthologous shard(s), compare vepyr with the VEP 116
# reference through run_comparison.py --profile merged_phenotypeorthologous,
# and gate each chromosome on strict body/header md5 concordance.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA="${DATA_VEPYR_DIR:-$HOME/workspace/data_vepyr}"
export DATA_VEPYR_DIR="$DATA"
PLUGIN_REPO="${VEPYR_PLUGIN_REPO:-$HOME/research/git/vepyr-plugins}"
PLUGIN_REF="${VEPYR_PLUGIN_REF:-v0.2.0}"
PLUGIN_CACHE="${VEPYR_PLUGIN_CACHE:-$DATA/plugin_cache_po_${PLUGIN_REF//\//_}}"
CACHE_DIR="$DATA/cache/116_GRCh38_merged"
SRC="$DATA/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
REF_DIR="$DATA/output/116/plugins"
LOG_DIR="$REF_DIR/po_comparison_logs"
WORKERS="${VEP_COMPARISON_WORKERS:-4}"
BUILDER="$SCRIPT_DIR/build_vep_phenotypeorthologous_reference.sh"
mkdir -p "$LOG_DIR"

if [[ "$#" -gt 0 ]]; then chroms=("$@"); else chroms=({1..22}); fi

# One build call for every requested chromosome: the builder verifies the
# source once and stages all shards before committing any of them.
build_shards() {
  local list; list="$(printf '"%s",' "${chroms[@]}")"
  (cd "$REPO_DIR" && uv run python - <<PYBUILD
import vepyr
print(vepyr.build_plugin_cache(
    "phenotypeorthologous", "$PLUGIN_REF",
    source_path="$SRC", cache_dir="$CACHE_DIR", plugin_cache_root="$PLUGIN_CACHE",
    chroms=[${list%,}], plugins_repo="$PLUGIN_REPO", overwrite=True, verify_source="strict"))
PYBUILD
  )
}

fail=0
[[ -s "$SRC" ]] || "$BUILDER" 22 >/dev/null   # the builder downloads and md5-checks the source
build_shards | tee "$LOG_DIR/build.log"
for chrom in "${chroms[@]}"; do
  ref="$REF_DIR/HG002_chr${chrom}_phenotypeorthologous_vep116.vcf.gz"
  [[ -s "$ref" && -s "$ref.tbi" ]] || "$BUILDER" "$chrom" > "$LOG_DIR/reference_chr${chrom}.log" 2>&1
  if ! (cd "$REPO_DIR" && uv run python e2e-testing/scripts/run_comparison.py \
      --release 116 --profile merged_phenotypeorthologous --chroms "$chrom" \
      --plugin-cache "$PLUGIN_CACHE" --workers "$WORKERS" --bgzf --force) \
      > "$LOG_DIR/compare_chr${chrom}.log" 2>&1; then
    echo "FAIL chr${chrom}: comparison error, see $LOG_DIR/compare_chr${chrom}.log" >&2; fail=1; continue
  fi
  results="$REPO_DIR/e2e-testing/results/116/fast_chr${chrom}"
  if (cd "$REPO_DIR" && uv run python e2e-testing/scripts/md5_concordance.py \
        --pair "$results/vep_chr${chrom}_merged_phenotypeorthologous.vcf" \
               "$results/vepyr_parquet_chr${chrom}_merged_phenotypeorthologous.vcf.gz" \
        --mode strict --explain --explain-limit 0) > "$LOG_DIR/strict_chr${chrom}.log" 2>&1; then
    echo 0 > "$LOG_DIR/strict_chr${chrom}.exit"; echo "PASS chr${chrom}: strict md5 concordant"
  else
    echo 1 > "$LOG_DIR/strict_chr${chrom}.exit"; echo "FAIL chr${chrom}: see $LOG_DIR/strict_chr${chrom}.log" >&2; fail=1
  fi
done
exit "$fail"
```

- [ ] **Step 2: Smoke-run on chr22 against the catalog branch** (before `v0.2.0` exists)

```bash
chmod +x e2e-testing/scripts/run_phenotypeorthologous_comparison.sh
VEPYR_PLUGIN_REF=feat/phenotypeorthologous e2e-testing/scripts/run_phenotypeorthologous_comparison.sh 22
```

Expected: `PASS chr22: strict md5 concordant` and a `e2e-testing/reports/fast_chr22_merged_phenotypeorthologous_116_report.json` showing the four fields with `both_nonempty_unequal = 0`.

- [ ] **Step 3: Commit**

```bash
git add e2e-testing/scripts/run_phenotypeorthologous_comparison.sh
git commit -m "feat(e2e): one-command PhenotypeOrthologous build + compare + strict gate"
```

### Task V5: Build the chr22 cache and run the parity gate

- [ ] **Step 1: Build chr22 + chr1 from the catalog branch** (the manifest lives on the vepyr-plugins branch until `v0.2.0` exists; `_resolve_plugin_manifest` accepts any git ref)

```bash
uv run python - <<'EOF'
import vepyr, os
D = os.path.expanduser("~/workspace/data_vepyr")
for c in ["22", "1"]:
    print(vepyr.build_plugin_cache(
        "phenotypeorthologous", "feat/phenotypeorthologous",
        source_path=f"{D}/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz",
        cache_dir=f"{D}/cache/116_GRCh38_merged",
        plugin_cache_root=f"{D}/plugin_cache_dev_po",
        chroms=[c], plugins_repo=os.path.expanduser("~/research/git/vepyr-plugins"),
        verify_source="strict"))
EOF
```

Expected: `[('chr22', 377, 0, 377)]` then `[('chr1', 1754, 0, 1754)]`.

- [ ] **Step 2: chr22 comparison, strict md5, workers 1 vs 4** (the Task V4b runner does build + compare + strict; run it twice with different worker counts and compare the two vepyr outputs)

```bash
for w in 1 4; do
  VEPYR_PLUGIN_REF=feat/phenotypeorthologous VEPYR_PLUGIN_CACHE=~/workspace/data_vepyr/plugin_cache_dev_po \
    VEP_COMPARISON_WORKERS=$w e2e-testing/scripts/run_phenotypeorthologous_comparison.sh 22
  cp e2e-testing/results/116/fast_chr22/vepyr_parquet_chr22_merged_phenotypeorthologous.vcf.gz /tmp/po_w$w.vcf.gz
done
uv run python e2e-testing/scripts/md5_concordance.py --pair /tmp/po_w1.vcf.gz /tmp/po_w4.vcf.gz --mode strict
```

Expected: the field report shows the four `PhenotypeOrthologous_*` fields with `both_nonempty_unequal = 0` and `vepyr_empty_only = vep_empty_only = 0`; both strict concordance runs exit 0. On a mismatch, read the JSONL ledger in `e2e-testing/reports/` for the first differing entry and classify: value escaping (engine `csq_escape`), span boundary (insertion swap), or gene id (see V4 step 4). Fix in the engine PR branch, re-pin, repeat.

- [ ] **Step 3: chr1**

`VEPYR_PLUGIN_REF=feat/phenotypeorthologous VEPYR_PLUGIN_CACHE=~/workspace/data_vepyr/plugin_cache_dev_po e2e-testing/scripts/run_phenotypeorthologous_comparison.sh 1` (builds the chr1 reference on first use, about an hour). Expected: 100% and `PASS chr1`.

- [ ] **Step 4: Record** the report paths and md5s in the PR description draft (`e2e-testing/reports/fast_chr22_merged_phenotypeorthologous_116_*`).

### Task V6: Whole-sample oracle (hours; start early, in the background)

- [ ] **Step 1: Launch**

```bash
mkdir -p ~/workspace/data_vepyr/output/116/plugins/logs
VEP_REFERENCE_JOBS=2 nohup e2e-testing/scripts/generate_vep_phenotypeorthologous_references.sh \
  > ~/workspace/data_vepyr/output/116/plugins/logs/po_all.log 2>&1 &
echo $! > ~/workspace/data_vepyr/output/116/plugins/logs/po_all.pid
```

Re-running the same command after an interruption skips completed contigs. Progress: `grep -E 'START|DONE|ERROR|SKIP' …/logs/po_all.log`.

- [ ] **Step 2: When complete, per-contig gate**

```bash
VEPYR_PLUGIN_REF=feat/phenotypeorthologous VEPYR_PLUGIN_CACHE=~/workspace/data_vepyr/plugin_cache_dev_po \
  e2e-testing/scripts/run_phenotypeorthologous_comparison.sh 2>&1 \
  | tee ~/workspace/data_vepyr/output/116/plugins/logs/po_strict_summary.txt
```

Expected: 22 × `PASS chr<N>: strict md5 concordant` and exit 0. This is the gate for tagging the Hub cache (Task V8).

### Task V7: Docs

**Files:**
- Modify: `docs/plugins.md:188-243` (manifest tables: `lookup` row, `provider` list, `[source.gff]` table), `:555-650` (Cache format: interval shard schema and "no tier join" note), `:682-733` (Supported plugins: sixth row, emitted-order row, note that `PhenotypeOrthologous` needs no licence caveat)
- Modify: `docs/downloads.md:183-200` (fifth dataset row + contig note: this cache covers chr1–22, X, MT; chrY has no rows)
- Modify: `docs/testing-vep.md:396-425` (profile note + the two new scripts)
- Modify: `docs/architecture.md:42` (one clause: "point or interval lookups"), `README.md:35` (plugin list), `e2e-testing/README.md:295-310` (two profile rows: `merged_plugins`, `merged_phenotypeorthologous`), `docs/dataframes.md:148` ("Plugin columns": interval plugins are per-transcript lists, gated by gene span)

- [ ] **Step 1: `docs/plugins.md` edits**

Top-level table, new row:

```markdown
| `lookup` | `point` \| `interval` | no (default `point`) | `point`: exact probe on `(start, allele_string, discriminators)` with variation-inherited tiering. `interval`: rows are genomic spans with no allele; a row matches when its `[start, end]` overlaps the variant's VEP-normalised span and every discriminator agrees, first row in file order. Use it for gene/region tracks (PhenotypeOrthologous). `ingest_sql` then projects `chrom, start, end` plus discriminator and value columns, and `allele_match` is rejected. |
```

`[[source]]` `provider` row: `csv \| tsv \| parquet \| vcf \| bed \| gff`, and a new `[source.gff]` table after `[source.csv]`:

```markdown
### `[source.gff]`

| Key | Type | Required | Description |
|---|---|---|---|
| `attributes` | array of strings | yes | GFF3 attribute keys exposed as flat nullable `Utf8` columns named exactly like the key (quote them in SQL: `"Rat_gene_id"`). Values are percent-decoded and not trimmed. The fixed columns are `chrom, start, end, type, source, score, strand, phase`. With `index = "tabix"` the BGZF file is sliced per chromosome; plain or gzip GFF is read whole. |
```

Shard schema section: add "For `lookup = "interval"` caches `allele_string` is absent and `tier` is always `1`: interval rows have no allele to inherit a tier from, so the variation join is skipped and rows are written in `(start, file order)`." Supported-plugins table row:

```markdown
| **PhenotypeOrthologous** | 4 | `{Gene}` + gene-span overlap | — (`interval`) | [`…plugin_phenotypeorthologous`](downloads.md#plugin-caches) | [Ensembl FTP](https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/) |
```

Emitted-order row: `PhenotypeOrthologous_Mouse_geneid`, `PhenotypeOrthologous_Mouse_phenotype`, `PhenotypeOrthologous_Rat_geneid`, `PhenotypeOrthologous_Rat_phenotype`. Update "All five" → "All six", "Four have a prebuilt cache" → "Five".

- [ ] **Step 2: Other docs** as listed in Files; in `docs/testing-vep.md` add:

```markdown
    `merged_phenotypeorthologous` reads its own per-contig reference
    (`HG002_chr{N}_phenotypeorthologous_vep116.vcf.gz`), produced by
    `build_vep_phenotypeorthologous_reference.sh <chrom>` or, for chr1–22,
    the resumable `generate_vep_phenotypeorthologous_references.sh`.
    `run_phenotypeorthologous_comparison.sh [chroms…]` chains the shard
    build, `run_comparison.py --profile merged_phenotypeorthologous` and the
    strict md5 gate in one command.
```

- [ ] **Step 3: Build docs and commit**

Run: `uv run mkdocs build --strict` (if mkdocs is in the venv; otherwise `uvx --with mkdocs-material mkdocs build --strict`).

```bash
git add docs/plugins.md docs/downloads.md docs/testing-vep.md docs/architecture.md docs/dataframes.md README.md e2e-testing/README.md
git commit -m "docs: gff sources, interval lookups and the PhenotypeOrthologous plugin"
```

### Task V8: Publish the cache (after vepyr-plugins `v0.2.0` is tagged and V6 is green)

- [ ] **Step 1: Build all 25 contigs from the tag, strict**

```bash
uv run python - <<'EOF'
import vepyr, os
D = os.path.expanduser("~/workspace/data_vepyr")
print(vepyr.build_plugin_cache("phenotypeorthologous", "v0.2.0",
    source_path=f"{D}/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz",
    cache_dir=f"{D}/cache/116_GRCh38_merged", plugin_cache_root=f"{D}/plugin_cache_v0.2.0",
    chroms=[str(c) for c in range(1, 23)] + ["X", "Y", "MT"], overwrite=True, verify_source="strict"))
EOF
```

Expected: 25 entries; `chrY` with `rows=0` and no shard; `chrMT` 13 rows; total rows 16,404 (the four unplaced contigs' 5 rows are outside the 25). `manifest.json` has `"lookup": "interval"`, `cache_source_version: "v0.2.0@<sha>"`, `sources[0].verified_md5 == md5`.

- [ ] **Step 2: Re-run the chr22 strict gate against this exact root**: `VEPYR_PLUGIN_REF=v0.2.0 VEPYR_PLUGIN_CACHE=~/workspace/data_vepyr/plugin_cache_v0.2.0 e2e-testing/scripts/run_phenotypeorthologous_comparison.sh 22` (the build step rebuilds chr22 in place from the same tag, byte-identical). Expected: `PASS chr22`.

- [ ] **Step 3: Write the card** `~/workspace/data_vepyr/plugin_cache_v0.2.0/plugin/phenotypeorthologous/README.md`, following `biodatageeks/vepyr_116_GRCh38_plugin_clinvar`'s card structure (download it with `hf download … README.md`): title, what it is, source url/md5/verified md5/.tbi md5, `cache_source_version`, lookup kind + match rule, CSQ fields and header descriptions, per-contig row table (from `manifest.json`), licence (Ensembl data, free use), how to download into `<root>/plugin/phenotypeorthologous/`, parity statement (chr1–22 strict concordance against VEP 116.0, date).

- [ ] **Step 4: Upload, tag, verify**

```bash
hf auth whoami
hf repos create biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous --type dataset
hf upload biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous \
  ~/workspace/data_vepyr/plugin_cache_v0.2.0/plugin/phenotypeorthologous . --type dataset \
  --commit-message "PhenotypeOrthologous cache v0.2.0 (VEP 116 GRCh38 merged, 25 contigs)"
hf repos tag create biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous v0.2.0 --type dataset --message "vepyr-plugins v0.2.0"
hf datasets info biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous --expand siblings --format json | jq '.siblings[].rfilename'
hf download biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous manifest.json --repo-type dataset --local-dir /tmp/po_check && diff /tmp/po_check/manifest.json ~/workspace/data_vepyr/plugin_cache_v0.2.0/plugin/phenotypeorthologous/manifest.json && echo manifest identical
```

Expected: 24 shards + `manifest.json` + `README.md` listed; the manifest diff is empty. Uploading is outward-facing: confirm with the user before `hf upload` unless they have already said to proceed.

### Task V9: Regression re-check, PR body, handoff

- [ ] **Step 1: Re-run V0** with the pinned extension and diff the md5 lines against `e2e-testing/reports/baseline_merged_plugins_chr22_2026-09-11.txt`. Expected: identical (point path unchanged).

- [ ] **Step 2: Full local test suite and lint**

```bash
uv run pytest -q
uv run pre-commit run ruff --all-files && uv run pre-commit run ruff-format --all-files
cargo clippy && cargo fmt --check
```

- [ ] **Step 3: Re-pin to the engine master SHA** once the engine PRs are squash-merged (`cargo update -p datafusion-bio-function-vep`, rebuild, `uv run pytest tests/test_annotate.py -k "Interval or Plugin" -q`), commit `chore: re-pin engine to master <sha>`.

- [ ] **Step 4: Push and open the draft PR**

```bash
git push -u origin feat/gff-plugin-source-phenotypeorthologous
gh pr create --draft --title "feat: PhenotypeOrthologous plugin via a GFF source and interval lookup" --body-file <(cat <<'EOF'
Pins the engine with `provider = "gff"` and `lookup = "interval"` (datafusion-bio-functions #<PR1>, #<PR2>), adds the `merged_phenotypeorthologous` comparison profile, the VEP 116 oracle scripts (BGZF output), a synthetic gene-span fixture test, and docs. Catalog manifest: vepyr-plugins #<PR3>, tagged v0.2.0.

Parity (VEP 116.0, merged cache, normalized HG002):
- chr22: 4/4 fields 100%, strict md5 PASS, workers 1 == 4 (<report path>)
- chr1: 4/4 fields 100%, strict md5 PASS
- chr1–22: 22/22 strict PASS (<summary path>)
- merged_plugins chr22 baseline unchanged (<md5>)

Cache: `biodatageeks/vepyr_116_GRCh38_plugin_phenotypeorthologous` @ v0.2.0 (25 contigs; chrY has no rows).

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01Ybip12LHW1qoorZjs3TwYT
EOF
)
```

- [ ] **Step 5: Review loop** per the vepyr-fix skill until green; hand the merge to the human.
