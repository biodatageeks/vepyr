# Catalog: PhenotypeOrthologous manifest and validator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish `plugins/phenotypeorthologous/phenotypeorthologous.source.toml` in `biodatageeks/vepyr-plugins`, with the validator, README and skill doc understanding `provider = "gff"`, `[source.gff]` and `lookup = "interval"`.

**Architecture:** One PR on `vepyr-plugins` (branch `feat/phenotypeorthologous`, based on `origin/master` at `3e1c039`). The validator is a single Python script run by CI; a small pytest file exercises the new rules with in-memory manifests. The manifest mirrors the engine contract from the engine plan (`docs/superpowers/plans/2026-09-11-engine-gff-provider-and-interval-lookup.md`). Release `v0.2.0` (minor: new plugin) is cut by a human via the Release workflow after merge.

**Tech Stack:** TOML, Python 3.11+ (`tomllib`), pytest, GitHub Actions.

**Spec:** vepyr `docs/superpowers/specs/2026-09-11-gff-plugin-source-and-phenotypeorthologous-design.md`.

## Global Constraints

- Repo: `/Users/mwiewior/research/git/vepyr-plugins`. Its checkout is on `fix/clinvar-numeric-id`; create a worktree from `origin/master` for this work.
- `url` must be the Ensembl release-116 FTP path; `md5` = `20e5401a198d7d3db66a982c037d3ad4` (computed locally on 2026-09-11 over the 3,424,946-byte file, because Ensembl's `CHECKSUMS` file there uses BSD `sum`). Say so in a comment.
- Top-level scalar keys precede every `[[table]]` header (TOML absorption rule).
- Validator exit code 1 on any error; CI (`.github/workflows/validate-manifests.yml`) runs `python scripts/validate_manifests.py`.
- Commit trailers as in the engine plan.

---

### Task 1: Validator rules for `gff`, `[source.gff]` and `lookup`

**Files:**
- Modify: `scripts/validate_manifests.py:16` (`PROVIDERS`), `:18-19` (constants), `:90-105` (top-level checks), `:129-158` (source checks)
- Create: `tests/test_validate_manifests.py`
- Modify: `.github/workflows/validate-manifests.yml` (add `pip install pytest && pytest -q tests`)

**Interfaces:**
- Produces: `LOOKUPS = {"point", "interval"}`; `validate_manifest(path: Path, errors: list[str]) -> None` unchanged signature.

- [ ] **Step 1: Write the failing tests**

`tests/test_validate_manifests.py`:

```python
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_manifests as vm  # noqa: E402

GFF = '''\
plugin_name = "demo"
coordinate_system = "1-based"
lookup = "interval"
field_order = "alphabetical"
ingest_sql = "SELECT chrom, start, \\"end\\", gene_id, \\"Rat_gene_id\\" AS rat FROM plugin_demo_src"

[[source]]
provider = "gff"
path = "demo.gff3.gz"
url = "https://example.org/demo.gff3.gz"
md5 = "20e5401a198d7d3db66a982c037d3ad4"
index = "tabix"
  [source.gff]
  attributes = ["gene_id", "Rat_gene_id"]

[[match_column]]
column = "gene_id"
template = "{Gene}"

[[value_columns]]
column = "rat"
csq_field = "Demo_Rat"
type = "Utf8"
'''


def _errors(tmp_path: Path, body: str) -> list[str]:
    d = tmp_path / "plugins" / "demo"
    d.mkdir(parents=True)
    p = d / "demo.source.toml"
    p.write_text(body, encoding="utf-8")
    errors: list[str] = []
    vm.validate_manifest(p, errors)
    return errors


def test_gff_interval_manifest_is_valid(tmp_path):
    assert _errors(tmp_path, GFF) == []


def test_gff_requires_attributes(tmp_path):
    body = GFF.replace('  [source.gff]\n  attributes = ["gene_id", "Rat_gene_id"]\n', "")
    assert any("source[0].gff" in e and "required" in e for e in _errors(tmp_path, body))
    body = GFF.replace('attributes = ["gene_id", "Rat_gene_id"]', "attributes = []")
    assert any("non-empty" in e for e in _errors(tmp_path, body))


def test_gff_table_rejected_for_other_providers(tmp_path):
    body = GFF.replace('provider = "gff"', 'provider = "bed"').replace('index = "tabix"\n', "")
    assert any("source[0].gff is only valid for provider gff" in e for e in _errors(tmp_path, body))


def test_tabix_allowed_for_gff(tmp_path):
    assert not any("index='tabix'" in e for e in _errors(tmp_path, GFF))


def test_lookup_values(tmp_path):
    body = GFF.replace('lookup = "interval"', 'lookup = "span"')
    assert any("lookup must be one of" in e for e in _errors(tmp_path, body))


def test_interval_rejects_allele_match(tmp_path):
    body = GFF.replace('lookup = "interval"', 'lookup = "interval"\nallele_match = "minimised"')
    assert any("allele_match" in e and "interval" in e for e in _errors(tmp_path, body))


def test_point_default_still_accepts_existing_manifests():
    errors: list[str] = []
    for path in sorted(vm.ROOT.glob(vm.MANIFEST_GLOB)):
        vm.validate_manifest(path, errors)
    assert errors == []
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd <worktree> && python -m pytest -q tests/test_validate_manifests.py`
Expected: `test_gff_interval_manifest_is_valid` fails with `source[0].provider must be one of […]`; the negative tests fail because the errors are not produced.

- [ ] **Step 3: Implement**

`scripts/validate_manifests.py`:

```python
PROVIDERS = {"vcf", "csv", "tsv", "parquet", "bed", "gff"}
LOOKUPS = {"point", "interval"}
TABIX_PROVIDERS = {"csv", "tsv", "vcf", "gff"}
```

After the `field_order` check:

```python
    lookup = manifest.get("lookup", "point")
    if lookup not in LOOKUPS:
        errors.append(f"{path}: lookup must be one of {sorted(LOOKUPS)}")
    elif lookup == "interval" and manifest.get("allele_match", "exact") != "exact":
        errors.append(
            f"{path}: allele_match has no meaning with lookup='interval' "
            "(interval rows carry no allele); remove it"
        )
```

In the source loop, replace the tabix provider set with `TABIX_PROVIDERS` and add after the `csv` checks:

```python
            gff = source.get("gff")
            if provider == "gff":
                if not isinstance(gff, dict):
                    errors.append(f"{path}: {label}.gff table is required for provider gff")
                else:
                    attributes = gff.get("attributes")
                    if (
                        not isinstance(attributes, list)
                        or not attributes
                        or not all(isinstance(a, str) and a for a in attributes)
                    ):
                        errors.append(
                            f"{path}: {label}.gff.attributes must be a non-empty list of strings"
                        )
            elif gff is not None:
                errors.append(f"{path}: {label}.gff is only valid for provider gff")
```

Update the tabix message to `"index='tabix' is supported only for csv/tsv/vcf/gff"`.

`.github/workflows/validate-manifests.yml`: after the existing validator step add

```yaml
      - run: pip install pytest && pytest -q tests
```

- [ ] **Step 4: Run to verify they pass**

Run: `python -m pytest -q tests/test_validate_manifests.py && python scripts/validate_manifests.py`
Expected: all pass; validator prints `Validated 5 plugin source manifests`.

- [ ] **Step 5: Commit**

```bash
git add scripts/validate_manifests.py tests/test_validate_manifests.py .github/workflows/validate-manifests.yml
git commit -m "feat(validator): accept gff sources, [source.gff].attributes and lookup=interval"
```

### Task 2: The manifest

**Files:**
- Create: `plugins/phenotypeorthologous/phenotypeorthologous.source.toml`
- Create: `plugins/phenotypeorthologous/README.md`

- [ ] **Step 1: Write the manifest**

```toml
# PhenotypeOrthologous — phenotypes of rat and mouse orthologues, keyed by the
# Ensembl gene overlapping the variant (Ensembl VEP `PhenotypeOrthologous.pm`,
# VEP_plugins release/116, sha256 89be8f30dd464f81913bef367831e8a08258e4085207f8291c956487f47de8ac).
#
# VEP queries the GFF with the VARIANT's span and keeps the first record whose
# `gene_id` equals the transcript's gene stable id, so a flanking variant only
# matches while it still lies inside the gene span. `lookup = "interval"`
# reproduces exactly that: span overlap + `{Gene}` discriminator, first row in
# file order. The plugin's optional `model=rat|mouse` filter is not modelled;
# all four fields are always emitted (empty where the file has no value).
plugin_name       = "phenotypeorthologous"
coordinate_system = "1-based"
lookup            = "interval"
field_order       = "alphabetical"   # loaded with --plugin: VEP sorts header keys
assume_unique     = true             # one row per gene_id in the file (16,409 rows, 0 dups)
ingest_sql = """
SELECT chrom,
       start,
       "end",
       gene_id,
       "Mouse_gene_id"               AS mouse_gene_id,
       "Mouse_Orthologous_phenotype" AS mouse_phenotype,
       "Rat_gene_id"                 AS rat_gene_id,
       "Rat_Orthologous_phenotype"   AS rat_phenotype
FROM plugin_phenotypeorthologous_src
"""

[[source]]
provider = "gff"
path     = "PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
url      = "https://ftp.ensembl.org/pub/release-116/variation/PhenotypeOrthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz"
# Computed locally on 2026-09-11 (3,424,946 bytes): the CHECKSUMS file in that
# directory is BSD `sum` output, not MD5. Sibling .tbi md5: 1eb833a26c247418910685289c6528cc.
md5      = "20e5401a198d7d3db66a982c037d3ad4"
index    = "tabix"
  [source.gff]
  attributes = [
    "gene_id",
    "Mouse_gene_id",
    "Mouse_Orthologous_phenotype",
    "Rat_gene_id",
    "Rat_Orthologous_phenotype",
  ]

[[match_column]]
column   = "gene_id"
template = "{Gene}"

[[value_columns]]
column      = "mouse_gene_id"
csq_field   = "PhenotypeOrthologous_Mouse_geneid"
type        = "Utf8"
description = "PhenotypeOrthologous MouseGene associated with Mouse"

[[value_columns]]
column      = "mouse_phenotype"
csq_field   = "PhenotypeOrthologous_Mouse_phenotype"
type        = "Utf8"
description = "PhenotypeOrthologous MousePhenotypes associated with orthologous genes in Mouse"

[[value_columns]]
column      = "rat_gene_id"
csq_field   = "PhenotypeOrthologous_Rat_geneid"
type        = "Utf8"
description = "PhenotypeOrthologous RatGene associated with Rat"

[[value_columns]]
column      = "rat_phenotype"
csq_field   = "PhenotypeOrthologous_Rat_phenotype"
type        = "Utf8"
description = "PhenotypeOrthologous RatPhenotypes associated with orthologous genes in Rat"
```

- [ ] **Step 2: Write the per-plugin README**

`plugins/phenotypeorthologous/README.md`:

```markdown
# PhenotypeOrthologous

Source: Ensembl release-116 FTP,
`variation/PhenotypeOrthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz`
(BGZF + `.tbi`, 3.4 MB, 16,409 gene features on chr1–22, X, MT and four
unplaced contigs; no chrY). GRCh38 only, as the upstream plugin enforces.

The file is used as published — no preprocessing — so `verify_source="strict"`
applies. Ensembl's `CHECKSUMS` there is BSD `sum` output; the manifest `md5`
was computed on the downloaded file.

Match rule (from `PhenotypeOrthologous.pm`): tabix query by the variant's span,
first record whose `gene_id` equals the transcript's gene stable id. Fields
are emitted alphabetically (VEP sorts plugin header keys). RefSeq transcripts
in a merged cache never match (their gene id is not an ENSG id). Licence:
Ensembl data, free for any use (see the Ensembl FTP README).
```

- [ ] **Step 3: Validate**

Run: `python scripts/validate_manifests.py && python -m pytest -q tests`
Expected: `Validated 6 plugin source manifests`; tests pass (the existing-manifests test now covers the new file).

- [ ] **Step 4: Build chr22 against the engine PR branch** (requires the engine plan's PR 2 branch checked out in `/Users/mwiewior/research/git/datafusion-bio-functions` worktree `<engine-wt>`; the GFF and `.tbi` downloaded to `~/workspace/data_vepyr/plugin_input/phenotypeorthologous/`)

```bash
cd <engine-wt>
cargo run -p datafusion-bio-function-vep --features parquet-cache --example build_plugin -- \
  --manifest <catalog-wt>/plugins/phenotypeorthologous/phenotypeorthologous.source.toml \
  --source-path ~/workspace/data_vepyr/plugin_input/phenotypeorthologous/PhenotypesOrthologous_homo_sapiens_112_GRCh38.gff3.gz \
  --variation-cache-dir ~/workspace/data_vepyr/cache/116_GRCh38_merged \
  --out /tmp/po_cache --chrom 22 --verify-source strict
```

Expected output: `chr22 rows=377 warm=0 cold=377` and `wrote plugin/phenotypeorthologous/manifest.json`. Then check the schema:

```bash
uv run --project /Users/mwiewior/research/git/vepyr python -c "
import polars as pl; df = pl.read_parquet('/tmp/po_cache/plugin/phenotypeorthologous/chr22.parquet')
print(df.schema); print(df.head(3))"
```

Expected columns: `chrom, start, end, gene_id, mouse_gene_id, mouse_phenotype, rat_gene_id, rat_phenotype, tier`; first row gene `ENSG00000198445` at 16590751–16592810 (the first chr22 feature in the file).

- [ ] **Step 5: Commit**

```bash
git add plugins/phenotypeorthologous/
git commit -m "feat(phenotypeorthologous): gff-sourced interval manifest for Ensembl's PhenotypeOrthologous plugin"
```

### Task 3: README and skill doc

**Files:**
- Modify: `README.md:24-45` (manifest table + "All five" paragraph), `:97-127` (Adding a plugin step 1 and 4)
- Modify: `.claude/skills/adding-a-plugin/SKILL.md:111-200` (§1 source formats: add a GFF bullet), and add a short "§1b. Lookup kinds" subsection after it

- [ ] **Step 1: README edits**

Add the table row after dbNSFP:

```markdown
| **PhenotypeOrthologous** | [`plugins/phenotypeorthologous`](plugins/phenotypeorthologous/phenotypeorthologous.source.toml) | GFF3 (tabix), `lookup = "interval"` | `{Gene}` + span overlap | 4 | ✅ |
```

Change "All five are validated" to "All six are validated against golden Ensembl VEP 116 output. Five have a prebuilt cache…". In "Adding a plugin" step 1 add "gene-span GFF → `phenotypeorthologous`"; in step 4 add "`lookup`, `[source.gff]`" to the list of things the validator checks.

- [ ] **Step 2: Skill doc edits**

Append to the §1 list:

```markdown
- **GFF3** (features with `key=value;` attributes): `provider = "gff"` via
  `datafusion-bio-format-gff`. Declare the attribute keys you need in
  `[source.gff].attributes`; each becomes a flat nullable Utf8 column with the
  attribute's exact name (quote it in SQL when it has upper-case letters:
  `"Rat_gene_id"`). The eight fixed columns are `chrom, start, end, type,
  source, score, strand, phase`. Values are percent-decoded (`%3B` → `;`) and
  never trimmed. BGZF + `.tbi` sources take `index = "tabix"` and are sliced
  per chromosome; plain or gzip GFF is read whole.
```

New subsection:

```markdown
## 1b. Lookup kinds: `point` (default) vs `interval`

`point` is the exact probe on `(start, allele_string, <match…>)` with tiering
inherited from the variation cache — every per-variant scoring plugin.

`interval` is for a source whose rows are genomic spans with no allele (gene
or region tracks). The shard drops `allele_string`, skips the tier join, keeps
file order per `(start)`, and at runtime a row matches when its `[start, end]`
overlaps the variant's VEP-normalised span **and** every `[[match_column]]`
discriminator agrees (an interval tree per discriminator, so dense tracks are
fine); the first row in file order wins. Decide by reading the
Ensembl plugin's `run()`: if it calls `get_data($vf->{chr}, $vf_start,
$vf_end)` and filters by an id, that is `interval` + a template such as
`{Gene}`. `allele_match` is rejected for `interval`.
```

- [ ] **Step 3: Validate docs render and commit**

Run: `python scripts/validate_manifests.py` (no change expected) and view the README diff.

```bash
git add README.md .claude/skills/adding-a-plugin/SKILL.md
git commit -m "docs: GFF sources, interval lookup and the PhenotypeOrthologous row"
```

### Task 4: Draft PR and release handoff

- [ ] **Step 1: Push and open the draft PR**

```bash
git push -u origin feat/phenotypeorthologous
gh pr create --draft --title "feat: PhenotypeOrthologous manifest (gff source, interval lookup)" --body-file <(cat <<'EOF'
Adds `plugins/phenotypeorthologous/`, sourced from Ensembl's release-116 GFF3 (md5-pinned, tabix), keyed by gene span + `{Gene}`. Validator learns `provider = "gff"`, `[source.gff].attributes` and `lookup = "interval"`.

Requires datafusion-bio-functions PRs #<PR1> and #<PR2>. Parity evidence lands in vepyr PR #<PR4>. Release as `v0.2.0` (minor: new plugin) after merge.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01Ybip12LHW1qoorZjs3TwYT
EOF
)
```

- [ ] **Step 2: Handoff note for the human**: after merge, run the Release workflow with `bump=minor` (→ `v0.2.0`); the vepyr plan's publish task pins that tag in `build_plugin_cache(..., "v0.2.0", ...)` and records it as `cache_source_version`.
