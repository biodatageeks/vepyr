# Download published caches from Hugging Face — design

Date: 2026-10-12
Status: approved (written spec reviewed 2026-10-13)
Builds on: the `build-cache` and `build-plugin-cache` subcommands (#181, merged
to `master`). This branch (`feat/download-cache`) starts from `master`.

## Goal

A user can fetch a published vepyr Parquet cache or plugin cache with one Python
call or one command, optionally for selected chromosomes only, and get a
directory that `annotate()` accepts as is:

```python
vepyr.download_cache(116, "~/vepyr_cache", cache_type="merged", chroms=["22"])
vepyr.download_plugin_cache("clinvar", 116, "~/vepyr_plugin_cache")
```

```bash
vepyr download-cache --release 116 --cache_type merged --dir_cache ~/vepyr_cache --chrom 22
vepyr download-plugin-cache --plugin clinvar --release 116 --plugin_cache_root ~/vepyr_plugin_cache
```

Today the same requires installing the Hugging Face CLI and typing the
`hf download … --include …` commands from `docs/downloads.md`, including the
sidecar files a partial cache needs.

## Non-goals

- Uploading or publishing caches.
- The WUT OneDrive mirror.
- Release 115 or other assemblies: nothing is published for them. The API takes
  `release` and `assembly`, so they work once datasets and pins exist.
- dbNSFP: its licence forbids redistribution, so there is no dataset.
- An `overwrite` option. Re-running resumes and skips files already present,
  which is `snapshot_download`'s behaviour.

## Published datasets

Core caches: `biodatageeks/vepyr_{release}_{assembly}_{cache_type}`.
Plugin caches: `biodatageeks/vepyr_{release}_{assembly}_plugin_{plugin}`.
All are public dataset repositories (`repo_type="dataset"`).

Layout on Hugging Face (checked 2026-10-12):

- Core: `<entity>/<contig>.parquet` for `exon`, `transcript`,
  `translation_core`, `translation_sift`, `variation`, `motif`, `regulatory`;
  contigs are `chr1`–`chr22`, `chrX`, `chrY`, `chrMT` and unplaced/patch contigs
  such as `GL000008.2` and `HG1012_PATCH`. Each entity directory has a
  `chrom_manifest.json`; the root has `reference_policy.json` and
  `chr_synonyms.txt`, plus `README.md` and `.gitattributes`.
- Plugin: flat `chr<N>.parquet`, `manifest.json`, for ClinVar also
  `qa_profile.json`, plus `README.md` and `.gitattributes`.

## Python API

Two new public functions in `vepyr/__init__.py`, mirroring `build_cache` and
`build_plugin_cache`:

```python
def download_cache(
    release: int,
    cache_dir: str,
    *,
    cache_type: str,                  # "ensembl" | "merged" | "refseq"
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:                             # <cache_dir>/<release>_<assembly>_<cache_type>

def download_plugin_cache(
    plugin: str,
    release: int,
    plugin_cache_root: str,
    *,
    assembly: str = "GRCh38",
    chroms: list[str] | None = None,
    revision: str | None = None,
    show_progress: bool = True,
) -> str:                             # <plugin_cache_root>/plugin/<plugin>
```

The returned path is what `annotate(cache_dir=…)` and
`annotate(plugin_cache_root=…, plugins=[plugin])` take. `cache_dir` and
`plugin_cache_root` are expanded with `os.path.expanduser`; missing directories
are created.

### Revisions

A new module `vepyr/_published.py` holds the pin table: a dict from
`(release, assembly, name)` to a full 40-character commit hash, where `name` is
`ensembl`, `merged`, `refseq` or `plugin_<plugin>`. Initial content:

| name | revision |
|---|---|
| `ensembl` | `058d887ec86163333fcbdd20c28ef75683faa8b5` |
| `refseq` | `ae28273ce0b4744201ddaecd71a7ea8bf2c19d95` |
| `merged` | `5b83dd8d249106c6cc3f1c04c522b4bec716cc97` |
| `plugin_cadd` | `bcfca1e030b6d372086f9b9f2bf7aae3295d321a` |
| `plugin_spliceai` | `7158a9c34758b1afe8a56b9e433640b9d3845920` |
| `plugin_alphamissense` | `00558a86d2ec30c5ed68ff291164786228534b19` |
| `plugin_clinvar` | `44b0029ed830bd5d4666b5575610e6f3ae56a2ba` |
| `plugin_phenotypeorthologous` | `08b9b2ff0ae9a8d09c04ff1e6cb4ebb6838fd534` |

All for release 116, GRCh38.

- `revision=None` uses the pinned hash. If there is no pin, the call raises
  `ValueError` listing the published caches (or plugins) and pointing to
  `build_cache` / `build_plugin_cache`. For `plugin="dbnsfp"` the message says
  it is supported but cannot be redistributed and must be built locally.
- Any other `revision` (branch, tag or hash, including `"main"`) is passed to
  Hugging Face unchanged, so unpinned datasets can still be fetched explicitly.
- `cache_type` is validated against `ensembl`/`merged`/`refseq` with the same
  check `build_cache` uses.

### File selection

The download always uses `allow_patterns`, so `README.md` and `.gitattributes`
are never fetched and the directory matches what `build_cache` /
`build_plugin_cache` write.

| Case | `allow_patterns` |
|---|---|
| Core, all contigs | `*/*.parquet`, `*/chrom_manifest.json`, `reference_policy.json`, `chr_synonyms.txt` |
| Core, `chroms` | `*/<contig>.parquet` for each requested contig, plus the three sidecar patterns above |
| Plugin, all contigs | `*.parquet`, `*.json` |
| Plugin, `chroms` | `<contig>.parquet` for each requested contig, plus `*.json` |

Contig names in `chroms`:

1. Before downloading, the function lists the dataset files at the resolved
   revision (`HfApi.list_repo_files`) and derives the available contigs from
   the shard names.
2. Each requested name is matched as given; if that fails and it has no `chr`
   prefix, `chr` + name is tried (`"22"` → `chr22`, `"X"` → `chrX`;
   `"GL000008.2"` matches as given).
3. Names that match nothing raise `ValueError` naming them, before any file is
   downloaded. Duplicates after normalisation are collapsed.
4. An empty list is a `ValueError` (it would download only sidecars).

### Download

One `huggingface_hub.snapshot_download(repo_id, repo_type="dataset",
revision=…, local_dir=…, allow_patterns=…)` call per function. Progress bars
are the library's own; `show_progress=False` disables them for that call only
(via `huggingface_hub.utils.disable_progress_bars()` / `enable_progress_bars()`
in a `try`/`finally`, restoring the previous state).

`snapshot_download` with `local_dir` keeps resume metadata in
`<local_dir>/.cache/huggingface/`. It stays: it is what makes re-running
resume and skip. A test asserts that `annotate()` opens a downloaded directory
that contains it.

Standard Hugging Face environment variables apply unchanged (`HF_ENDPOINT`,
`HF_HUB_OFFLINE`, `HF_TOKEN`); vepyr adds none.

### Errors

Library exceptions are translated so that the CLI's existing handling
(`ValueError`, `FileNotFoundError`, `RuntimeError` → exit 2, one line) covers
them. Every message names the repository and revision.

| Situation | Raised |
|---|---|
| No pin and no `revision`; unknown `cache_type`; dbNSFP | `ValueError` |
| Unknown or empty `chroms` | `ValueError` |
| `RepositoryNotFoundError`, `RevisionNotFoundError` | `ValueError` |
| Other `HfHubHTTPError`, connection errors, `LocalEntryNotFoundError` (offline without the files) | `RuntimeError` |

## CLI

Two subcommands in `src/vepyr/cli.py`, following the `build-cache` pattern from
#181 (an `_add_…_parser` function, a `…_kwargs()` helper, a branch in
`main()`):

```
vepyr download-cache --release N --cache_type {ensembl,merged,refseq} --dir_cache DIR
    [--assembly NAME] [--chrom NAME ...] [--revision REV] [--no_progress]

vepyr download-plugin-cache --plugin NAME --release N --plugin_cache_root DIR
    [--assembly NAME] [--chrom NAME ...] [--revision REV] [--no_progress]
```

Flags map one to one to the keywords (`--chrom` → `chroms`, `--no_progress` →
`show_progress=False`). On success each prints:

```
Downloaded biodatageeks/vepyr_116_GRCh38_merged @ 5b83dd8 (chr22) -> /home/u/vepyr_cache/116_GRCh38_merged
Next: vepyr annotate --dir_cache /home/u/vepyr_cache/116_GRCh38_merged ...
```

For plugins the next step is `--plugin_cache_root DIR --plugin NAME`. The
contig list in parentheses is omitted for full downloads. Exit codes are the
existing ones: 0 on success, 2 for bad arguments or a rejected request.

## Packaging

- `pyproject.toml`: add `huggingface_hub>=1.0,<3` to the base dependencies;
  update `uv.lock`.
- `bioconda-recipe/meta.yaml`: add `huggingface_hub >=1.0,<3` under `run`.
  The nf-core module container follows once Bioconda rebuilds.

## Tests

`tests/test_download.py`, no network by default. `snapshot_download` and
`HfApi.list_repo_files` are monkeypatched: the fakes record their arguments,
return a file list modelled on the real layouts, and write placeholder files
into `local_dir`.

- Revision: pinned hash by default; explicit `revision` (including `"main"`)
  passed through; unpinned release and unknown plugin give `ValueError` listing
  what is published; dbNSFP gives the redistribution message.
- Patterns: exact `allow_patterns` for the four cases in the table; `chroms`
  accepts `22`, `chr22`, `X`, `GL000008.2`; unknown contig and empty list raise
  before `snapshot_download` is called.
- Destination: `<cache_dir>/116_GRCh38_merged` and
  `<plugin_cache_root>/plugin/clinvar` are created, passed as `local_dir` and
  returned; `~` is expanded.
- Errors: `RepositoryNotFoundError` / `RevisionNotFoundError` → `ValueError`;
  connection error and `LocalEntryNotFoundError` → `RuntimeError`; messages
  contain repository and revision.
- Progress: `show_progress=False` disables the library's bars during the call
  and restores them afterwards, also when the call raises.
- Pin table: every value is a 40-character lowercase hex string; there is an
  entry for each published plugin and cache type of release 116.

`tests/test_cli_download.py`: flag-to-keyword mapping for both subcommands,
required flags, the summary output against a spy, and API errors becoming
exit 2 with one line on stderr.

Network test, skipped unless `VEPYR_NETWORK_TESTS=1`: download the merged
cache for `MT` (the datasets name it `chrMT`) and `plugin_phenotypeorthologous`
for chr22 into `tmp_path`, check the files and the `.cache/` resume metadata,
and open the downloaded core cache with `cache_contig_identity()` to prove the
engine reads a directory that contains `.cache/`. Annotating needs a reference
FASTA, so annotation of downloaded caches is verified on HG002 chromosome 22
outside CI, against the output of caches fetched with `hf download`.

## Documentation

- `docs/downloads.md`: lead with `vepyr download-cache` / `download_cache()`
  and the plugin equivalents, including `--chrom`; keep `hf download` as the
  manual alternative and the WUT mirror as is. Explain the pinned default and
  `revision="main"`.
- `docs/cli.md`: a "Downloading caches" section with both flag tables.
- `docs/api.md`: add both functions to the API reference.

## Delivery

One PR from `feat/download-cache` against `master`.
