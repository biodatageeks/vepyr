# External Integrations

**Analysis Date:** 2026-03-22

## APIs & External Services

**Ensembl FTP:**
- Ensembl public FTP/HTTPS endpoints - source for downloading VEP cache tarballs in `src/vepyr/__init__.py`
  - Integration method: raw `http.client.HTTPSConnection` requests with redirect handling in `_download_with_progress()`
  - Auth: none
  - Endpoints used: `https://ftp.ensembl.org/pub/release-{release}/variation/indexed_vep_cache/...` and fallback `.../variation/vep/...`

**Notebook display integrations:**
- IPython display APIs - optional notebook status rendering in `build_cache()` inside `src/vepyr/__init__.py`
  - Integration method: conditional import of `IPython.get_ipython` and `IPython.display`
  - Auth: none

## Data Storage

**Datasets on local disk:**
- Ensembl VEP cache directories - primary input source for conversion and annotation
  - Connection: direct filesystem paths passed into `build_cache()` and Rust `convert_entity()`
  - Client: `datafusion_bio_format_ensembl_cache::EnsemblCacheTableProvider` in `src/convert.rs`
- VCF files - annotation input
  - Connection: direct path passed to `annotate()` / `create_annotator()`
  - Client: `datafusion_bio_format_vcf::table_provider::VcfTableProvider` in `src/annotate.rs`
- Parquet outputs - generated cache target
  - Writer: Arrow Parquet writer configured in `src/convert.rs`

**Caching:**
- No network/service cache layer is configured
- Annotation/cache reuse is file-based through the generated parquet directory tree

## Authentication & Identity

**Auth Provider:**
- None. The library is a local data-processing package with no user/session model.

## Monitoring & Observability

**Logs:**
- Python `logging` logger named after `vepyr` in `src/vepyr/__init__.py`
- Rust-side progress uses `eprintln!` in `src/convert.rs`
- No external error tracker or metrics sink is configured in tracked files

## CI/CD & Deployment

**Hosting:**
- None detected. This repository builds a library/package rather than a deployed service.

**CI Pipeline:**
- No `.github/workflows/` or alternative CI configuration was found in the repository root

## Environment Configuration

**Development:**
- No mandatory secrets or environment files detected
- Large local reference datasets are expected outside the repo, especially for `tests/data/golden/prepare.py`
- Optional fixture-prep overrides come from environment variables rather than config files

**Production:**
- Consumers must provide filesystem paths to VCF/cache/reference data
- Performance-sensitive installs may use the README’s native CPU build flag: `RUSTFLAGS="-C target-cpu=native"`

## Webhooks & Callbacks

**Incoming:**
- None detected

**Outgoing:**
- HTTPS GET requests to Ensembl FTP mirror during cache download in `_download_cache()`
  - Trigger: `build_cache()` when `local_cache` is not provided and the cache tarball is missing
  - Retry logic: tries multiple URL patterns; does not implement backoff/retry beyond HTTP redirect handling

---

*Integration audit: 2026-03-22*
*Update when adding/removing external services*
