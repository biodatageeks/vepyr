# Ensembl VEP caches

vepyr annotates against an **Ensembl VEP cache** converted into a
point-lookup-optimized, per-chromosome Parquet layout by
[`vepyr.build_cache`](api.md). This page describes the cache types, the supported
Ensembl releases, the entities a converted cache contains, and their on-disk
sizes.

## Cache types

VEP ships three flavours of the GRCh38 cache, differing only in which transcript
set they carry:

| Type | Transcripts | `build_cache(cache_type=…)` |
|---|---|---|
| **ensembl** | Ensembl/GENCODE transcripts | `ensembl` |
| **refseq** | RefSeq transcripts | `refseq` |
| **merged** | Ensembl **and** RefSeq (both transcript sets) | `merged` |

The variation, regulatory, and motif data are the same across types; only the
transcript/exon/translation entities differ (merged is the union, so it is the
largest).

## Supported releases

- **Release 115** (GRCh38) — the golden reference VCFs in this project are
  produced with the `ensemblorg/ensembl-vep:release_115.2` docker image.
- **Release 116** (GRCh38) — exact VEP 116.0 semantics.

Both are built the same way; a converted cache directory is named
`<release>_<assembly>_<type>`, e.g. `115_GRCh38_merged`, `116_GRCh38_merged`.

### Strict Parquet identity

The current vepyr release supports cache 115 with VEP 115.2 semantics and cache
116 with VEP 116.0 semantics. `build_cache()` derives the raw cache release,
checks it against the requested release, and embeds `bio.vep.cache_version` in
the Arrow schema metadata of every generated Parquet shard. There is no
generated-cache version sidecar.

Before annotating a contig, vepyr checks only that contig's
manifest-referenced shards. Every participating entity must declare the same
supported release and source type. The first contig establishes the invocation
identity; a later contig must agree before any of its rows are annotated. A
chr1-only annotation therefore does not open chr2 Parquet footers.

Missing, malformed, mixed, and unsupported releases are errors. The generated
cache directory name is not a fallback, and `expected_cache_version` can only
assert the independently detected metadata—it cannot label an old
metadata-less cache. Such caches must be rebuilt.

## Layout & entities

`build_cache` downloads the Ensembl cache tarball and converts each **entity**
into a directory of per-chromosome Parquet shards:

```
<cache_dir>/<release>_<assembly>_<type>/
  variation/chr1.parquet … chrY.parquet   (+ non-standard contigs)
  transcript/chr1.parquet …
  exon/…  translation_core/…  translation_sift/…  regulatory/…  motif/…
```

The entities and what they feed in the CSQ output:

| Entity | Feeds | Notes |
|---|---|---|
| **variation** | `Existing_variation`, co-located allele frequencies (1000G, gnomAD exomes/genomes), `MAX_AF*`, `CLIN_SIG`, `SOMATIC`, `PHENO`, `PUBMED` | By far the largest entity; also carries the warm/cold `tier` that plugin caches inherit. |
| **transcript** | `Gene`, `Feature`, `SYMBOL`, `BIOTYPE`, `STRAND`, `CANONICAL`, `MANE*`, `TSL`, `APPRIS`, `CCDS`, source/xref fields | The transcript models; differs by cache type. |
| **exon** | `EXON`/`INTRON` numbering, `cDNA_position`, `CDS_position` | Exon/intron structure. |
| **translation_core** | `Protein_position`, `Amino_acids`, `Codons`, `HGVSp`, `ENSP` | Protein translations. |
| **translation_sift** | `SIFT`, `PolyPhen` | Precomputed missense predictions; the second-largest entity (per-protein blobs). |
| **regulatory** | regulatory-region consequences | Regulatory features. |
| **motif** | TF-binding / motif consequences (`MOTIF_NAME`, `MOTIF_POS`, `HIGH_INF_POS`, `MOTIF_SCORE_CHANGE`, `TRANSCRIPTION_FACTORS`) | Present from release 116; empty in the release-115 caches. |

(The Ensembl `translation` entity is split into two Parquet entities on
conversion: `translation_core` and `translation_sift`.)

## Cache format & lookup internals

--8<-- "includes/cache-internals.md"

## Sizing

Measured on-disk sizes of the converted Parquet caches in
`/Users/mwiewior/workspace/data_vepyr` (GRCh38):

| Cache | Total | variation | translation_sift | transcript | translation_core | exon | regulatory | motif |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| `115_GRCh38_ensembl` | **29 G** | 26 G | 2.7 G | 533 M | 196 M | 178 M | 71 M | — |
| `115_GRCh38_refseq`  | **29 G** | 26 G | 2.8 G | 295 M | 156 M | 73 M  | 71 M | — |
| `115_GRCh38_merged`  | **32 G** | 26 G | 5.5 G | 720 M | 348 M | 240 M | 71 M | — |
| `116_GRCh38_ensembl` | **32 G** | 27 G | 4.1 G | 633 M | 263 M | 211 M | 71 M | 89 M |
| `116_GRCh38_refseq`  | **31 G** | 27 G | 2.9 G | 296 M | 156 M | 73 M | 71 M | 89 M |
| `116_GRCh38_merged`  | **36 G** | 27 G | 7.0 G | 818 M | 418 M | 272 M | 71 M | 89 M |

Observations:

- **`variation` dominates** every cache (~26–27 G, ~80–90% of the total) — it is
  the full set of known variants with population frequencies.
- **`translation_sift` is the clear #2** and scales with the transcript set:
  merged (both transcript sets) is ~2× the single-set caches, and 116 is larger
  than 115.
- **`merged` ≈ `ensembl` + `refseq`** for the transcript/exon/translation
  entities (it carries both), while sharing the same variation/regulatory data.
- **`motif`** is populated only from release **116** (empty in the 115 caches).

## Building a cache

```python
import vepyr

vepyr.build_cache(
    release=116,
    cache_dir="/Users/mwiewior/workspace/data_vepyr",
    cache_type="merged",          # "ensembl" | "refseq" | "merged"
    assembly="GRCh38",
    partitions=8,                 # conversion parallelism
)
# → /Users/mwiewior/workspace/data_vepyr/116_GRCh38_merged/<entity>/chr*.parquet
```

See the [API reference](api.md#vepyr.build_cache) for the full signature. Plugin
caches (e.g. AlphaMissense) are built separately and layered on top — see
[Plugins](plugins.md).
