# Fresh ClinVar chromosome comparison

Candidate engine: `8e279811b10b8ce95de465b4bd69b79c0e4af75f`. Baseline engine: `562af65835f443305784b6567004872966922df0`.

Three fresh-process measurements per engine and configuration after one discarded warm-up; engine order rotates between rounds. Plain VCF output, normalized input, full annotation, the same reference FASTA, native vepyr builds and official vep-rs v0.5.0 on the same macOS arm64 host. Startup is timed; digest/order verification is outside timing. All observations are retained.

| Chromosome | Workers | Previous vepyr s | Optimized vepyr s | vep-rs s | Previous GiB | Optimized GiB | vep-rs GiB | Time vs previous | Time vs vep-rs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 1 | 268.22 | 169.56 | 312.74 | 3.742 | 3.648 | 3.946 | -36.8% | -45.8% |
| 2 | 8 | 130.96 | 48.77 | 47.57 | 4.943 | 4.407 | 3.998 | -62.8% | +2.5% |
| 17 | 1 | 115.73 | 95.69 | 206.14 | 3.136 | 3.193 | 3.769 | -17.3% | -53.6% |
| 17 | 8 | 27.36 | 23.26 | 31.33 | 4.280 | 3.939 | 4.109 | -15.0% | -25.8% |

Values are medians; negative time changes mean less elapsed time. Candidate time and RSS passed the unchanged +10% guard against the paired previous vepyr arm in every case. This table is separate from the traced dense/sparse phase gate and the BGZF whole-input comparison.

## Output verification

Every warm-up and measured vepyr output matches the original pre-edit raw body MD5 and input identity/order. vep-rs reproduces its own stable body digests. Its regulatory and edited RefSeq coverage differs, so these measurements do not establish cross-engine biological parity.

| Chromosome | Records | Original/final vepyr body MD5 | vep-rs body MD5 |
|---|---:|---|---|
| 2 | 401,230 | `4df6ae8cdc714236b95943abd465752d` | `a2d18b2aab44946c47db472f04860d04` |
| 17 | 277,114 | `d2f003d60f8c85a91dbc2247d21d855a` | `d16b8b23cacf2de478e125e967d46b47` |

## Provenance

Candidate extension SHA256: `46f4074b2bd708e107a089c5c1ef56851480bf7c20c219de149271f5c46566ec`.

Exact source hashes, published-pin verification, native build logs, all failed and passing standard-gate attempts, and per-observation commands/load samples remain in the adjacent artifacts. Whole-ClinVar validation uses the existing `clinvar_merged` / `strict-clin-sig` golden policy in addition to raw pre-edit vepyr parity. This report does not claim whole-input completion.

[All observations, including discarded warm-ups](clinvar-final-chromosome-2026-11-07.csv).
