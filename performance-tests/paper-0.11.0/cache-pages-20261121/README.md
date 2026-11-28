# Position-column pages touched by the cache lookup (2026-11-21)

Backs the page-count statement of main-text Figure 1 (panel C3) and Methods: with vepyr's cache
layout, the records that a lookup actually matches lie on 64-72% fewer pages of the position column
than in a plain position-sorted file; the position-column scan itself is not reduced.

## Method

`tool/` is a throwaway Rust program (`pages76`, parquet 58.0.0). Its `PageDir`,
`CoalescingAsyncReader` and `build_probe_starts` are copies of the logic in datafusion-bio-functions
`46d8c20`; the reader additionally records the requested and coalesced byte ranges. `measure` runs
the HG002 chromosome 22 variants (50,861 records, buffers of 5,000) against a variation file and
prints, per buffer, the candidate pages decoded and the pages that hold matched records
(`HIT ... hit_pages`). `rewrite` produces the comparison files.

| File | Variation file |
|---|---|
| `chr22_ext.txt`, `chr22_noext.txt` | vepyr's merged release-116 cache, `variation/chr22.parquet` (two sorted runs) |
| `sorted_ext.txt`, `sorted_noext.txt` | the same rows sorted by position into one run (`rewrite ... sorted`) |
| `identity_ext.txt`, `identity_noext.txt` | the same rows in their original (tier, start) order, rewritten with the same writer settings (control for the rewrite) |
| `orig_ext.txt` | first version of the output, before the per-buffer `HIT` lines were added |

`ext` uses the extended probe starts of the lookup (as vepyr does), `noext` one probe per variant.

## Numbers used in the paper (`*_ext.txt`)

- Pages holding matched records, vepyr layout vs position-sorted file: buffer 0 907 -> 315 (65%);
  full buffers 0-9 range from 64% to 72% fewer (buffer 9: 788 -> 286, 64%); the partial last
  buffer 10 (861 records): 238 -> 55.
- The position-column scan is not reduced: buffer 0 decodes 964 candidate pages with vepyr's layout
  against 907 in the position-sorted file.

An earlier figure of 920 -> 220 pages (the "76% fewer" claim this tool was written to check) counted
one probe per variant rather than the extended probes of the lookup; it was replaced by the numbers
above.

`target/` (build output) is not included; the variant TSV given to the tool is derived from the
HG002 input and is not included either.
