# R4 ClinVar engine pin

Tracking issue: biodatageeks/vepyr#240. Base: vepyr master `c9a1ab86d99ad296f7c9399dcab49d24506491af` (#263).

The functions integration is PR #304 at `1fc0597aa7885b90170907ae4e0e8ea9a5349203`. It carries the R4C stack (#242, #236, #155), R4B (#223, #227, #244), and R4A (#202, #208), plus Ensembl-source-backed conflict resolutions. This branch pins that functions revision and updates Cargo.lock. Formats stay at `6af1fa1480b2fc527e3b218585eae3a087f2aca9`; the release remains postponed.

## Round gates

- `cargo test --no-default-features --features mimalloc` and `uv run pytest`.
- Release builds for the R4 head and vepyr master `c9a1ab8`, using native CPU flags.
- Strict HG002 merged-profile md5 for 22 contigs.
- Unfiltered ClinVar full-ledger comparison over all 9,249 records and 25 contigs against the R3 baseline, plus the R4A/R4B/R4C identical-record union check and the named chr1/chr10 records.
- `compare_runs.py` against vepyr master at 1 and 8 workers, with discarded warm-ups and ABBA/BAAB pairs.

No formats or schema change is planned. The PR stays draft; `/parity-wgs` remains pending for the coordinator and is never run in this round task.
