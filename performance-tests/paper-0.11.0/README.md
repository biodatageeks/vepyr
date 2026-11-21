# Raw results behind the vepyr paper's 0.11.0 numbers

Index from each table and figure of the vepyr paper (main text and supplement) to the raw results it
is computed from. Paths are relative to the repository root. Sets added together with this index
are marked "this PR"; the others were already on master.

Only small files are kept (CSV, TSV, JSON, text, logs, scripts). VCF, Parquet, FASTA and other
outputs or inputs, build trees and virtual environments are left out; per-contig MD5 checksums of the
record lines stand in for the VCF outputs.

The repository's pre-commit hooks (ruff check --fix, ruff format) reformatted the copied Python
scripts when they were committed: imports split one per line, statements split at semicolons, long
lines wrapped. Their behaviour is unchanged (same AST, or only those import and statement splits).

| Paper item | Raw results | Where |
|---|---|---|
| Supplement Table 17, macOS rows; main Figure 1E, macOS curve (whole genome, merged cache, 8/4/2/1 workers) | `performance-tests/vepyr/outputs/116/macos_mwiewior_wgs_vepyr_0.11.0_20261121/` | this PR |
| Same, harness logs of that run (memory log, run log, the unused first set) | `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/wgs2/` (reported), `wgs/` (first set, **not used**: other light work ran meanwhile) | this PR |
| Supplement Table 17, Linux rows (merged cache only); main Figure 1E, Linux curve | `performance-tests/vepyr/outputs/116/linux_vepyr_0.9.0_20261002/vepyr_merged_worker_scaling/` (vepyr **0.9.0**) | master |
| Ensembl VEP reference times, macOS (Table 17, Figure 1E) | `performance-tests/vep/outputs/116/macos_mwiewior_wgs_20260914/` | master |
| Ensembl VEP fork scaling, Linux (Table 17 Linux rows, Figure 1E) | `performance-tests/vep/outputs/116/merged_fork_scaling/` | master |
| Supplement Table 12 (interval routes) | `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/interval/` (`v0110.json`; `v0100.json` is the same measurement with vepyr 0.10.0; `time_routes.py`, `route1.py`) | this PR |
| Supplement Section 8.3, Table 18 (LazyFrame worker sweep, chr22 and chr1) | `performance-tests/polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110/workers_chr22/`, `workers_chr1/` | this PR |
| Supplement Section 8.4 (writer memory) | `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/writer/` (`v0110.json`; `v0100.json` reproduces the 0.10.0 values), `writer_memory.py`, `writer_once.py` | this PR |
| Supplement Section 8.5, Tables 19-28, Figures 4, 6, 7; main Figure 1D | `performance-tests/polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110/` (A, B, B_sliced, C, csv, supplement); harness on branch `perf/polars-integration-harness` at `e5947c8` | this PR |
| Supplement Table 31 (golden parity per profile and chromosome); main-text concordance with VEP | `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/parity/` (per-profile reports, per-contig source metadata, logs; `pick/` for the pick-mode profiles) | this PR |
| Supplement Tables 32-36 (vepyr 0.11.0, vep-rs 0.5.0, fastVEP 0.6.1; per chromosome and whole VCF); main Figure 1A wall time, memory, records as in VEP | `performance-tests/paper-0.11.0/engines-two-modes-20261120/` (`runs.csv`, `whole-md5/`, `validate*`, `recheck-*`, `remeasure.txt`, `scripts/`) | this PR |
| Main Figure 1A, "Fields filled" row; supplement Section 10 fields paragraph | `performance-tests/paper-0.11.0/engines-two-modes-20261120/fields-filled/` | this PR |
| Main Figure 1A, VEP peak memory (3.4 GiB summed RSS, 1.9 GiB summed PSS) | `performance-tests/paper-0.11.0/vep-peakmem-fork7-20261121/` | this PR |
| Main Figure 1, panel C3, and Methods: matched records on 64-72% fewer position-column pages | `performance-tests/paper-0.11.0/cache-pages-20261121/` | this PR |

## Directories of this PR

- `vepyr-0110-supplement-20261121/`: re-runs of the supplement's vepyr measurements with the
  vepyr 0.11.0 PyPI wheel (README.txt). `run_wgs2.zsh` produced `wgs2/` (reported), `run_wgs.zsh`
  produced `wgs/` (not used). `chain.zsh` and `chain2.zsh` ran the stages in order; the results of
  the Polars stages and the worker sweep are in the polars-integration run directory.
  `parity/env.txt` records the vepyr build used.
- `engines-two-modes-20261120/`: engine comparison in two modes (per chromosome, whole VCF).
  `runs.csv` is the reported table; `runs.before-memfix.csv`, `runs.before-remeasure.csv`,
  `sentinel.before-memfix.csv`, `remeasure.obsolete.txt` and `whole-md5.before-remeasure/` are earlier
  states kept for the record (`remeasure.txt` explains each re-measurement and the user's decisions).
  `memlog.csv` tracks free memory before each run. `logs/*-build.json` record how the vep-rs and
  fastVEP binaries were built; `whole/*/prepared.json` and `cache-veprs/info.json` describe the
  whole-VCF inputs and engine caches.
- `vep-peakmem-fork7-20261121/`: README.md.
- `cache-pages-20261121/`: README.md.

## Open items

- The Linux rows of supplement Table 17 (merged cache only since the RefSeq rows were removed) and
  the Linux curve of Figure 1E are still vepyr 0.9.0; a re-run with 0.11.0 is pending.
- The trio analysis of main Figure 1B (4.2 min + 1.3 min) has no raw results located yet.

Console logs, stdout and stderr of the runs are not included; only results, the scripts that
produced them and their metadata.

## JSON bundles

To keep the pull request small, the per-run JSON results of these sets are merged into one JSON
file each. A bundle has `bundle_of` (the original directory), `note` and `files` (path inside that
directory -> the original JSON content). Other files of these directories (GNU time logs, CSV, TSV)
stay where they were.

- `vepyr-0110-supplement-20261121/`: `wgs.json`, `wgs2.json`, `parity/{ensembl,merged,merged_plugins,merged_phenotypeorthologous,refseq}.json`
  (per-chromosome reports and input source metadata of each profile), `parity/pick.json` (all
  transcript-selection profiles, chromosomes 1 and 22)
- `engines-two-modes-20261120/`: `whole-md5.json`, `whole-md5.before-remeasure.json`, `validate-md5.json`,
  `fields-filled/whole/<engine>-<cache>-<dataset>.json` (per-chromosome field scans)
- `../polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110/`: `A.json`, `B.json`,
  `B_sliced.json`, `C.json`, `vep.json`

Restore the original files with `python performance-tests/paper-0.11.0/unpack_json_bundles.py BUNDLE.json`
(add `--out DIR` to write elsewhere); every bundle was checked to restore each file with identical
content.
