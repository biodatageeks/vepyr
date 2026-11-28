# Measured values: one source for text, tables and Figure 1

Every measured value is entered once, in a file here. `tools/make_numbers.py` turns them into
- `generated/data.tex`: every value, already formatted, used as `\val{key}`. Both documents load
  it, the main text as `generated/data.tex` and the supplement as `../generated/data.tex`;
- `figures/figure1-tikz.tex` and `figures/figure1-table.tex`: Figure 1, drawn by `tools/figure1/`.

Text and tables are written by hand and take each value from `data.tex`, for example:

```latex
vepyr annotated the genome in \val{wgs.macos.1.vepyr_s}\,s          % 190.3\,s
1 & no fork & \val{wgs.macos.1.vep_s} & 1 worker & \val{wgs.macos.1.vepyr_s} & \val{wgs.macos.1.speedup}$\times$ \\
```

The values are already formatted: times in seconds (vepyr with 1 decimal, VEP whole seconds), thousands with commas, speed-ups as whole numbers.
Every value is rounded half up from the unrounded data (sums are exact), and derived values such as
speed-ups are calculated from unrounded data, never typed.
The keys are listed in `generated/data.tex`. An unknown key prints `??key??` in bold and a warning
in the log.

| File | Keys / output | Used in |
|---|---|---|
| `wgs_scaling.csv` (whole-genome VEP and vepyr times, merged cache, 1/2/4/8 processes) | `wgs.<macos\|linux>.<1\|2\|4\|8>.{vep_s,vepyr_s,speedup,peak}`, `wgs.<platform>.peaks` | Supplementary Table 18 and its caption, main-text Results, Figure 1E |
| `figure1/hg002-tiered-annotation/` (trio analysis results of `python analysis.py all`: annotation and queries in one Python session, vepyr 0.11.0, MacBook Air M2 24 GB on mains power; `session.json` has the stage timings, the measured session total (`trio.total_s`) and the machine, which must match the text; `trio.files_s` sums the read times of all 12 remote files, the parental VCFs included) | Figure 1B; `trio.{annotate_s,queries_s,total_s,url_reads_s,files,files_s,files_rows_million,workers,vepyr_version,clingen_genes,clingen_genes_lt90,plp_gene,plp_hgvsp,plp_af_pct,plp_asj_pct,plof,hc,hc_genes,hc_rare,hc_rare_resolved}`, `trio.loeuf.{plof_pct,syn_pct,plof_p}` | Figure 1B and caption, main text, abstract, Supplementary Section 5.7 (the three tiers; its figure, `supplementary/figures/figS_hg002_summary.pdf`, is drawn from the same results by `paper/hg002-tiered-annotation/figures.py` in the vepyr repository) |
| `raw/vepyr_clean_host_20261123_engines.csv` (vepyr 0.11.0, HG002, merged cache, whole VCF, macOS: 3 runs per worker count) | medians fill the macOS vepyr columns of `wgs_scaling.csv` and the HG002 merged whole-VCF vepyr row of the engine table | Supplementary Tables 18 and 36, Figure 1A and 1E, abstract, main text |
| `raw/engines_two_modes_20261120_runs.csv` (raw run log of the engine comparison: one row per run, chromosome and worker count; macOS, 2026-11-20/21) | `engines.<hg002\|clinvar>.<merged\|ensembl>.<perchrom\|whole>.<vepyr\|veprs\|fastvep>.{wall1,wall2,wall4,wall8,cpu1,cpu8,scaling8,peak8,peak8_r1}`, summed and derived by the script | Supplementary Table 36, Figure 1A, abstract |
| `raw/polars_chr22_v0110_{pushdown,vep_matrix,e2e,e2e_sliced,filter_only}.csv` (Polars integration runs on HG002 chr22, vepyr 0.11.0, macOS; medians of three runs at full precision, written by the row functions of vepyr `performance-tests/polars-integration/scripts/make_supplement.py` from the result bundles in `performance-tests/polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110/`; the published CSVs there round to 4 decimals) | `pushdown.*`, `vepmatrix.*`, `e2e.*`, `e2esum.*`, `sliced.*`, `filteronly.*`, `e2e.text.*` | Supplementary Tables 20-29, main text |
| `raw/engine_concordance_{merged,ensembl}_{totals.csv,mismatches.json}` (comparison of vepyr, vep-rs and fastVEP with VEP 116.0 on HG002 and ClinVar; from `~/research/benchmarks/merged-engine-comparison-20261107/all-chromosomes-w8` and `ensembl-engine-comparison-20261108/hg002-w8`) | `conc.<hg002\|clinvar>.<merged\|ensembl>.<vepyr\|veprs\|fastvep>.{expected,returned,md5,allmatch,na,withcsq,mean,compared,missing,extra,unequal,pct1,pct0}`, `mism.<...>.r<rank>.{field,rows}` | Supplementary Tables 33-35, Figure 1A, main text |
| `raw/fields_filled_hg002/<engine>-<cache>-HG002/<chromosome>.json` (CSQ fields declared and filled in the whole-genome HG002 outputs; from `~/research/benchmarks/engines-two-modes-20261120/fields-filled/whole`) | `fields.<ensembl\|merged>.vep.{declared,filled}`, `fields.<cache>.<vepyr\|veprs\|fastvep>.{filled,notdeclared,empty}` | Supplementary Table 37, Figure 1A and caption |
| `raw/{interval_routes_chr22,writer_memory_chr1,workers_chr22,workers_chr1}_v0110.json` (vepyr 0.11.0 supplement runs, macOS; from vepyr `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121` and the chr22 Polars run's `workers_chr*`) | `interval.*`, `writer.*.rss`, `workers.<chr>.<cache>.w<n>`, `workers.<chr>.rows` | interval-overlap table, writer memory (Section 8.4), Supplementary Table 19 |
| `raw/cache_tiering_wgs/` (warm/cold cache layout on the whole HG002 genome: times, pages per lookup buffer, pages decoded; vepyr `performance-tests/cache-tiering`, results 240e030) | Figure 1C3 and C4 (via `tools/figure1/tiering_data.py`), `tiering.*` | Figure 1C3, C4, main text |
| `raw/wgs_parity_v0110.csv` (one row per parity report: profile, chromosome, records, strict MD5 match, vepyr version; extracted from vepyr `performance-tests/paper-0.11.0/vepyr-0110-supplement-20261121/parity`) | `parity.<set>.{records,matching,version}`, `parity.pick.*` | Supplementary Tables 30 and 32 |
| (several raw files) | `input.<hg002\|clinvar>.{records,records_million}`: one count per input; the engine runs, concordance totals, parity reports and trio summary must agree, or the script stops | abstract, main text, Figure 1B caption, Supplementary Sections 7-10 and Table 30 |
| `measurements.csv` (single measured values, one per row, with their source) | `<key>`, and `<key>.r0`, `.r1`, `.r2` rounded | Figure 1A (VEP peak memory), test counts (Table 31, main text), interval VEP row |

Each row of `wgs_scaling.csv` names the raw results it comes from (`*_source`, paths in the vepyr
repository). `tools/figure1/README.md` says where the `figure1/` files come from.

## When the script runs

- **On every compile.** `latexmkrc` (main text) and `supplementary/latexmkrc` run it before LaTeX,
  locally and on Overleaf. A change in `data/` therefore reaches the text, the tables and
  Figure 1 in the next PDF. On Overleaf, files written during a compile are used for that PDF only
  and are not saved to the project.
- **To update the committed files.** Run it locally (`python3 tools/make_numbers.py`) or on
  Overleaf with the Python Script Runner: join the experiment once at
  <https://www.overleaf.com/labs/participate>, open `tools/make_numbers.py` in the editor and press
  **Run**. Files written there are saved to the project and reach git with the next sync. The
  output lists each file as `wrote` or `unchanged`, and a `WARNING` line for anything to fix.

The script and `tools/figure1/` use the Python standard library only.

## How to change a number

1. Edit the CSV and compile.
2. Before committing, run `python3 tools/make_numbers.py` (or press Run on Overleaf), then
   `python3 tools/make_numbers.py --check`. The check fails if
   - `generated/data.tex` or Figure 1 is out of date;
   - a `\val{key}` in any `.tex` file has no value;
   - a table that uses `\val` still has a number typed by hand (a time such as 3:15, a decimal, or
     a speed-up such as 97$\times$).
3. Commit the data and the generated files together.

## Adding a value or a table

Add the value to a CSV (or a new CSV) and a few lines to `tools/make_numbers.py` that format it
under a new key. Then write `\val{new.key}` wherever it is used, in text or in a table cell. Tables
stay ordinary LaTeX in the documents. Do not edit the generated files by hand.

## One value, one place

A value is entered once: hand-entered values only in `measurements.csv` and `wgs_scaling.csv`
(VEP times and the Linux vepyr runs), everything else in `raw/`. Values that several raw files
report (the record counts of the inputs) get one key and a consistency check. The vepyr runs for
HG002 with the merged cache on the whole VCF were removed from
`raw/engines_two_modes_20261120_runs.csv`: they are replaced by `raw/vepyr_clean_host_20261123_engines.csv`
(the original log stays in the benchmark folder). `wgs_scaling.csv` has no columns that follow from
others: the vepyr workers equal the processes, and VEP's `--fork` is processes minus one.
