# Figure 1 and the measured values of the vepyr paper

This folder is a copy of the generator that produces Figure 1 of the vepyr paper and every
measured value printed in its main text and supplement. One script reads the files in `data/`
and writes the figure as TikZ and the values as LaTeX macros.

The generator is maintained in the paper repository. This copy was taken from its commit
`0547a25` (2026-11-28). The folder keeps the paper repository's relative layout, so the scripts
run here as they are.

## Layout

| Path | What |
|---|---|
| `tools/make_numbers.py` | the entry point |
| `tools/figure1/` | the modules that draw Figure 1 (13 Python files; `README.md` describes them) |
| `data/` | the inputs: every measured value, entered once (`data/README.md` lists each file, the keys it gives and where the paper uses them) |
| `generated/data.tex` | output: 1,574 values, already formatted, used in the paper as `\val{key}` |
| `figures/figure1-tikz.tex` | output: Figure 1 as TikZ |
| `figures/figure1-table.tex` | output: the table of panel A (`\figonetable`) |
| `generated/figure1-panel-a-size.txt` | output: the typeset size of the panel A table, which the layout needs |
| `vepyr-main.tex` | a stub with the manuscript's `\documentclass` line, not the manuscript (see "Size of the panel A table") |

## Running it

```bash
cd paper/figure1
python3 tools/make_numbers.py          # regenerate the outputs
python3 tools/make_numbers.py --check  # exit status 1 if an output is out of date or a value is missing
```

Only the Python standard library is used (run here with Python 3.12). With the files as committed,
the first command reports all four outputs as `unchanged` and `--check` exits with status 0.
After `figures/figure1-tikz.tex`, `figures/figure1-table.tex` and `generated/data.tex` are
deleted, the first command writes them again, byte for byte the files of the paper repository at
`0547a25`.

To change a value, edit the file in `data/` and run the script (`data/README.md`, "How to change
a number").

## Where the inputs come from

`data/README.md` gives the origin of every file. In short:

- `data/wgs_scaling.csv`: whole-genome times of Ensembl VEP and vepyr; each row names its
  sources (run directories under `performance-tests/` of this repository, or a file in
  `data/raw/`).
- `data/raw/vepyr_clean_host_20261123_engines.csv`: vepyr 0.11.0 on the whole HG002 genome on
  macOS, three runs per worker count.
- `data/raw/polars_chr22_v0110_*.csv`, `data/raw/*_v0110.json`, `data/raw/wgs_parity_v0110.csv`:
  from the vepyr 0.11.0 results under `performance-tests/polars-integration/outputs/116/` and
  `performance-tests/paper-0.11.0/`.
- `data/raw/cache_tiering_wgs/`: the three files are identical to those in
  `performance-tests/paper-0.11.0/cache-tiering-20261122/wgs/`.
- `data/raw/engines_two_modes_20261120_runs.csv`, `data/raw/engine_concordance_*`,
  `data/raw/fields_filled_hg002/`: the comparison of vepyr, vep-rs and fastVEP with Ensembl
  VEP 116.0 (run times, concordance, CSQ fields filled).
- `data/figure1/hg002-tiered-annotation/`: results of the HG002 trio analysis
  `paper/hg002-tiered-annotation` (pull request #295).
- `data/measurements.csv`: single measured values, each with its source.

## Differences from the paper repository

- `generated/figure1-panel-a-size.txt` is the file the generator writes from these inputs. The
  file committed at `0547a25` carries the key of an earlier `data.tex`; width and height are the
  same.
- `vepyr-main.tex` is a stub.
- The logos of panel C1 (`figures/logos-bw/*.png`, 11 files) are not copied: they are third-party
  trademarks and stay in the paper repository. Generating the outputs does not read them.
- The Python files went through this repository's pre-commit hooks (`ruff check --fix`,
  `ruff format`), which changed their formatting only, and the docstring of
  `tools/figure1/build_analytical_workflow.py` says "the paper repository" where the original
  gives its address. The outputs are the same.

## Size of the panel A table

The layout of the figure needs the size of the panel A table as typeset by LaTeX.
`tools/figure1/panel_a.py` stores it in `generated/figure1-panel-a-size.txt` under a key made of
the `\documentclass` line of `vepyr-main.tex`, the table and `data.tex`.

- While the key matches, no LaTeX is needed.
- After a change, the table is measured again with `pdflatex` and the class
  `oup-authoring-template`. The class is found if it is installed with TeX (as in TeX Live) or
  placed in this folder.
- Without `pdflatex`, the script prints a warning and uses the stored size.

## Compiling the figure

The two `.tex` files are meant to be read by the manuscript, which is not part of this
repository. Its preamble has `\input{generated/data.tex}`, `\usetikzlibrary{arrows.meta}`,
`\usepackage{colortbl}`, `\usepackage{booktabs}` and `\input{figures/figure1-table.tex}`; the
figure environment has `\input{figures/figure1-tikz.tex}`. The figure is sized to the text width
of the Bioinformatics template (488.5 pt). Compiling it therefore needs the manuscript's document
class and the logos, which `figure1-tikz.tex` reads from `\figonelogos` (by default
`figures/logos-bw`). It was not compiled in this folder.

Some modules in `tools/figure1/` also have `main()` functions that render SVG, draw.io or PNG
previews. They are not used for the paper figure, and those that embed the logos need the logo
files.
