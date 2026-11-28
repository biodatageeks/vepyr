# Figure 1 generator

Panels: A tool comparison, B interactive analysis of HG002 with vepyr and polars-bio, C1–C4
architecture and cache, D query pushdown, E whole-genome performance. The
figure is TikZ, sized to the text width of the Bioinformatics template (488.5 pt).

It is not run on its own: `tools/make_numbers.py` calls `build_tikz.render()` and writes
`figures/figure1-tikz.tex` and `figures/figure1-table.tex`. `latexmkrc` runs that script before
every compile, so a change in `data/` reaches Figure 1 locally and on Overleaf. The code uses the
Python standard library only.

Inputs, all in the paper repository:

| Panel | File |
|---|---|
| A | `panel_a.py` (the table cells; its size is measured with pdflatex into `generated/figure1-panel-a-size.txt`) |
| B | `data/figure1/hg002-tiered-annotation/` (results of the vepyr analysis `paper/hg002-tiered-annotation`) |
| E | `data/wgs_scaling.csv` (also Supplementary Table 18 and the main-text times) |
| C1 logos | `figures/logos-bw/` (read by LaTeX) |

Panel A's table is LaTeX (`\figonetable`), typeset by the paper itself. The layout needs its size:
`panel_a.size()` measures it with pdflatex and the class of `vepyr-main.tex` whenever the table or
the class changes, and otherwise reads the stored size (so no pdflatex is needed for an unchanged
table, e.g. in the Overleaf Script Runner). The panel B files are the `results/` of
`paper/hg002-tiered-annotation`. When those analyses change, copy their output here.

Modules:
- `build_figure1_v2.py` lays out the scene (pixel coordinates). `build_tiered_annotation.py`,
  `build_pushdown.py`, `build_layout_effect.py` and `build_cache_workflow_proposal.py` draw panels.
  `build_figure.py` holds the scene types.
- `build_performance.py` draws panel E as SVG from the CSV.
- `build_tikz.py` writes the scene as TikZ: one node per text line, fonts in points, the panel A
  table as a LaTeX `tabular`, and the panel E chart translated from its SVG.

Some modules also have `main()` functions that render PNG or draw.io previews with headless
Chrome. Those are not used for the paper figure.

Moved from the vepyr repository (branch `paper/three-tier-analysis`, `paper/figure1/`, commit
53e037e) on 2026-11-23. This folder is now the only copy that is maintained.
