# Figure 1

Panels: A tool comparison, B interactive analysis of HG002 with vepyr and
polars-bio, C1–C4 architecture and cache, D query pushdown, E performance.
The figure is TikZ, sized to the text width of the Bioinformatics template
(488.5 pt); draw.io is no longer produced for it.

```bash
python3 ../tool-comparison/build_table.py --pdf  # panel A table
python3 build_tikz.py                            # figure1-tikz.tex, figure1-table.tex
pdflatex figure1-page.tex                        # one journal page: figure + caption
```

- `build_figure1_v2.py` lays out the scene (pixel coordinates);
  `build_tiered_annotation.panel_b()`, `build_pushdown.panel()` and
  `build_cache_workflow_proposal.lookup_column()` draw the new panels.
- `build_tikz.py` writes the scene as TikZ: one node per text line on the
  baseline of the SVG preview, fonts in points, the panel A table as a LaTeX
  `tabular`, the performance chart translated from its SVG.
- `figure1-page.tex` reproduces the page of `oup-authoring-template`
  (`contemporary,large`), sets the caption below the figure and reports the
  space left on the page.
- To use the figure in the paper, `\input{figure1-table.tex}` once and
  `\input{figure1-tikz.tex}` inside the figure; the OUP class already loads
  tikz, xcolor and array, so add `\usetikzlibrary{arrows.meta}`, `colortbl`
  and `booktabs`.
