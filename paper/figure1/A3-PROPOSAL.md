# A3: selective reads — design proposal

Separate draft; `figure1.svg`, `.drawio`, `.png` and `.pdf` are unchanged.

The visual reference is the repeated Parquet-file views in the [Apache DataFusion pruning diagram](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/). This is an original drawing for vepyr, not a reproduction of that figure. Its stages follow the variation point-lookup reader described in this directory's README, rather than importing all of DataFusion's generic scan optimizations.

## Proposed panel

Five steps replace the schema/defaults-heavy A3:

1. **Load metadata:** schema, file identity and Parquet page indexes.
2. **Resolve pages:** requested positions and PageDir select candidate page row ranges.
3. **Locate positions:** read only `start` pages to find exact position row offsets.
4. **Take payload:** retrieve required physical columns at those row offsets.
5. **Return rows:** Arrow cache rows feed allele matching and the annotation engine.

Four aligned views show the same shard at successive access stages. Pale dashed pages are not accessed in that pass; bold outlines denote candidates; filled pages denote reads, with a white mark for selected rows. Two example row groups contain three representative column chunks each. Payload-page boundaries intentionally differ from key-page boundaries. The final view is a cache-row batch, not the final annotated VCF or the user-facing Arrow output.

The repeated views are schematic, not measurements of bytes avoided. Reading a selected row can still decode an entire page, and the reader can coalesce nearby byte ranges. A projected logical field may require multiple physical columns and matching dependencies. Position matches are not yet allele matches.

Keep warm/cold as a short note: both sorted runs are searched, and tier boundaries need not align with row groups. Move the AF threshold, explicit schema field lists, identity-metadata details and writer defaults to the caption or Methods. Do not add Bloom-filter pruning or a separate generic row-group-pruning stage without evidence that this reader uses them.

## Draft caption for A3

**(A3)** Selective access to a variation chromosome shard. File metadata and page indexes form a directory of genomic-position ranges. Requested positions identify candidate pages; a `start`-only read then resolves exact position row offsets. The reader retrieves the required payload columns at those offsets and returns Arrow cache rows for downstream allele matching and annotation. Successive file views illustrate access states rather than separate files or measured I/O reductions. Both warm and cold sorted runs remain queryable, and tier boundaries need not coincide with row groups. Physical page boundaries can differ between leaf columns; selected-row retrieval still operates through page reads and decoding.

## Files

- `a3-proposal.svg` / `a3-proposal.png`: panel-only preview.
- `figure1-a3-proposal.svg` / `figure1-a3-proposal.png`: proposal in the complete Figure 1 layout.
- `figure1-a3-proposal.drawio`: editable native diagram.
- `build_a3_proposal.py`: generator using the existing figure primitives.

Rebuild the vector files with `python3 -B paper/figure1/build_a3_proposal.py` from the repository root. Raster previews are rendered from those SVG files separately.
