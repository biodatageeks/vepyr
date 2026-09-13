# A3 — frequency-aware cache lookup

Separate monochrome proposal based on Marek's `image-panelC.png`. The standalone
panel and its placement in Figure 1 both use **A3**; panel C remains Performance.
The accepted Figure 1 and the earlier five-step A3 proposal are not replaced.

## Proposed main-panel message

Grouping common positions makes lookup hits less scattered, reducing the number
of position pages read while retaining access to both warm and cold tiers.

The main panel retains three elements:

- A short input → retrieval / allele matching → known-variant annotation path.
- Two schematic layouts with the same six common and two rare hit symbols.
  Black / open symbols distinguish common / rare hits; heavy outlines identify
  pages read. These eight symbols illustrate the mechanism, not measured ratios.
- A zero-based linear bar chart with the reported counts from Marek's image:
  920 versus 220 pages. The displayed 76% reduction is calculated as
  `100 * (1 - 220 / 920) = 76.09%`.

## Draft manuscript caption

**(A3) Frequency-aware lookup in the variation cache.** Grouping records by
frequency tier and then genomic position concentrates common-position hits
within a compact warm region; both warm and cold regions remain queryable.
Filled and open symbols denote common and rare lookup hits, respectively, and
heavy outlines identify accessed pages. Both layouts depict the same
illustrative hits; tier widths and page counts in the schematic are not to
scale. In the reported example of one 5,000-record HG002 chromosome-22 buffer
using the GRCh38 merged VEP 116 cache, the position-only layout requires 920
position pages compared with 220 for frequency grouping (76% fewer), for the
same 6,178 position matches. The position-only layout is the counterfactual
baseline in the source illustration; counts refer to the locate step.
Retrieved records undergo allele matching before their annotations are attached
to the input variants. Warm and cold denote on-disk frequency tiers within a
single shard, not separate storage devices or operating-system cache states.

## Interpretation and provenance

The counts are transcribed from `paper/figure1/image-panelC.png`; this proposal
does not introduce a new benchmark or independently verify the original
measurement. It does not present the page-count ratio as a runtime speedup,
an I/O-call count, a reduction in bytes, or the cost of reading payload columns.
The source explicitly calls the position-only layout counterfactual, so its
920-page count is not presented here as a separately timed annotation run.
The 5,000 input records and 6,178 position matches have different meanings:
one input record can require multiple probe positions and a position can yield
multiple cached rows before allele matching.

Warm membership is assigned by position: maximum global allele frequency
at least 1%, with the one-base neighbourhood described in the cache design.
Other alleles at those positions can also enter the warm tier. Therefore a rare
allele is not necessarily stored in cold. The drawing shows one possible case;
it does not assert a one-to-one correspondence between tier and allele rarity.
Each tier is position-sorted, and the tier seam need not align with a page or
row-group boundary. The schematic deliberately puts the seam inside a page.
Known-variant annotations here describe this lookup's output, not the complete
transcript consequence / HGVS annotation pipeline.

## Suggested supplementary figure

Use Marek's detailed example as the basis of a separate supplementary figure:

1. Requested positions and page-directory resolution.
2. A position-only read that locates exact file-row offsets.
3. Retrieval of required payload columns for those offsets.
4. Allele matching and assignment back to the original input records.

Keep one explicit same-position / different-allele example and one unmatched
variant. Put cursor bookkeeping, neighbouring-page coalescing, writer defaults,
the frequency-threshold details and full measurement table in its caption or
Methods. Check physical writer settings against the actual measured cache;
Marek's image and this branch's documented defaults refer to different page-row
settings (1,024 versus 512), so this proposal deliberately quotes neither.

The 5.4% share of cache rows and ~96% share of lookup hits can be shown there as
two separate normalized bars with explicit denominators. Avoid mixing those
percentages with the common/rare allele symbols in the main schematic.

## Files

- `a3-cache-simplified-proposal.svg`, `.png`, `.drawio`: standalone A3.
- `figure1-cache-simplified-proposal.svg`, `.png`, `.drawio`: same panel placed
  in the current Figure 1, with its other panels retained.
- `build_cache_simplified_proposal.py`: generator using the existing
  `build_figure.py` drawing primitives. Both the schematic and the chart are
  native editable Drawio cells; SVG text remains selectable.

When saved next to the existing Figure 1 builders, regenerate with:

```bash
python3 -B paper/figure1/build_cache_simplified_proposal.py --render
```

Chrome/Chromium is needed only for PNG previews. Geometry and XML validation
are run during generation / review; no source-code or benchmark changes are
needed for this proposal.
