Re-runs of the supplement's vepyr measurements with the vepyr 0.11.0 wheel from PyPI (2026-11-21)
Environment: ~/workspace/venvs/vepyr-0.11.0 (vepyr 0.11.0, polars 1.44.2, polars-bio 0.38.0, Python 3.12.2).
The user decided to keep Polars 1.44.2, the newest the 0.11.0 wheel accepts (it declares polars<2).

wgs/    whole genome, merged cache, 8/4/2/1 workers after one discarded 8-worker run (run_wgs.zsh). While these
        runs were measured I ran light commands (git fetch/push, gh, small Python edits), so this set is NOT used.
wgs2/   the same again with nothing else running (run_wgs2.zsh): the set reported in the supplement.
sweep   worker sweep (Section 8.3): outputs in the vepyr worktree .worktrees/polars-0110,
        performance-tests/polars-integration/outputs/116/macos_Mareks-MacBook-Pro_chr22_merged_v0110/workers_chr{22,1}
writer/ writer memory (Section 8.4): writer_memory.py, writer_once.py; v0110.json is the measurement. v0100.json,
        which reproduced the published 0.10.0 figures as a check of the method, is on the branch archive-pre-0.11.0.
polars  Section 8.5: run.sh in the run directory above (stages prepare, equiv, pushdown, e2e, tables); VEP side
        reused from ..._v0100 because Docker was not available.
chain.zsh runs everything in this order (console logs not included).
