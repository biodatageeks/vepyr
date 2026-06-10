# Parallel benchmark comparison

- baseline: `benchmark-results/chr22-indexed-parquet-20260608/parallelism-full-matrix-adaptive-prefetch-wave/summary.json`
- candidate: `benchmark-results/chr22-indexed-parquet-20260608/cost-aware-partitioning-smoke/summary.json`
- threshold: `5.0%`
- baseline hash: `16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`
- candidate hash: `16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`

| status | count |
|---|---:|
| improvement | 0 |
| neutral | 6 |
| regression | 6 |
| incorrect | 0 |
| missing | 52 |

| workers | target | baseline s | candidate s | change | RSS change | status |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | - | - | - | - | missing |
| 1 | 2 | - | - | - | - | missing |
| 1 | 4 | 10.791 | 11.289 | +4.6% | +4.0% | neutral |
| 1 | 6 | 9.706 | 10.317 | +6.3% | +0.4% | regression |
| 1 | 8 | 9.830 | 10.097 | +2.7% | +3.0% | neutral |
| 1 | 10 | - | - | - | - | missing |
| 1 | 12 | 10.365 | 10.459 | +0.9% | -9.7% | neutral |
| 1 | 16 | 10.778 | 11.377 | +5.6% | -9.5% | regression |
| 2 | 1 | - | - | - | - | missing |
| 2 | 2 | - | - | - | - | missing |
| 2 | 4 | 10.305 | 10.995 | +6.7% | +0.5% | regression |
| 2 | 6 | - | - | - | - | missing |
| 2 | 8 | 9.591 | 10.926 | +13.9% | -3.5% | regression |
| 2 | 10 | 9.991 | 9.775 | -2.2% | +4.3% | neutral |
| 2 | 12 | - | - | - | - | missing |
| 2 | 16 | - | - | - | - | missing |
| 4 | 1 | - | - | - | - | missing |
| 4 | 2 | - | - | - | - | missing |
| 4 | 4 | - | - | - | - | missing |
| 4 | 6 | 9.718 | 10.066 | +3.6% | +0.6% | neutral |
| 4 | 8 | - | - | - | - | missing |
| 4 | 10 | - | - | - | - | missing |
| 4 | 12 | - | - | - | - | missing |
| 4 | 16 | - | - | - | - | missing |
| 6 | 1 | - | - | - | - | missing |
| 6 | 2 | - | - | - | - | missing |
| 6 | 4 | - | - | - | - | missing |
| 6 | 6 | - | - | - | - | missing |
| 6 | 8 | - | - | - | - | missing |
| 6 | 10 | 9.986 | 10.601 | +6.2% | -3.8% | regression |
| 6 | 12 | - | - | - | - | missing |
| 6 | 16 | - | - | - | - | missing |
| 8 | 1 | - | - | - | - | missing |
| 8 | 2 | - | - | - | - | missing |
| 8 | 4 | - | - | - | - | missing |
| 8 | 6 | - | - | - | - | missing |
| 8 | 8 | 9.547 | 9.433 | -1.2% | -1.6% | neutral |
| 8 | 10 | - | - | - | - | missing |
| 8 | 12 | 10.340 | 10.864 | +5.1% | +4.8% | regression |
| 8 | 16 | - | - | - | - | missing |
| 10 | 1 | - | - | - | - | missing |
| 10 | 2 | - | - | - | - | missing |
| 10 | 4 | - | - | - | - | missing |
| 10 | 6 | - | - | - | - | missing |
| 10 | 8 | - | - | - | - | missing |
| 10 | 10 | - | - | - | - | missing |
| 10 | 12 | - | - | - | - | missing |
| 10 | 16 | - | - | - | - | missing |
| 12 | 1 | - | - | - | - | missing |
| 12 | 2 | - | - | - | - | missing |
| 12 | 4 | - | - | - | - | missing |
| 12 | 6 | - | - | - | - | missing |
| 12 | 8 | - | - | - | - | missing |
| 12 | 10 | - | - | - | - | missing |
| 12 | 12 | - | - | - | - | missing |
| 12 | 16 | - | - | - | - | missing |
| 16 | 1 | - | - | - | - | missing |
| 16 | 2 | - | - | - | - | missing |
| 16 | 4 | - | - | - | - | missing |
| 16 | 6 | - | - | - | - | missing |
| 16 | 8 | - | - | - | - | missing |
| 16 | 10 | - | - | - | - | missing |
| 16 | 12 | - | - | - | - | missing |
| 16 | 16 | - | - | - | - | missing |
