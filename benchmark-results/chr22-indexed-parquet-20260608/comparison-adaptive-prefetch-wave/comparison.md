# Parallel benchmark comparison

- baseline: `benchmark-results/chr22-indexed-parquet-20260608/parallelism-full-matrix-baseline/summary.json`
- candidate: `benchmark-results/chr22-indexed-parquet-20260608/parallelism-full-matrix-adaptive-prefetch-wave/summary.json`
- threshold: `5.0%`
- baseline hash: `16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`
- candidate hash: `16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30`

| status | count |
|---|---:|
| improvement | 40 |
| neutral | 17 |
| regression | 7 |
| incorrect | 0 |
| missing | 0 |

| workers | target | baseline s | candidate s | change | RSS change | status |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 22.189 | 22.081 | -0.5% | -8.6% | neutral |
| 1 | 2 | 19.071 | 15.353 | -19.5% | -1.4% | improvement |
| 1 | 4 | 13.856 | 10.791 | -22.1% | -4.0% | improvement |
| 1 | 6 | 12.815 | 9.706 | -24.3% | -6.8% | improvement |
| 1 | 8 | 11.162 | 9.830 | -11.9% | -12.3% | improvement |
| 1 | 10 | 11.811 | 10.099 | -14.5% | -11.5% | improvement |
| 1 | 12 | 11.002 | 10.365 | -5.8% | -1.5% | improvement |
| 1 | 16 | 10.945 | 10.778 | -1.5% | -1.5% | neutral |
| 2 | 1 | 19.919 | 20.626 | +3.6% | -1.2% | neutral |
| 2 | 2 | 16.143 | 15.794 | -2.2% | -1.8% | neutral |
| 2 | 4 | 14.142 | 10.305 | -27.1% | -4.8% | improvement |
| 2 | 6 | 13.788 | 9.967 | -27.7% | -7.3% | improvement |
| 2 | 8 | 10.459 | 9.591 | -8.3% | -5.5% | improvement |
| 2 | 10 | 11.869 | 9.991 | -15.8% | -12.2% | improvement |
| 2 | 12 | 11.292 | 10.384 | -8.0% | -2.6% | improvement |
| 2 | 16 | 11.391 | 11.087 | -2.7% | -7.8% | neutral |
| 4 | 1 | 22.122 | 23.038 | +4.1% | +2.6% | neutral |
| 4 | 2 | 14.145 | 14.871 | +5.1% | -1.1% | regression |
| 4 | 4 | 13.574 | 10.147 | -25.3% | +3.2% | improvement |
| 4 | 6 | 13.416 | 9.718 | -27.6% | -11.5% | improvement |
| 4 | 8 | 12.187 | 9.654 | -20.8% | -15.8% | improvement |
| 4 | 10 | 10.953 | 9.850 | -10.1% | -10.8% | improvement |
| 4 | 12 | 11.229 | 10.625 | -5.4% | -8.9% | improvement |
| 4 | 16 | 11.267 | 11.172 | -0.8% | -6.9% | neutral |
| 6 | 1 | 21.541 | 20.831 | -3.3% | +1.2% | neutral |
| 6 | 2 | 15.846 | 15.919 | +0.5% | -0.7% | neutral |
| 6 | 4 | 15.831 | 11.380 | -28.1% | +1.9% | improvement |
| 6 | 6 | 13.140 | 9.764 | -25.7% | -6.1% | improvement |
| 6 | 8 | 11.559 | 9.555 | -17.3% | -15.6% | improvement |
| 6 | 10 | 10.607 | 9.986 | -5.9% | -10.8% | improvement |
| 6 | 12 | 10.901 | 10.722 | -1.6% | -0.7% | neutral |
| 6 | 16 | 10.606 | 11.325 | +6.8% | +4.9% | regression |
| 8 | 1 | 21.670 | 22.488 | +3.8% | +1.2% | neutral |
| 8 | 2 | 14.333 | 15.855 | +10.6% | -2.0% | regression |
| 8 | 4 | 13.846 | 10.579 | -23.6% | -0.6% | improvement |
| 8 | 6 | 13.392 | 9.360 | -30.1% | -12.1% | improvement |
| 8 | 8 | 11.774 | 9.547 | -18.9% | -11.2% | improvement |
| 8 | 10 | 10.799 | 9.925 | -8.1% | -13.8% | improvement |
| 8 | 12 | 11.716 | 10.340 | -11.7% | -11.5% | improvement |
| 8 | 16 | 11.849 | 10.899 | -8.0% | +1.1% | improvement |
| 10 | 1 | 19.198 | 20.411 | +6.3% | +1.0% | regression |
| 10 | 2 | 13.981 | 15.268 | +9.2% | +2.0% | regression |
| 10 | 4 | 15.764 | 10.527 | -33.2% | -3.0% | improvement |
| 10 | 6 | 12.540 | 9.513 | -24.1% | -12.7% | improvement |
| 10 | 8 | 12.995 | 9.961 | -23.3% | -6.5% | improvement |
| 10 | 10 | 11.140 | 10.064 | -9.7% | -13.5% | improvement |
| 10 | 12 | 10.538 | 10.270 | -2.5% | -6.6% | neutral |
| 10 | 16 | 11.475 | 10.966 | -4.4% | -3.2% | neutral |
| 12 | 1 | 19.432 | 20.525 | +5.6% | -0.5% | regression |
| 12 | 2 | 15.445 | 13.519 | -12.5% | +2.1% | improvement |
| 12 | 4 | 15.326 | 10.634 | -30.6% | -1.7% | improvement |
| 12 | 6 | 12.659 | 9.674 | -23.6% | -1.5% | improvement |
| 12 | 8 | 11.015 | 9.572 | -13.1% | -7.9% | improvement |
| 12 | 10 | 11.798 | 10.212 | -13.4% | -7.7% | improvement |
| 12 | 12 | 10.817 | 10.321 | -4.6% | -2.7% | neutral |
| 12 | 16 | 10.922 | 10.700 | -2.0% | +4.7% | neutral |
| 16 | 1 | 18.975 | 20.404 | +7.5% | +7.8% | regression |
| 16 | 2 | 15.306 | 14.818 | -3.2% | +2.6% | neutral |
| 16 | 4 | 13.741 | 10.425 | -24.1% | -1.1% | improvement |
| 16 | 6 | 13.452 | 9.948 | -26.0% | -2.2% | improvement |
| 16 | 8 | 11.243 | 9.423 | -16.2% | -11.0% | improvement |
| 16 | 10 | 10.784 | 9.904 | -8.2% | -8.9% | improvement |
| 16 | 12 | 11.025 | 10.323 | -6.4% | +3.0% | improvement |
| 16 | 16 | 11.130 | 10.978 | -1.4% | -2.3% | neutral |
