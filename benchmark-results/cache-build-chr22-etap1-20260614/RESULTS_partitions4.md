# Cache build benchmark — 22

- created: 2026-06-14T01:53:40+0200
- knob swept: `build_concurrency`
- platform: macOS-26.4.1-arm64-arm-64bit
- git: test-annotation-target-partitions@b2bea8a8d3
- raw cache: /tmp/bench_chr22full/raw_subset

| build_concurrency | median s | min s | max s | speedup | RSS GiB | rows |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 304.75 | 304.75 | 304.75 | 1.0x | 6.26 | 15262195 |
| 2 | 256.01 | 256.01 | 256.01 | 1.19x | 5.96 | 15262195 |
| 4 | 233.51 | 233.51 | 233.51 | 1.305x | 6.42 | 15262195 |
| 6 | 246.20 | 246.20 | 246.20 | 1.238x | 6.14 | 15262195 |
