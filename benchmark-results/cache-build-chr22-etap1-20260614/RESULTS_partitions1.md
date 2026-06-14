# Cache build benchmark — 22

- created: 2026-06-14T02:08:01+0200
- knob swept: `build_concurrency`
- platform: macOS-26.4.1-arm64-arm-64bit
- git: test-annotation-target-partitions@b2bea8a8d3
- raw cache: /tmp/bench_chr22_p1/raw_subset

| build_concurrency | median s | min s | max s | speedup | RSS GiB | rows |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 208.33 | 208.33 | 208.33 | 1.0x | 6.77 | 15262195 |
| 6 | 165.27 | 165.27 | 165.27 | 1.261x | 6.25 | 15262195 |
