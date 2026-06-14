# Cache build benchmark — 21,22

- created: 2026-06-14T09:54:55+0200
- knob swept: `build_concurrency`
- platform: macOS-26.4.1-arm64-arm-64bit
- git: test-annotation-target-partitions@b2bea8a8d3
- raw cache: /tmp/bench_chr2122/raw_subset

| build_concurrency | median s | min s | max s | speedup | RSS GiB | rows |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 351.92 | 351.92 | 351.92 | 1.0x | 6.81 | 29893709 |
| 2 | 198.34 | 198.34 | 198.34 | 1.774x | 7.30 | 29893709 |
| 4 | 177.78 | 177.78 | 177.78 | 1.98x | 6.69 | 29893709 |
| 8 | 178.48 | 178.48 | 178.48 | 1.972x | 6.43 | 29893709 |
