# Cache build benchmark — 19,20,21,22

- created: 2026-06-14T12:45:28+0200
- knob swept: `build_concurrency`
- platform: macOS-26.4.1-arm64-arm-64bit
- git: test-annotation-target-partitions@b2bea8a8d3
- raw cache: /tmp/bench_chr19_22/raw_subset

| build_concurrency | median s | min s | max s | speedup | RSS GiB | rows |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1047.34 | 1047.34 | 1047.34 | 1.0x | 10.57 | 78305358 |
| 2 | 649.06 | 649.06 | 649.06 | 1.614x | 8.88 | 78305358 |
| 4 | 491.05 | 491.05 | 491.05 | 2.133x | 10.61 | 78305358 |
| 8 | 420.99 | 420.99 | 420.99 | 2.488x | 9.87 | 78305358 |
