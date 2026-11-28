| Mode | Layout | Workers | n | Wall (s) | CPU (s) | User (s) | System (s) | Peak RSS (GiB) |
|---|---|---|---|---|---|---|---|---|
| cold | sorted | 1 | 3 | 5.19 (5.07-5.24) | 7.38 (7.34-7.53) | 6.74 (6.71-6.86) | 0.64 (0.63-0.67) | 1.13 (1.10-1.16) |
| cold | tiered | 1 | 3 | 4.41 (4.36-4.49) | 5.13 (5.11-5.20) | 4.49 (4.48-4.55) | 0.65 (0.62-0.65) | 1.21 (1.17-1.21) |
| cold | sorted | 8 | 3 | 2.62 (2.53-2.62) | 8.05 (8.03-8.06) | 7.12 (7.06-7.13) | 0.93 (0.93-0.97) | 1.56 (1.54-1.57) |
| cold | tiered | 8 | 3 | 2.29 (2.22-2.30) | 6.02 (5.99-6.19) | 5.07 (5.04-5.17) | 0.95 (0.95-1.02) | 1.53 (1.52-1.54) |

Ratio sorted / tiered of the medians (> 1: tiering helps):

- cold, 1 workers: wall_s 1.177, cpu_s 1.439, user_s 1.501, sys_s 0.985, max_rss_kib 0.938, annotation_s 1.200
- cold, 8 workers: wall_s 1.144, cpu_s 1.337, user_s 1.404, sys_s 0.979, max_rss_kib 1.019, annotation_s 1.201

Invalid runs (failed, output differs, or cold run with > 5% of the shard cached): 0
