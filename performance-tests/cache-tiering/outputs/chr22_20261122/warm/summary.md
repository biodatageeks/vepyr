| Mode | Layout | Workers | n | Wall (s) | CPU (s) | User (s) | System (s) | Peak RSS (GiB) |
|---|---|---|---|---|---|---|---|---|
| warm | sorted | 1 | 10 | 4.54 (4.47-4.61) | 7.35 (7.26-7.46) | 6.83 (6.78-6.91) | 0.50 (0.48-0.55) | 1.14 (1.09-1.21) |
| warm | tiered | 1 | 10 | 3.48 (3.41-3.53) | 5.08 (4.97-5.14) | 4.59 (4.51-4.63) | 0.49 (0.46-0.51) | 1.23 (1.19-1.28) |
| warm | sorted | 8 | 10 | 1.86 (1.82-1.98) | 7.72 (7.67-7.77) | 7.03 (6.98-7.08) | 0.69 (0.66-0.71) | 1.58 (1.55-1.62) |
| warm | tiered | 8 | 10 | 1.52 (1.50-1.55) | 5.70 (5.66-5.74) | 4.98 (4.95-5.01) | 0.72 (0.70-0.73) | 1.58 (1.54-1.66) |

Ratio sorted / tiered of the medians (> 1: tiering helps):

- warm, 1 workers: wall_s 1.303, cpu_s 1.444, user_s 1.489, sys_s 1.020, max_rss_kib 0.929, annotation_s 1.335
- warm, 8 workers: wall_s 1.224, cpu_s 1.354, user_s 1.411, sys_s 0.958, max_rss_kib 1.002, annotation_s 1.308

Invalid runs (failed, output differs, or cold run with > 5% of the shard cached): 0
