# chr22 Indexed Parquet baseline - 2026-06-08

## Dataset

- VEPyR commit: `8caf5cb7fb2085331375a8834021e95c5a819d38`
- Branch: `test-annotation-target-partitions`
- Platform: Apple Silicon, macOS 26.4.1
- Cache: full Ensembl 115 GRCh38 Indexed Parquet cache for chr22
- Input: 59,013 sorted, single-ALT records using the `chr22` contig name
- Annotation flags: `everything=True` with the GRCh38 reference FASTA
- Repetitions: 3 fresh processes per configuration

The first serial run established the reference VCF-body SHA-256:

```text
16cc94c0afee4553d18f37b8a0083b8f5011d32cab753c72f75e24a125664b30
```

All 15 measured outputs matched this hash and contained 59,013 records.
The same dataset was previously compared semantically against Ensembl VEP:
59,013 variants and 765,250 consequences matched, with zero differences.

## Results

| Mode | Workers | Median time | Range | Speedup vs serial | Median peak RSS |
|---|---:|---:|---:|---:|---:|
| Serial | 1 | 32.271 s | 27.913-34.865 s | 1.00x | 2.28 GiB |
| Parallel path | 1 | 25.305 s | 22.124-30.524 s | 1.28x | 2.79 GiB |
| Parallel path | 2 | 22.231 s | 21.470-24.510 s | 1.45x | 2.76 GiB |
| Parallel path | 4 | 26.465 s | 23.755-28.398 s | 1.22x | 2.88 GiB |
| Parallel path | 8 | 22.840 s | 22.716-23.676 s | 1.41x | 2.78 GiB |

## Initial conclusion

The current implementation gains useful throughput from the parallel lookup
path, but does not scale monotonically with worker count. Two workers produced
the best median and the best individual run. Four workers were slower than two,
while eight workers recovered most of the gain without beating two workers.

The parallel path also increased median peak RSS by roughly 0.48-0.60 GiB.
The first optimization target should therefore be worker/partition scheduling
and queue overhead, not simply increasing the default worker count.
