"""One annotation of a VCF with one output path, for the writer memory figures of the supplement (Section 8.4).

Usage: writer_once.py collect|parquet|vcf <input.vcf.gz> <cache_dir> <fasta> <out>
collect: annotate(...).collect(); parquet: sink_parquet(out, row_group_size=5000); vcf: annotate(..., output_vcf=out).
One worker. Peak RSS and wall time are measured by the caller (wait4 on this process).
"""

import sys

import vepyr

path, vcf, cache, fasta, out = sys.argv[1:6]
if path == "vcf":
    vepyr.annotate(
        vcf,
        cache,
        reference_fasta=fasta,
        workers=1,
        output_vcf=out,
        show_progress=False,
    )
else:
    lf = vepyr.annotate(
        vcf, cache, reference_fasta=fasta, workers=1, show_progress=False
    )
    if path == "collect":
        df = lf.collect()
        open(out, "w").write(f"{df.height}\n")
    else:
        lf.sink_parquet(out, row_group_size=5000)
