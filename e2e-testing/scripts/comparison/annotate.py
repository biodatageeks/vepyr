"""The only module that imports vepyr.

The import is deferred into the function body so the rest of the harness -- and
its tests -- run without the native extension built.
"""

import os
import sys
import time

from . import vcfio

BACKEND = "parquet"
REUSE_MIN_BYTES = 1000


def supported_vep_targets():
    """Return the native compatibility matrix without importing at module load."""
    import vepyr

    return vepyr.supported_vep_targets()


def cache_contig_identity(cache_dir, chrom, expected_cache_version):
    """Validate one contig's embedded Parquet identity through the native runtime."""
    import vepyr

    return vepyr.cache_contig_identity(
        cache_dir,
        chrom,
        expected_cache_version=expected_cache_version,
    )


def annotate_contig(
    chrom_vcf_gz,
    cache_dir,
    fasta,
    output_vcf,
    workers,
    annotate_kwargs,
    force=False,
    bgzf=False,
):
    """Annotate one contig slice, reusing existing output unless forced.

    Returns (elapsed_seconds, output_variant_count). elapsed is None when the
    existing output was reused, which the report renders as a blank timing.
    """
    if (
        not force
        and os.path.exists(output_vcf)
        and os.path.getsize(output_vcf) > REUSE_MIN_BYTES
    ):
        # Validate framing before reading: a mis-framed .vcf.gz would otherwise
        # fail inside gzip with BadGzipFile instead of a useful message.
        _validate_bgzf(output_vcf, bgzf)
        n_out = vcfio.count_data_lines(output_vcf)
        size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
        print(f"  Reusing {output_vcf} ({n_out:,} variants, {size_mb:.0f} MB)")
        print("  Use --force to re-run")
        return None, n_out

    import vepyr

    t0 = time.time()
    vepyr.annotate(
        chrom_vcf_gz,
        cache_dir,
        everything=True,
        reference_fasta=fasta,
        cache_format=BACKEND,
        output_vcf=output_vcf,
        workers=workers,
        **annotate_kwargs,
    )
    elapsed = time.time() - t0

    _validate_bgzf(output_vcf, bgzf)
    n_out = vcfio.count_data_lines(output_vcf)
    size_mb = os.path.getsize(output_vcf) / (1024 * 1024)
    rate = n_out / elapsed if elapsed > 0 else 0
    print(
        f"  Done: {n_out:,} variants in {elapsed:.1f}s "
        f"({rate:,.0f} variants/s), {size_mb:.0f} MB"
    )
    return elapsed, n_out


def _validate_bgzf(output_vcf, bgzf):
    if not bgzf:
        return
    if vcfio.is_bgzf(output_vcf):
        print("  bgzf check: output is valid block-gzip (BGZF)")
    else:
        sys.exit(f"Error: --bgzf output {output_vcf} is not valid BGZF")
