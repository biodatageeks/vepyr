# HGVS shift confirmation against Ensembl VEP 116.2

Run on 2026-10-18 against the exact VEP image used for the full chrMT check:

```text
ensemblorg/ensembl-vep:release_116.2
sha256:5c57abdc40b637cac198370b4c114777fa24de3336141fdf09d2b0c43cd4b8da
VEP v116.2 / API 116 / GRCh38
Ensembl core 0d85231 / variation db7394a
```

The baseline outputs and source revisions are recorded in
[`full-chrmt-1162-verification.md`](full-chrmt-1162-verification.md). This note
records additional source instrumentation and a one-line diagnostic patch to
the container's temporary copy of
[`TranscriptVariationAllele.pm` at variation revision `db7394a`](https://github.com/Ensembl/ensembl-variation/blob/db7394a/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm).
The image, cache, FASTA, engine repository, and tracked Ensembl source were not
modified.

The same source behavior and corrected-flank diagnostic have also been
confirmed against the exact VEP 116.0 image. See
[`vep1160-cross-check.md`](vep1160-cross-check.md) for its image digest,
revision, full MT comparison, fixture results, and diagnostic outputs.

## Source path

VEP 116.2's `TranscriptVariationAllele::_genomic_shift`:

1. Takes the whole variation-feature slice.
2. Expands it to leave 1,000 bases around the variant and calls
   `constrain_to_seq_region()`.
3. Defines `pre_seq` as the first 1,000 bases and `post_seq` as the last 1,000
   bases of the resulting slice.

That last step assumes the constrained slice is at least 2,000 bases long.
Near the end of the 16,569 bp mitochondrial sequence, it is shorter. The last
1,000 bases then overlap upstream sequence, rather than starting immediately
after the variant. `perform_shift` compares against that `post_seq`, producing
the observed premature stop or no shift.

## TRNT probes

The original normalized records are:

| Input | VEP 116.2 default | Sequence-based shift | Diagnostic VEP with corrected flank |
|---|---|---|---|
| MT:15933 `GA>G` | `n.47del`, shift 0 | 3 bases to `n.50del` | `n.50del`, shift 3 |
| MT:15939 `C>CT` | `n.54dup`, shift 1 | 5 bases to `n.57dup` | `n.57dup`, shift 5 |
| MT:15939 `CT>C` | `n.54del`, shift 1 | 4 bases to `n.57del` | `n.57del`, shift 4 |

For the A deletion, VEP's constrained slice is 14934–16569 (1,636 bp). Its
`post_seq` starts at MT:15570, but the true post-variant flank starts at MT:15935.
The incorrect flank begins `TATTC...`; the correct one begins with the remaining
A run. The same 1,000-base suffix mistake makes the T-run cases see only one
matching T. The corrected diagnostic run used the actual sequence after the
variant, truncated only by the MT sequence-region end. It returned the full
3/5/4 shifts and matched both vepyr 0.10.0 and ClinVar HGVS.

## MT:6467 large deletion

The VCF record is MT:6467, REF length 9,120, ALT `C`. In the VEP variation
object the deleted interval is MT:6468–15586. Its source slice is 1–16569; the
expanded, constrained slice is 5468–16569 (11,102 bp). VEP's last-1,000-base
`post_seq` starts at MT:15570, 17 bases inside the deleted interval. The actual
post-deletion flank starts at MT:15587, with 983 bases available.

In the default VEP 116.2 run, CYTB is emitted as `c.1_840del` with no
`HGVS_OFFSET`; ATP6 is emitted as `c.1_681del`. A temporary diagnostic patch
changed only how `_genomic_shift` chooses `post_seq`: it starts immediately
after the variation interval and uses the remaining sequence up to the MT end.
With that change, VEP emitted CYTB `c.3_842del` with `HGVS_OFFSET=2`, matching
vepyr. It emitted no ATP6 HGVSc after the shift, also matching vepyr: the
existing `hgvs_transcript` boundary guard rejects the shifted end beyond the
transcript slice. Thus both transcript differences follow from the same
incorrect MT-end flank in the default VEP 116.2 run.

## Diagnostic patch and scope

The temporary change was equivalent, for these plus-strand MT probes, to:

```perl
my $post_offset = $var_end - $new_slice->start + 1;
my $post_seq = substr($seqs, $post_offset, $area_to_search);
```

The release source instead uses the final 1,000 bases of `$seqs`. This was a
causal diagnostic, not a proposed general Ensembl fix: other strand and
sequence-region boundary cases need their own treatment. The finding confirms
that the current vepyr annotations use the adjacent genomic sequence correctly
for these examples. It does not decide whether vepyr should reproduce VEP
116.2's default output for exact parity.
