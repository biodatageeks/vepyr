# Cross-check against Ensembl VEP 116.0

Run on 2026-10-18 using the exact `release_116.0` image:

```text
ensemblorg/ensembl-vep:release_116.0
sha256:f354dd8d09073e4d943acbbd02f5eb234a9d9e9d444371c1c349910f2123de11
VEP v116.0 / API 116 / GRCh38.p14
Ensembl core c0cf13d / variation 2fb834b
```

The run used the release 116 merged cache, the same FASTA, the same normalized
ClinVar MT input, and the same VEP flags as the stored golden run. The input
was the 3,134-record `clinvar_20261011_MT.norm.vcf.gz` slice (record-body MD5
`387468a844114c3f32b36a925c94d9ad`); the source ClinVar file MD5 is
`0f3a1a5e84252386033d5fcab1bf1f6a`. Generated outputs are in
`/tmp/clinvar-chrmt-1160-confirmation` on the run machine.

VEP was run with `--dir /opt/vep/.vep --cache --offline --assembly GRCh38
--merged --vcf --force_overwrite --no_stats --everything --hgvs` and the
release 116 primary assembly FASTA.

## Results

- Fresh VEP 116.0 outputs for `codon_table`, `csq_order`, and `hgvs_shift` are
  body-identical to the checked-in VEP 116.0 fixture outputs (12, 3, and 4
  records, respectively).
- The fresh full MT run and vepyr each emitted 3,134 records. They differ on
  2,327 records, and the exact set of mismatching VCF record keys is the same
  as in the VEP 116.2 comparison and the stored 116.0 ledger. The VEP 116.0
  record-body MD5 is `148b2439b649d7c8cbedf4c5c22e56fc`.
- Fresh VEP 116.0 and VEP 116.2 full outputs differ at one record, MT:9204.
  This is the terminal frameshift HGVSp notation change already recorded in
  [`full-chrmt-1162-verification.md`](full-chrmt-1162-verification.md); the
  record remains a mismatch against vepyr, so the mismatch key set is unchanged.
- The stored ledger class totals are unchanged: 2,030 order-only records, 294
  codon-table records, and four HGVS records. The MT:6467 deletion is shared
  by the latter two classes, making 2,327 unique records.

## `csq_order` and `codon_table`

The 116.0 image contains the same ordering mechanism: `VariationFeature`
returns transcript variations by sorted hash keys, and
`BaseVariationFeature::_get_transcript_key` builds each key as
`stable_id_dbID`. This produces the same ND4L-before-ND4 order as the stored
116.0 fixture. The source is
[`VariationFeature.pm` at variation revision `2fb834b`](https://github.com/Ensembl/ensembl-variation/blob/2fb834b/modules/Bio/EnsEMBL/Variation/VariationFeature.pm)
and
[`BaseVariationFeature.pm` at `2fb834b`](https://github.com/Ensembl/ensembl-variation/blob/2fb834b/modules/Bio/EnsEMBL/Variation/BaseVariationFeature.pm).

The 116.0 image's
`/opt/vep/src/ensembl-vep/Bio/EnsEMBL/Variation/BaseTranscriptVariation.pm`
implements `_codon_table` by reading
`_variation_effect_feature_cache->{codon_table}` first, then falls back to the
slice `codon_table` attribute, then to table 1. The freshly reproduced
`codon_table` fixture is byte-identical to the stored VEP 116.0 output, so the
mitochondrial table-2 consequences are present in this image as well. This is
the same cache contract and fallback as 116.2.

The vepyr-side reader and translation findings are unchanged: the converted
cache has null `codon_table` values because the reader looks at the wrong JSON
path, and the engine translates with table 1. Those implementation findings
are detailed in the parent analysis plan.

## `hgvs_shift`: causal check with the 116.0 source

The exact 116.0 `TranscriptVariationAllele::_genomic_shift` has the same
boundary behavior as 116.2: it expands the variation slice, constrains it to
the sequence region, then takes the last 1,000 bases as `post_seq`. When the
constrained MT slice is shorter than 2,000 bases, that suffix can start before
the variant. See
[`TranscriptVariationAllele.pm` at variation revision `2fb834b`](https://github.com/Ensembl/ensembl-variation/blob/2fb834b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm).

I reran the four HGVS fixture inputs with a temporary copy of the 116.0 module.
The only code change selected the downstream sequence immediately after the
variation interval, up to the MT boundary:

```perl
my $post_offset = $var_end - $new_slice->start + 1;
my $post_seq = substr($seqs, $post_offset, $area_to_search);
```

With the unmodified module, VEP 116.0 emits the same short shifts and partial
deletion HGVS as the stored golden reference. With that flank-only diagnostic
change, it emits:

| Input | Default VEP 116.0 | Corrected-flank diagnostic |
|---|---|---|
| MT:15933 `GA>G` | TRNT `n.47del`, offset empty | `n.50del`, offset 3 |
| MT:15939 `C>CT` | TRNT `n.54dup`, offset 1 | `n.57dup`, offset 5 |
| MT:15939 `CT>C` | TRNT `n.54del`, offset 1 | `n.57del`, offset 4 |
| MT:6467 9,119 bp deletion (REF 9,120 bases) | CYTB `c.1_840del`; ATP6 `c.1_681del` | CYTB `c.3_842del`, offset 2; ATP6 HGVSc empty |

These corrected HGVS outputs match vepyr and ClinVar for the affected
transcripts. The diagnostic patch was mounted into the container with Docker's
read-only bind mount and was not applied to the image or a tracked Ensembl
source file.

One difference remains separate: CYTB's consequence for the large deletion is
`coding_sequence_variant&5_prime_UTR_variant` in both default VEP 116.0 and
116.2, while vepyr reports `start_lost&5_prime_UTR_variant`. The flank
diagnostic changes the HGVS and offset but leaves VEP's consequence unchanged.
This consequence mismatch is therefore still open; the cross-version check
does not establish its root cause.
