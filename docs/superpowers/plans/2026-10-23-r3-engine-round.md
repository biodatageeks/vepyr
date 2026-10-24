# R3 ClinVar engine round

This vepyr pin carries functions#289–#294 and the integration conflict fix
through functions#295. The integration branch was gated at
9176255fe8a1e0310ede21aef58287d00017edf6; functions#295 merged with an
identical source tree as a8a6b2cd2b9b8e3ae266ea4ff45cced0ab07323c on
2026-10-24. The formats source is
6af1fa1480b2fc527e3b218585eae3a087f2aca9; the formats release is postponed.
The vepyr PR changes pins and this plan only.

## Per-fix record

All fixes are owned by datafusion-bio-functions. Formats and the cache schema
do not change. Downstream, vepyr only moves its function pin. Functions
dependency order is #289 → #290 → #291; #292 is a separate RefSeq leaf; #293
→ #294; the integration resolves the #291/#292 overlap in bc3cfc3. The three
second-pass regression fixes are based on the integrated tree. Each test name
below is a Rust unit fixture in datafusion/bio-function-vep/src/transcript_consequence.rs.
The targeted panel outcome is against progress_R0-final-99dd788.tsv.

### vepyr#220 — partial codon and coding_unknown (functions#289)

- **Trigger and root cause:** A coding variant at a partial terminal CDS boundary can have an undefined codon or an X-padded peptide. The engine inferred stop/coding terms from its local peptide window and did not apply VEP’s coding_unknown exclusions.
- **Pinned rules:** “return 0 if partial_codon(@_);” for stop-loss ([VariationEffect.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L1237-L1241)); coding_unknown requires unknown peptide data and excludes stronger predicates ([VariationEffect.pm](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L1621-L1637)). VEP 116 formats a partial endpoint as “start-?” ([OutputFactory.pm, release/116.0](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1730-L1735)); core maps untranslated spans to Mapper::Gap ([TranscriptMapper.pm, core c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/TranscriptMapper.pm#L450-L474)).
- **Functions change/order:** Fix 4ca1a9767a9b52254bd12c4882f0f309d2dc66cf; PR head 03014db8929a811aee43c8dee7b2c20ed403a15a. Base for #290, then #291.
- **Tests and panel:** issue_220_partial_terminal_insertion_can_coemit_stop_gained; coding_unknown_matches_peptide_ambiguity_and_vep_exclusions; partial_codon_vetoes_frameshift_on_both_strands_but_keeps_itc. Panel: 89/102 records and 97/144 entries identical. Expected gate movement: these attributed records improve; no positive performance delta is expected.

### vepyr#211/#212 — in-frame and PAV predicates (functions#290)

- **Trigger and root cause:** In-frame indels at a start boundary or stop/reference peptide can emit the wrong inframe or protein_altering_variant term, including alongside start_lost. The port did not follow VEP’s predicate ordering and peptide sentinel handling.
- **Pinned rule:** “return 0 unless defined $ref_pep && defined $alt_pep”; PAV rejects equal peptide lengths, insertion requires a reference codon and longer ALT, and deletion requires shorter ALT ([VariationEffect.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L375-L390), [insertion/deletion predicates](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L1107-L1112)).
- **Functions change/order:** PR head 1175b20eceb397662b5218e62de968eb1170dac6, stacked on #289 and used as the base for #291.
- **Tests and panel:** vep116_inframe_insertion_requires_codon_and_peptide_alleles; vep116_start_lost_yields_to_valid_inframe_insertion; protein_altering_variant_obeys_defined_peptides_and_stop_coemission. Panel: #211 830/843 records, 22,859/22,883 entries; #212 162/163 records, 4,515/4,515 entries. Expected gate movement: these issue rows improve; no positive performance delta is expected.

### vepyr#213/#234 — partial CDS boundary coordinates and terms (functions#291)

- **Trigger and root cause:** A delins or equal-length MNV crossing a CDS boundary leaves one mapper endpoint undefined. The engine used complete-codon positions and emitted codon terms where VEP has only partial coordinates; the equal-length MNV early return also lost positions.
- **Pinned rule:** A mapped CDS endpoint that is a Mapper::Gap is undefined ([BaseTranscriptVariation.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/BaseTranscriptVariation.pm#L258-L264)); core creates the untranslated Gap ([TranscriptMapper.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/TranscriptMapper.pm#L450-L474)); VEP 116 prints the surviving endpoint as “start-?” or “?-end” ([OutputFactory.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1730-L1735)).
- **Functions change/order:** Fixes 4dc0809203471c08edfc0fe9012f92d7f4cc1b25 and 7ddc150b7236350d0869d2fbec58c16ef5bd92ed; stack head ea79602ef0854b7c059cf70732b07e8659bf0eec. Follows #289/#290.
- **Tests and panel:** partial_cds_boundary_equal_length_mnv_plus_strand_keeps_only_partial_positions; partial_cds_boundary_equal_length_mnv_minus_strand_keeps_only_partial_positions; deletion_ending_in_tiny_intron_has_no_length_or_splice_term. Panel: #213 12/15 records, 104/107 entries; #234 30/38 records, 106/268 entries. Expected gate movement: these issue rows improve; no positive performance delta is expected.

### vepyr#232/#237 — edited RefSeq coordinates (functions#292)

- **Trigger and root cause:** On edited RefSeq transcripts, upstream insertions/deletions shift cDNA/CDS coordinates and a successful BAM edit changes the spliced reference. The engine could apply a cumulative edit offset to the neighboring mapper segment or use unedited translation coordinates.
- **Pinned rules:** “isolate the insertions and deletions found before the given variant and sum their lengths” ([TranscriptVariationAllele.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L1599-L1600)); VEP 116 marks successful BAM edits as ok and installs edited spliced sequence ([AnnotationType/Transcript.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/AnnotationType/Transcript.pm#L599-L617)); core slices edited mRNA at coding bounds ([Transcript.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/Transcript.pm#L910-L925)).
- **Functions change/order:** PR head 9c8955b0f12211bcf224b00f968aed58572b7e06; integrated after #289–#291, with the overlapping insertion window reconciled by bc3cfc3.
- **Tests and panel:** adjust_refseq_cds_output_position_applies_upstream_edit_after_mapper_fallback; refseq_bam_edit_alternate_cds_does_not_double_apply_mapper_offset; test_refseq_bam_edit_hgvsp_uses_edited_cds_when_ok. Panel: #232 1,014/1,020 records, 1,016/1,021 entries; #237 83/111 records, 112/147 entries. Expected gate movement: these issue rows improve; no positive performance delta is expected.

### vepyr#228 — stop-retaining terminal delins HGVSp (functions#293)

- **Trigger and root cause:** A stop-retaining delins leaves a longer local peptide window, so the engine formatted residues after the first translated stop as delinsTer.
- **Pinned rule:** “don't report other peptides after a stop is found” ([TranscriptVariationAllele.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L1958-L1962)); VEP 116 routes HGVSp through hgvs_protein ([OutputFactory.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1751-L1754)); core derives translated CDS by an inclusive slice ([Transcript.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/Transcript.pm#L903-L916)).
- **Functions change/order:** PR head 17f44915261385f460c753f6aa1b6c11f0e8c01b; #294 stacks on it.
- **Test and panel:** test_issue_228_preserves_partial_terminal_codon_in_codons. Panel: 7/11 records and 35/47 entries identical. Expected gate movement: these issue rows improve; no positive performance delta is expected.

### vepyr#245/#247 — partial-terminal HGVSp (functions#294)

- **Trigger and root cause:** Partial-terminal synonymous and in-frame indels need VEP’s shared X in peptide alleles; clipping the X or dropping it from an insertion loses HGVSp parity.
- **Pinned rule:** “if($partial_codon && $pep ne '*') { $pep .= 'X'; }” ([TranscriptVariationAllele.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L774-L777)); VEP 116 sets Amino_acids from pep_allele_string ([OutputFactory.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1724-L1727)).
- **Functions change/order:** Fix 0a3a04a5daa8e556f5b75e44be8d1921f877f888; PR head 9fcce454dc94f3db4ec3de8a7a5753594dc51669, after #228.
- **Tests and panel:** test_issue_245_hgvsp_synonymous_partial_terminal_codon_uses_local_peptide; test_issue_247_inframe_deletion_clips_shared_partial_terminal_x; test_issue_247_insertion_keeps_partial_terminal_x_on_hgvsp_alleles. Panel: #245 3/3 records and entries; #247 all 4/4 HGVSp entries match, while 1/4 full records match (the other consequence differences are #220). Expected gate movement: these HGVSp values improve; no positive performance delta is expected.

### R3 integration conflict — bc3cfc3

- **Trigger and root cause:** #291 retained the local CDS remainder for the displayed terminal peptide; #292 sized the alternate codon window by replaced reference length. One partial-codon flag had been doing both jobs, so its predicate veto erased payload needed for HGVSp.
- **Pinned rule:** Variation 2fb834b sizes the alternate window as codon length plus allele length minus replaced-nucleotide length ([TranscriptVariationAllele.pm](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L841-L877)); VEP 116 uses partial_codon for stop/frameshift predicates while amino-acid alleles drive protein formatting ([OutputFactory.pm, release/116.0](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1724-L1727)).
- **Functions change/order:** bc3cfc3 integrates after #291/#292 and separates display-window remainder from mapper-derived predicate state.
- **Test and panel:** r3_integration_keeps_partial_terminal_hgvsp_payload_after_frame_veto asserts L/LTLX and preserved terminal X after the frameshift veto. No direct ClinVar record is attributed to this integration-only conflict fix. Expected gate movement: preserve component parity with no new panel delta; no positive performance delta is expected.

### Second-pass regression fix — RefSeq mapper offset (0dcc12cb)

- **Trigger and root cause:** Two preceding cDNA insertions made a cumulative edit delta select the neighboring mapper segment; a RefSeq edited insertion gap then used a peptide-equality fallback over VEP’s shifted window.
- **Pinned rules:** Variation sums preceding RefSeq edit lengths ([TranscriptVariationAllele.pm](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L1599-L1600)); VEP 116 marks a successful BAM edit and installs its edited sequence ([AnnotationType/Transcript.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/AnnotationType/Transcript.pm#L599-L617)); core adjusts mapper cDNA coordinates by the length delta ([TranscriptMapper.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/TranscriptMapper.pm#L199-L220)).
- **Functions change/order:** 0dcc12cb9ecbeaea99185967848d667f9f399785, based on the integrated functions tree.
- **Test and records:** test_refseq_offset_uses_edit_adjusted_mapper_after_cdnainsertions; ClinVar 4501521, 218750, 4501528, and 3910356 now match golden byte-for-byte. Expected gate movement: restore these four first-pass regression records; no positive performance delta is expected.

### Second-pass regression fix — terminal stop delins (7227584)

- **Trigger and root cause:** A shared translated stop at the first changed residue was followed by a longer local peptide tail (*LX or *LRNYX); the frameshift resolver incorrectly emitted delinsTer.
- **Pinned rules:** “don't report other peptides after a stop is found” ([TranscriptVariationAllele.pm](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/TranscriptVariationAllele.pm#L1958-L1962)); VEP 116 formats protein consequence through hgvs_protein ([OutputFactory.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1751-L1754)); core translates the inclusive CDS slice ([Transcript.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/Transcript.pm#L903-L916)).
- **Functions change/order:** 722758409560a83e0234efca09b2c00807c9adcd, based on the integrated tree.
- **Test and records:** frameshift_shared_translated_stop_ignores_local_peptide_after_stop; ClinVar 998357, 1452502, 2088332, 2812067, and 2872368 now match golden byte-for-byte. Expected gate movement: restore these five first-pass regression records; no positive performance delta is expected.

### Second-pass regression fix — phase-padded partial CDS (ba31039)

- **Trigger and root cause:** Phase padding supplied X in the local peptide window for three in-frame insertions whose output peptides were known. The engine consequently added coding_unknown beside the specific in-frame term.
- **Pinned rules:** coding_unknown tests whether peptide alleles contain X and excludes a stronger in-frame predicate ([VariationEffect.pm, variation 2fb834b](https://github.com/Ensembl/ensembl-variation/blob/2fb834b987ede3824e200197a838ce11e91aeb4b/modules/Bio/EnsEMBL/Variation/Utils/VariationEffect.pm#L1621-L1637)); VEP 116 emits Amino_acids from pep_allele_string ([OutputFactory.pm](https://github.com/Ensembl/ensembl-vep/blob/57ea5c52340acc1f156267f810ad162e26597082/modules/Bio/EnsEMBL/VEP/OutputFactory.pm#L1724-L1727)); core represents insertion boundaries as Mapper::Gap ([Mapper.pm, c0cf13d](https://github.com/Ensembl/ensembl/blob/c0cf13daa961d80584bad797b2eb0ff3a7500ef3/modules/Bio/EnsEMBL/Mapper.pm#L264-L269)).
- **Functions change/order:** ba310391a07074c83c7b6ed3028db6cf234749ce, based on the integrated tree.
- **Test and records:** coding_unknown_keeps_unknown_terminal_insertion_peptide; ClinVar 2180991, 3910969, and 3910970 now match golden byte-for-byte. Expected gate movement: restore these three first-pass regression records; no positive performance delta is expected.

## Round acceptance and carried gates

The R3 panel outcome is 2,181/2,251 identical; the 76 changed HGVSp rows on
14 #202/#218/#219 records are inherited from R2 and unchanged. The second-pass
replay restores all 12 first-pass regressions byte-for-byte; no new true
regression records remain. The performance comparator flags positive deltas
only (the lines marked “<-- REGRESSION”); no positive delta is expected.

Functions#295 merged as a8a6b2cd2b9b8e3ae266ea4ff45cced0ab07323c. Its tree
equals gated integration head 9176255fe8a1e0310ede21aef58287d00017edf6
(git diff 9176255 a8a6b2c is empty), so the maintainer carries the md5,
ClinVar full-contig, and performance gates over from
.r3-integration-evidence/pass2/. vepyr master remains
b7538e955b68587f946618e058e3d7851dc5f26e. The final PR re-pin runs
cargo test --no-default-features --features mimalloc; review and CI run on
the resulting vepyr head. /parity-wgs remains pending for the coordinator and
must run last.
