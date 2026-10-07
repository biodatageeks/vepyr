# SNVs surrounding consecutive ALT-dot records

Genuine merged 116 chr21 data, preserving all biological fields and native source
identity. Both original normalized inputs from issue #151 are kept separately:
DT-1d6dc19f25 (trailing) and DT-ce2724f603 (leading). They share exact bytes but
retain their distinct ported-test identities. The reference-consistent control
is independent. No input REF is repaired.

Actual Docker VEP 116.2 --everything --merged oracles and normalization commands
are in provenance.json. Both surviving SNVs remain in order, positions 25587759
and 25587762, with 39 ordered CSQ entries each; the two intervening ALT-dot rows
must be absent. Exonic ENST00000307301 uses T while intronic mappings retain
the supplied REF for USED_REF; ordinary HGVSc uses the genomic reference.

prepare.py filters the downloaded merged cache to the 5 kb context around both
SNVs and retains complete exon/translation context. Its coordinate-preserving
FASTA contains real sequence 25567000–25613000 and N padding elsewhere. The
reference policy comes from verified native info.txt BAM metadata; it is not
inferred from the merged label. bgzip/samtools/tabix generate indexes.
