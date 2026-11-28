Evidence for the "Fields filled" row of Figure 1A and the fields paragraph of supplement Section 10 (2026-11-20).

Data: HG002 chromosome 21, Ensembl cache of release 116 (55,812 records), the chromosome on which the stored VEP 116.0
output (golden.vcf.gz of ensembl-engine-comparison-20261108/hg002-w8) has a value in each of the 80 CSQ fields.
Engines: vepyr 0.11.0, vep-rs 0.5.0, fastVEP 0.6.1 (builds of this directory's parent), one worker, the per-chromosome
caches and commands of scripts/run.zsh.

fieldfill.py            records with at least one non-empty value, per CSQ field
feattypes.py            CSQ entries per Feature_type
golden-ens-hg002-*.json VEP output: chr22 (79 of 80 fields filled), chr21, chr19, chr17 (80 of 80)
engines-ens-21.json     fastVEP 0.6.1: 78 fields declared, 73 of VEP's 80, 66 filled; vep-rs 0.5.0: 80 declared, 75 filled;
                        vepyr 0.11.0: 80 filled, record lines identical to VEP (body MD5 e230640e4e88e6c03d475f87180cc5fe)

CSQ entries on chr21: VEP 691,543 (Transcript 675,947; intergenic 12,794; RegulatoryFeature 2,612; MotifFeature 190);
vep-rs 0.5.0 and fastVEP 0.6.1 688,741 each (no RegulatoryFeature or MotifFeature entries).
Merged cache, chr21: fastVEP declares 78 fields, 74 of VEP's 86 (not REFSEQ_MATCH, REFSEQ_OFFSET, GIVEN_REF, USED_REF,
BAM_EDIT, SWISSPROT, TREMBL, UNIPARC, UNIPROT_ISOFORM, GENE_PHENO, DOMAINS, miRNA); vep-rs declares 82 (not REFSEQ_MATCH,
SOURCE, REFSEQ_OFFSET, BAM_EDIT).

Merged cache, HG002 chromosome 21 (merged-21.json, 2026-11-20): VEP 116.0 writes 86 fields and fills 85; REFSEQ_MATCH is
empty in every record (no "rseq_" value either in the VEP output for HG002 chr1, chr17, chr19, chr22 or ClinVar chr1).
vepyr 0.11.0: 86 declared, 85 filled, record lines identical to VEP (body MD5 a42bbf6468779da3c6ad0102c5bfd8e2).
vep-rs 0.5.0: 82 declared (not REFSEQ_MATCH, SOURCE, REFSEQ_OFFSET, BAM_EDIT), 77 filled (the five motif fields empty).
fastVEP 0.6.1: 74 of the 86 declared, 67 filled (motif fields, SIFT and PolyPhen empty).
