Running Ensembl VEP in Docker

1. Normalize the input VCF (decompose multi-allelics)

bcftools norm -m -both \
-o /Users/mwiewior/research/git/vepyr/sandbox/HG002_normalized.vcf \
/Users/mwiewior/research/git/datafusion-bio-functions/vep-benchmark/data/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz

2. Create a working directory

mkdir -p /tmp/vep_work
cp /tmp/vep_work/HG002_normalized.vcf /tmp/vep_work/

(Or skip the copy if you already normalized into /tmp/vep_work.)

3. Run VEP with the standard 74-field mode

docker run --rm \
-v /Users/mwiewior/research/data/vep/homo_sapiens/115_GRCh38:/opt/vep/.vep/homo_sapiens/115_GRCh38:ro \
-v /tmp/vep_work:/work \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated.vcf \
--vcf \
--force_overwrite \
--no_stats \
--regulatory \
--variant_class \
--canonical \
--tsl \
--mane \
--protein \
--gene_phenotype \
--ccds \
--uniprot \
--check_existing \
--af \
--af_1kg \
--af_gnomade \
--af_gnomadg \
--max_af \
--pubmed \
--fields
"Allele,Consequence,IMPACT,SYMBOL,Gene,Feature_type,Feature,BIOTYPE,EXON,INTRON,HGVSc,HGVSp,cDNA_position,CDS_position,Protein_position,Amino_acids,Codons,
Existing_variation,DISTANCE,STRAND,FLAGS,SYMBOL_SOURCE,HGNC_ID,MOTIF_NAME,MOTIF_POS,HIGH_INF_POS,MOTIF_SCORE_CHANGE,TRANSCRIPTION_FACTORS,SOURCE,VARIANT_CL
ASS,CANONICAL,TSL,MANE_SELECT,MANE_PLUS_CLINICAL,ENSP,GENE_PHENO,CCDS,SWISSPROT,TREMBL,UNIPARC,UNIPROT_ISOFORM,AF,AFR_AF,AMR_AF,EAS_AF,EUR_AF,SAS_AF,gnomAD
e_AF,gnomADe_AFR,gnomADe_AMR,gnomADe_ASJ,gnomADe_EAS,gnomADe_FIN,gnomADe_MID,gnomADe_NFE,gnomADe_REMAINING,gnomADe_SAS,gnomADg_AF,gnomADg_AFR,gnomADg_AMI,g
nomADg_AMR,gnomADg_ASJ,gnomADg_EAS,gnomADg_FIN,gnomADg_MID,gnomADg_NFE,gnomADg_REMAINING,gnomADg_SAS,MAX_AF,MAX_AF_POPS,CLIN_SIG,SOMATIC,PHENO,PUBMED"

4. Alternative: --everything mode (80 fields)

docker run --rm \
-v /Users/mwiewior/research/data/vep/homo_sapiens/115_GRCh38:/opt/vep/.vep/homo_sapiens/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything.vcf \
--vcf \
--force_overwrite \
--no_stats \
--everything

5. Optional: with HGVS (requires reference FASTA)

Add these flags and an extra mount:

docker run --rm \
-v /Users/mwiewior/research/data/vep/homo_sapiens/115_GRCh38:/opt/vep/.vep/homo_sapiens/115_GRCh38:ro \
-v /tmp/vep_work:/work \
-v /path/to/fasta/dir:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
... \
--hgvs \
--fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa

Notes

- No plugins are used in the benchmark — everything comes from the offline cache.
- The cache mount expects the VEP directory structure: the container looks for homo_sapiens/115_GRCh38 under --dir. Since your cache path already ends with
  homo_sapiens/115_GRCh38, it's mounted directly to that subpath.
- Output is in VCF format with annotations in the CSQ INFO field.
- The bcftools norm -m -both step is important — it decomposes multi-allelic sites so each record has a single ALT allele, matching how the benchmark
  compares results.