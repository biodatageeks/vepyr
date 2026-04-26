
### VEP cache + everything + hgvs
```bash
time docker run --rm \
-v /Users/mwiewior/research/data/vep/homo_sapiens/115_GRCh38:/opt/vep/.vep/homo_sapiens/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/research/data/vep:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs.vcf \
--vcf \
--force_overwrite \
--no_stats \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

### Merged cache + everything + hgvs
```bash
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged.vcf \
--vcf \
--force_overwrite \
--no_stats \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```
#### Warnings
```bash
WARNING: Transcript-assembly mismatch in chr1_16053748_A/G
WARNING: Transcript-assembly mismatch in chr1_22893000_T/C
WARNING: Transcript-assembly mismatch in chr1_25317062_T/C
WARNING: Transcript-assembly mismatch in chr1_155324483_A/G
WARNING: Transcript-assembly mismatch in chr2_9904885_C/T
WARNING: Transcript-assembly mismatch in chr2_73385904_T/TGGA
WARNING: Transcript-assembly mismatch in chr2_115768384_G/A
WARNING: Transcript-assembly mismatch in chr2_219248520_T/G
WARNING: Transcript-assembly mismatch in chr3_4725578_T/C
WARNING: Transcript-assembly mismatch in chr3_4775373_T/C
WARNING: Transcript-assembly mismatch in chr3_4814496_T/C
WARNING: Transcript-assembly mismatch in chr3_38698083_T/C
WARNING: Transcript-assembly mismatch in chr3_38722372_G/C
WARNING: Transcript-assembly mismatch in chr3_38726809_T/C
WARNING: Transcript-assembly mismatch in chr3_114139503_C/T
WARNING: Transcript-assembly mismatch in chr3_114171968_C/T
WARNING: Transcript-assembly mismatch in chr3_184319745_A/G
WARNING: Transcript-assembly mismatch in chr3_184321878_A/G
WARNING: Transcript-assembly mismatch in chr5_120686122_C/A
WARNING: Transcript-assembly mismatch in chr5_120686124_G/A
WARNING: Transcript-assembly mismatch in chr6_18138983_G/A
WARNING: Transcript-assembly mismatch in chr8_17628773_T/C
WARNING: Transcript-assembly mismatch in chr8_17642714_T/C
WARNING: Transcript-assembly mismatch in chr8_39223113_C/G
WARNING: Transcript-assembly mismatch in chr9_33798019_A/G
WARNING: Transcript-assembly mismatch in chr9_127868360_A/G
WARNING: Transcript-assembly mismatch in chr9_128473042_A/G
WARNING: Transcript-assembly mismatch in chr10_48174883_G/T
WARNING: Transcript-assembly mismatch in chr10_48180858_C/T
WARNING: Transcript-assembly mismatch in chr10_102399439_A/G
WARNING: Transcript-assembly mismatch in chr10_102400677_A/G
WARNING: Transcript-assembly mismatch in chr11_64243969_T/C
WARNING: Transcript-assembly mismatch in chr11_89400281_C/T
WARNING: Transcript-assembly mismatch in chr11_89490933_G/T
WARNING: Transcript-assembly mismatch in chr12_56236660_G/C
WARNING: Transcript-assembly mismatch in chr13_30462544_A/G
WARNING: Transcript-assembly mismatch in chr14_59596754_A/G
WARNING: Transcript-assembly mismatch in chr16_29697029_A/G
WARNING: Transcript-assembly mismatch in chr17_7101608_C/T
WARNING: Transcript-assembly mismatch in chr19_5844526_A/G
WARNING: Transcript-assembly mismatch in chr19_53805300_C/T
WARNING: Transcript-assembly mismatch in chr20_35496586_C/G
WARNING: Transcript-assembly mismatch in chr20_62306724_T/C
```


### Merged cache + pick-mode references

These commands generate the VEP reference files used by the e2e pick-mode
profiles. All modes use the same merged VEP 115 cache, normalized HG002 input,
`--everything --hgvs`, and pick ranking order:
`biotype,rank,mane_select,tsl,canonical,appris,ccds,length`.

#### `merged_pick_filter` e2e mode: `--pick`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_pick_filter.vcf \
--vcf \
--force_overwrite \
--no_stats \
--pick --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_pick_allele` e2e mode: `--pick_allele`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_pick_allele.vcf \
--vcf \
--force_overwrite \
--no_stats \
--pick_allele --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_per_gene` e2e mode: `--per_gene`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_per_gene.vcf \
--vcf \
--force_overwrite \
--no_stats \
--per_gene --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_pick_allele_gene` e2e mode: `--pick_allele_gene`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_pick_allele_gene.vcf \
--vcf \
--force_overwrite \
--no_stats \
--pick_allele_gene --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_flag_pick` e2e mode: `--flag_pick`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_flag_pick.vcf \
--vcf \
--force_overwrite \
--no_stats \
--flag_pick --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_flag_pick_allele` e2e mode: `--flag_pick_allele`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele.vcf \
--vcf \
--force_overwrite \
--no_stats \
--flag_pick_allele --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### `merged_flag_pick_allele_gene` e2e mode: `--flag_pick_allele_gene`
```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_flag_pick_allele_gene.vcf \
--vcf \
--force_overwrite \
--no_stats \
--flag_pick_allele_gene --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### Legacy `merged_pick` e2e mode: `--flag_pick_allele_gene`

The historical `merged_pick` profile is kept as an alias for the original
`--flag_pick_allele_gene` comparison output.

```shell
time docker run --rm \
-v /Users/mwiewior/workspace/data_vepyr/homo_sapiens_merged/115_GRCh38:/opt/vep/.vep/homo_sapiens_merged/115_GRCh38:ro \
-v /Users/mwiewior/research/git/vepyr/sandbox:/work \
-v /Users/mwiewior/workspace/data_vepyr:/fasta:ro \
ensemblorg/ensembl-vep:release_115.2 \
vep \
--dir /opt/vep/.vep \
--cache \
--merged \
--offline \
--assembly GRCh38 \
--input_file /work/HG002_normalized.vcf \
--output_file /work/HG002_annotated_wgs_everything_hgvs_merged_pick.vcf \
--vcf \
--force_overwrite \
--no_stats \
--flag_pick_allele_gene --pick_order biotype,rank,mane_select,tsl,canonical,appris,ccds,length \
--everything --hgvs --fasta /fasta/Homo_sapiens.GRCh38.dna.primary_assembly.fa
```

#### Time
2026-04-11 06:00:51 - INFO: BAM-edited cache detected, enabling --use_transcript_ref; use --use_given_ref to override this
docker run --rm -v  -v /Users/mwiewior/research/git/vepyr/sandbox:/work -v     0.99s user 0.48s system 0% cpu 7:17:55.06 total
