// HG002 annotation through the nf-core vepyr/annotate module, with the same
// workspace inputs, caches and Ensembl VEP references as
// scripts/run_comparison.py (see e2e-testing/README.md).
//
// Per contig: slice the normalized input (as vcfio.slice_contig does), annotate
// it with VEPYR_ANNOTATE, then compare the record-body md5 against the same
// contig of the VEP reference -- the strict digest md5_concordance.py computes.
//
//   cd e2e-testing/nextflow && nextflow run main.nf --release 116 --profile merged --chroms chr21,chr22

include { VEPYR_ANNOTATE } from '../../nf-core-module/modules/nf-core/vepyr/annotate/main'

// profile -> cache flavour, VEP reference and plugins, mirroring
// scripts/comparison/profiles.py. Every profile `vepyr annotate` can express is
// listed; the pick modes are not, because the CLI does not implement them.
def profileSpec(profile) {
    def specs = [
        merged: [
            flavour: 'merged',
            vep: 'HG002_annotated_wgs_everything_hgvs_merged',
            plugins: [],
            perContig: null,
        ],
        ensembl: [
            flavour: 'ensembl',
            vep: 'HG002_annotated_wgs_everything_hgvs_vep',
            plugins: [],
            perContig: null,
        ],
        refseq: [
            flavour: 'refseq',
            vep: 'HG002_annotated_wgs_everything_hgvs_refseq',
            plugins: [],
            perContig: null,
        ],
        // Plugin order is the CSQ block order of the VEP reference.
        merged_plugins: [
            flavour: 'merged',
            vep: 'HG002_annotated_wgs_everything_hgvs_merged_clinvar_spliceai_cadd_am_dbnsfp',
            plugins: ['spliceai', 'cadd', 'alphamissense', 'dbnsfp', 'clinvar'],
            perContig: 'plugins/HG002_{chrom}_5plugins_vep116_caddfix',
        ],
        merged_phenotypeorthologous: [
            flavour: 'merged',
            vep: 'HG002_annotated_wgs_everything_hgvs_merged_phenotypeorthologous',
            plugins: ['phenotypeorthologous'],
            perContig: 'plugins/HG002_{chrom}_phenotypeorthologous_vep116',
        ],
    ]
    if (!specs.containsKey(profile)) {
        error "Unknown --profile '${profile}'. Supported: ${specs.keySet().join(', ')}"
    }
    return specs[profile]
}

def releaseDir(release) {
    def dirs = ['115': '115.2', '116': '116']
    if (!dirs.containsKey(release)) {
        error "Unknown --release '${release}'. Supported: ${dirs.keySet().join(', ')}"
    }
    return dirs[release]
}

// $DATA/<subdir>/<name>, falling back to the legacy $DATA/<name> like profiles.py.
def dataPath(subdir, name) {
    def preferred = file("${params.data_dir}/${subdir}/${name}")
    def legacy = file("${params.data_dir}/${name}")
    return (!preferred.exists() && legacy.exists()) ? legacy : preferred
}

// The contig's own reference when the profile has per-contig files, else the
// whole-genome one -- profiles.vep_vcf_for.
def vepReference(spec, release, chrom) {
    def outputDir = "${params.data_dir}/output/${releaseDir(release)}"
    if (spec.perContig) {
        def perContig = file("${outputDir}/${spec.perContig.replace('{chrom}', chrom)}.vcf.gz")
        if (perContig.exists()) {
            return perContig
        }
    }
    return file("${outputDir}/${spec.vep}.vcf.gz")
}

def existing(path, what) {
    def f = file(path)
    if (!f.exists()) {
        error "${what} not found: ${f}"
    }
    return f
}

process SLICE_CONTIG {
    tag "${chrom}"
    label 'process_single'

    input:
    tuple val(chrom), path(vcf), path(tbi)

    output:
    tuple val(chrom), path("input_${chrom}.vcf.gz"), path("input_${chrom}.vcf.gz.tbi")

    script:
    """
    { tabix -H ${vcf}; tabix ${vcf} ${chrom}; } | bgzip -c > input_${chrom}.vcf.gz
    tabix -p vcf input_${chrom}.vcf.gz
    """
}

process COMPARE_MD5 {
    tag "${chrom}"
    label 'process_single'

    input:
    tuple val(chrom), path(annotated), path(vep_vcf), path(vep_tbi)

    output:
    path "${chrom}.md5.tsv"

    script:
    """
    # One pass per stream: md5 over the record lines as written, and their count.
    body_digest() {
        python -c 'import hashlib, sys
    md5, n = hashlib.md5(), 0
    for line in sys.stdin.buffer:
        md5.update(line)
        n += 1
    print(md5.hexdigest(), n)'
    }
    read -r vepyr_md5 vepyr_records < <(bgzip -dc ${annotated} | grep -v '^#' | body_digest)
    read -r vep_md5 vep_records < <(tabix ${vep_vcf} ${chrom} | body_digest)
    if [ "\$vepyr_md5" = "\$vep_md5" ] && [ "\$vepyr_records" = "\$vep_records" ]; then
        status=MATCH
    else
        status=DIFFER
    fi
    printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \\
        ${chrom} "\$vepyr_records" "\$vep_records" "\$vepyr_md5" "\$vep_md5" "\$status" \\
        > ${chrom}.md5.tsv
    """
}

workflow {
    main:
    def release = params.release.toString()
    def profile = params.profile.toString()
    def spec = profileSpec(profile)

    def shared = file("${projectDir}/../results/${release}/_shared/normalized.vcf.gz")
    def vcfPath = params.vcf ?: (shared.exists() ? shared : dataPath('input', 'HG002_normalized.vcf.gz'))
    def fastaPath = params.fasta ?: dataPath('input', 'Homo_sapiens.GRCh38.dna.primary_assembly.fa')
    def cachePath = params.cache ?: dataPath('cache', "${release}_GRCh38_${spec.flavour}")

    def inputVcf = existing(vcfPath, 'Normalized input VCF (docs/testing-vep.md steps 1-3)')
    def inputTbi = existing("${vcfPath}.tbi", 'Input VCF index')
    def fasta = existing(fastaPath, 'Reference FASTA')
    def fai = existing("${fastaPath}.fai", 'Reference FASTA index')
    // A bgzip FASTA opens only with its .gzi, so it rides in the index slot with
    // the .fai; Nextflow stages only declared paths into the task directory.
    def fastaIndex = fastaPath.toString().endsWith('.gz')
        ? [ fai, existing("${fastaPath}.gzi", 'Reference FASTA bgzip index') ]
        : fai
    def cacheDir = existing(cachePath, 'Parquet cache')

    // Resolved to the real directory: the workspace's plugin_cache_<release> is
    // a relative symlink, and a container sees only the staged path, not its target.
    def pluginCache = spec.plugins
        ? existing(params.plugin_cache ?: dataPath('cache', "plugin_cache_${release}"), 'Plugin cache').toRealPath()
        : null

    def chroms = params.chroms.toString() == 'all'
        ? (1..22).collect { n -> "chr${n}".toString() }
        : params.chroms.toString().tokenize(',').collect { c -> c.trim().startsWith('chr') ? c.trim() : "chr${c.trim()}".toString() }

    log.info """
        vepyr HG002 via nf-core vepyr/annotate
          profile   : ${profile}
          release   : ${release}
          contigs   : ${chroms.join(', ')}
          input vcf : ${inputVcf}
          cache     : ${cacheDir}
          plugins   : ${spec.plugins ? "${spec.plugins.join(', ')} from ${pluginCache}" : '(none)'}
          fasta     : ${fasta}
          vep ref   : ${params.compare ? (params.vep ?: vepReference(spec, release, chroms[0])) : '(comparison off)'}${params.compare && chroms.size() > 1 && spec.perContig && !params.vep ? ' (per contig)' : ''}
          outdir    : ${params.outdir}/${release}/nextflow_${profile}
        """.stripIndent()

    SLICE_CONTIG(channel.fromList(chroms).map { chrom -> tuple(chrom, inputVcf, inputTbi) })

    VEPYR_ANNOTATE(
        // meta.plugins becomes --plugin flags in nextflow.config's ext.args.
        SLICE_CONTIG.out.map { chrom, vcf, tbi -> tuple([ id: chrom, plugins: spec.plugins ], vcf, tbi) },
        channel.value(tuple([ id: cacheDir.name ], cacheDir)),
        channel.value(tuple([ id: 'GRCh38' ], fasta, fastaIndex)),
        release.toInteger(),
        pluginCache ? tuple([ id: pluginCache.name ], pluginCache) : [[], []]
    )

    if (params.compare) {
        COMPARE_MD5(
            VEPYR_ANNOTATE.out.vcf.map { meta, annotated ->
                def reference = existing(params.vep ?: vepReference(spec, release, meta.id), "VEP reference for ${meta.id}")
                tuple(meta.id, annotated, reference, existing("${reference}.tbi", 'VEP reference index'))
            }
        )

        COMPARE_MD5.out
            .collectFile(
                name: 'md5_summary.tsv',
                storeDir: "${params.outdir}/${release}/nextflow_${profile}",
                seed: "chrom\tvepyr_records\tvep_records\tvepyr_body_md5\tvep_body_md5\tstatus\n",
                sort: { f -> f.name.replace('chr', '').replace('.md5.tsv', '').padLeft(3, '0') },
            )
            .subscribe { summary ->
                log.info "md5 summary (${summary}):\n" + summary.text
                def differing = summary.readLines().drop(1).findAll { row -> row.endsWith('DIFFER') }.collect { row -> row.tokenize('\t')[0] }
                if (differing) {
                    error "Body md5 differs from Ensembl VEP on: ${differing.join(', ')}"
                }
            }
    }
}
