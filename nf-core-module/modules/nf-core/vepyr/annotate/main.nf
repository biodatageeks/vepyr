process VEPYR_ANNOTATE {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    // TODO Replace both URIs once bioconda-recipes#69191 ships vepyr 0.7.0 and
    // the Seqera Wave image for this environment.yml has been built.
    container "${workflow.containerEngine in ['singularity', 'apptainer'] && !task.ext.singularity_pull_docker_container
        ? 'PLACEHOLDER_SINGULARITY_URI'
        : 'PLACEHOLDER_DOCKER_URI'}"

    input:
    tuple val(meta), path(vcf), path(tbi)
    tuple val(meta2), path(cache)
    tuple val(meta3), path(fasta), path(fai)
    val cache_version
    tuple val(meta4), path(plugin_cache)

    output:
    tuple val(meta), path("${prefix}.vcf.gz"), emit: vcf
    tuple val(meta), path("${prefix}.vcf.gz.tbi"), emit: tbi
    tuple val("${task.process}"), val('vepyr'), eval("vepyr --version | cut -d' ' -f2"), topic: versions, emit: versions_vepyr
    tuple val("${task.process}"), val('tabix'), eval("tabix -h 2>&1 | grep -oP 'Version:\\s*\\K[^\\s]+'"), topic: versions, emit: versions_tabix

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def args2 = task.ext.args2 ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    // vepyr opens the reference through its .fai and does not build one, so the
    // index must be staged alongside the FASTA or --everything/--hgvsc fail. A
    // bgzip FASTA also needs its .gzi: pass [ fai, gzi ] in the fai slot.
    def reference = fasta ? "--fasta ${fasta}" : ''
    def version_arg = cache_version ? "--cache_version ${cache_version}" : ''
    def plugin_arg = plugin_cache ? "--plugin_cache_root ${plugin_cache}" : ''
    // --fork above 1 requires a tabix/CSI index on the input; vepyr raises
    // without one. Fall back to a single pipeline so a missing index costs
    // throughput rather than failing the task.
    //
    // The flag is emitted *after* ${args} rather than before it, the one place
    // this module overrides the user: --fork and --workers are a single
    // argparse option, so the last occurrence wins, and an ext.args carrying
    // either spelling would silently defeat the fallback and fail the task.
    // Parallelism belongs to the cpus directive here.
    def fork = tbi ? task.cpus : 1
    """
    vepyr annotate \\
        -i ${vcf} \\
        -o ${prefix}.vcf.gz \\
        --dir_cache ${cache} \\
        ${reference} \\
        ${version_arg} \\
        ${plugin_arg} \\
        ${args} \\
        --fork ${fork} \\
        --no_progress

    tabix ${args2} ${prefix}.vcf.gz
    """

    stub:
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo "" | gzip > ${prefix}.vcf.gz
    touch ${prefix}.vcf.gz.tbi
    """
}
