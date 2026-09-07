process VEPYR_ANNOTATE {
    tag "${meta.id}"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    // TODO Replace both URIs once bioconda-recipes#68869 ships vepyr 0.6.0 and
    // the Seqera Wave image for this environment.yml has been built.
    container "${workflow.containerEngine in ['singularity', 'apptainer'] && !task.ext.singularity_pull_docker_container
        ? 'PLACEHOLDER_SINGULARITY_URI'
        : 'PLACEHOLDER_DOCKER_URI'}"

    input:
    tuple val(meta), path(vcf), path(tbi)
    tuple val(meta2), path(cache)
    tuple val(meta3), path(fasta)
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
    def reference = fasta ? "--fasta ${fasta}" : ''
    def version_arg = cache_version ? "--cache_version ${cache_version}" : ''
    def plugin_arg = plugin_cache ? "--plugin_cache_root ${plugin_cache}" : ''
    // --fork above 1 requires a tabix/CSI index on the input; vepyr raises
    // without one. Fall back to a single pipeline so a missing index costs
    // throughput rather than failing the task.
    def fork = tbi ? task.cpus : 1
    """
    vepyr annotate \\
        -i ${vcf} \\
        -o ${prefix}.vcf.gz \\
        --dir_cache ${cache} \\
        ${reference} \\
        ${version_arg} \\
        ${plugin_arg} \\
        --fork ${fork} \\
        --no_progress \\
        ${args}

    tabix ${args2} ${prefix}.vcf.gz
    """

    stub:
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo "" | gzip > ${prefix}.vcf.gz
    touch ${prefix}.vcf.gz.tbi
    """
}
