import polars as pl

from pibench.queries import BY_ID, QUERIES, panel_genes

L = pl.List(pl.String)


def frame(**cols):
    schema = {
        "chrom": pl.String,
        "start": pl.UInt32,
        "ref": pl.String,
        "alt": pl.String,
        "IMPACT": L,
        "Consequence": L,
        "SYMBOL": L,
        "CANONICAL": L,
        "MANE_SELECT": L,
        "SIFT": L,
        "PolyPhen": L,
        "Existing_variation": L,
        "CLIN_SIG": L,
        "MAX_AF": pl.Float32,
        "gnomADg_AF": pl.Float32,
        "CADD_PHRED": pl.String,
        "am_class": L,
        "ClinVar_CLNSIG": pl.String,
        **{f"SpliceAI_pred_DS_{k}": L for k in ("AG", "AL", "DG", "DL")},
    }
    n = len(next(iter(cols.values())))
    defaults = {
        "chrom": ["chr22"] * n,
        "start": list(range(1, n + 1)),
        "ref": ["A"] * n,
        "alt": ["G"] * n,
    }
    data = {c: cols.get(c, defaults.get(c, [None] * n)) for c in schema}
    return pl.DataFrame(data, schema=schema)


def kept(qid, df):
    return df.filter(BY_ID[qid].expr())["start"].to_list()


def test_catalogue_has_18_unique_ids():
    ids = [q.id for q in QUERIES]
    assert len(ids) == 18 and len(set(ids)) == 18


def test_every_query_names_the_columns_it_reads():
    for q in QUERIES:
        assert q.columns, q.id


def test_region_bounds_are_inclusive():
    df = frame(start=[19_999_999, 20_000_000, 25_000_000, 25_000_001], IMPACT=[[]] * 4)
    assert kept("R1", df) == [20_000_000, 25_000_000]


def test_region_without_pushdown_keeps_the_same_rows():
    df = frame(start=[19_999_999, 20_000_000, 25_000_001], IMPACT=[[]] * 3)
    q = BY_ID["R1"]
    assert df.filter(q.expr_no_pushdown())["start"].to_list() == [20_000_000]


def test_r2_100kb_window_bounds_are_inclusive():
    df = frame(start=[29_999_999, 30_000_000, 30_100_000, 30_100_001], IMPACT=[[]] * 4)
    assert kept("R2", df) == [30_000_000, 30_100_000]


def test_rare_keeps_missing_and_is_strict_at_the_threshold():
    # 0.01 parsed to Float32 must not count as < 0.01 (filter_vep compares text as double).
    df = frame(start=[1, 2, 3], MAX_AF=[None, 0.01, 0.009])
    assert kept("Q1", df) == [1, 3]


def test_gnomad_af_keeps_missing_and_is_strict_at_the_threshold():
    # Same Float32-vs-text-double boundary as Q1, on gnomADg_AF at 0.001.
    df = frame(start=[1, 2, 3], gnomADg_AF=[None, 0.001, 0.0009])
    assert kept("Q2", df) == [1, 3]


def test_novel_is_null_or_empty_existing_variation():
    df = frame(start=[1, 2, 3], Existing_variation=[None, [], ["rs1"]])
    assert kept("Q3", df) == [1, 2]


def test_clin_sig_match_is_case_insensitive_substring():
    df = frame(start=[1, 2, 3], CLIN_SIG=[["likely_pathogenic"], ["benign"], None])
    assert kept("Q4", df) == [1]


def test_impact_is_high_only():
    df = frame(start=[1, 2, 3], IMPACT=[["HIGH"], ["MODERATE"], ["LOW"]])
    assert kept("Q5", df) == [1]


def test_impact_is_high_or_moderate():
    df = frame(start=[1, 2, 3], IMPACT=[["HIGH"], ["MODERATE"], ["LOW"]])
    assert kept("Q6", df) == [1, 2]


def test_a_wholly_null_flag_list_does_not_veto_the_other_side_of_an_or():
    # MANE_SELECT is absent for the whole row (not just this entry); CANONICAL
    # alone must still carry Q7's and Q10's "canonical or MANE" leg.
    df = frame(
        start=[1],
        IMPACT=[["HIGH"]],
        Consequence=[["stop_gained"]],
        CANONICAL=[["YES"]],
        MANE_SELECT=None,
        MAX_AF=[None],
    )
    assert kept("Q7", df) == [1]
    assert kept("Q10", df) == [1]


def test_lof_needs_both_conditions_on_the_same_entry():
    # Variant 1: LoF on a non-canonical entry, canonical on a different entry -> not kept.
    # Variant 2: LoF and MANE on the same entry -> kept.
    df = frame(
        start=[1, 2],
        Consequence=[
            ["stop_gained", "intron_variant"],
            ["frameshift_variant&splice_region_variant"],
        ],
        CANONICAL=[[None, "YES"], [None]],
        MANE_SELECT=[[None, None], ["NM_1.1"]],
    )
    assert kept("Q7", df) == [2]


def test_splice_donor_does_not_match_the_5th_base_term():
    df = frame(
        start=[1],
        Consequence=[["splice_donor_5th_base_variant"]],
        CANONICAL=[["YES"]],
        MANE_SELECT=[[None]],
    )
    assert kept("Q7", df) == []


def test_damaging_missense_is_per_entry():
    df = frame(
        start=[1, 2],
        Consequence=[["missense_variant", "missense_variant"], ["missense_variant"]],
        SIFT=[
            ["deleterious(0.01)", "tolerated(0.3)"],
            ["deleterious_low_confidence(0.02)"],
        ],
        PolyPhen=[
            ["benign(0.1)", "probably_damaging(0.99)"],
            ["possibly_damaging(0.6)"],
        ],
    )
    assert kept("Q8", df) == [2]


def test_composite_combines_variant_and_entry_levels():
    df = frame(
        start=[1, 2, 3],
        IMPACT=[["HIGH"], ["MODERATE"], ["HIGH"]],
        CANONICAL=[["YES"], [None], ["YES"]],
        MANE_SELECT=[[None], [None], [None]],
        MAX_AF=[None, 0.001, 0.5],
    )
    assert kept("Q10", df) == [1]


def test_cadd_phred_is_numeric_on_a_string_column():
    df = frame(start=[1, 2, 3], CADD_PHRED=["25.1", "3.2", None])
    assert kept("P1", df) == [1]


def test_am_class_is_likely_pathogenic():
    df = frame(start=[1, 2, 3], am_class=[["likely_pathogenic"], ["benign"], None])
    assert kept("P2", df) == [1]


def test_spliceai_any_of_four_scores():
    df = frame(
        start=[1, 2],
        SpliceAI_pred_DS_AG=[["0.00"], ["0.10"]],
        SpliceAI_pred_DS_AL=[["0.00"], ["0.00"]],
        SpliceAI_pred_DS_DG=[["0.00"], ["0.00"]],
        SpliceAI_pred_DS_DL=[["0.50"], None],
    )
    assert kept("P3", df) == [1]


def test_clinvar_clnsig_match_is_case_insensitive_substring():
    # Same substring semantics as Q4/CLIN_SIG: "Conflicting_classifications_of_
    # pathogenicity" contains "pathogenicity", which contains "pathogenic" as a
    # substring, so filter_vep's `match` keeps it too.
    df = frame(
        start=[1, 2, 3],
        ClinVar_CLNSIG=[
            "Pathogenic",
            "Benign",
            "Conflicting_classifications_of_pathogenicity",
        ],
    )
    assert kept("P4", df) == [1, 3]


def test_p5_cadd_is_variant_level_and_am_is_per_entry():
    df = frame(
        start=[1, 2, 3],
        IMPACT=[["MODERATE", "MODIFIER"], ["MODERATE", "MODIFIER"], ["MODERATE"]],
        CANONICAL=[["YES", None], ["YES", None], [None]],
        MANE_SELECT=[[None, None], [None, None], [None]],
        MAX_AF=[None, None, None],
        CADD_PHRED=["30", None, "30"],
        am_class=[[None, None], [None, "likely_pathogenic"], [None]],
    )
    # 1: CADD high + canonical HIGH/MODERATE entry -> kept.
    # 2: AlphaMissense only on the non-canonical MODIFIER entry -> not kept.
    # 3: CADD high but no canonical/MANE entry -> not kept.
    assert kept("P5", df) == [1]


def test_panel_has_78_symbols_and_includes_nf2():
    genes = panel_genes()
    assert len(genes) == 78 and "NF2" in genes


def test_r3_is_the_nf2_locus():
    from pibench.queries import filter_vep_expression, panel_regions

    assert panel_regions() == [("chr22", 29_603_520, 29_698_598)]
    assert (
        filter_vep_expression(BY_ID["R3"])
        == "(CHROM is chr22 and POS >= 29603520 and POS <= 29698598)"
    )


def test_expr_reads_exactly_its_declared_columns():
    # R3 and Q9 build their expr from the panel files, which do not exist yet
    # (Task 5); every other query's expr must read exactly q.columns, no more
    # and no less.
    for q in QUERIES:
        if q.id in ("R3", "Q9"):
            continue
        assert set(q.expr().meta.root_names()) == set(q.columns), q.id
