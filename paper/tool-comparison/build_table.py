"""Comparison of Ensembl VEP, fastVEP, vep-rs and vepyr: tables and panel.

One data source, three outputs:

* the compact panel for Figure 1 (``panel-tools.*``, tools as columns;
  ``panel-tools-nonotes.*`` without the footnotes), typeset with XeLaTeX in
  Arial to match the rest of the figure and exported as PDF, PNG and SVG;
* ``tab-detail.tex``: the full table for the supplement, every attribute
  with the evidence behind the compact cells;
* ``tool-comparison.pdf``: the working document that shows both, the
  footnotes, the sources and the notes.

The cells for fastVEP and vep-rs come from their preprints and repositories
at the commits listed in ``tool-comparison.tex``; field counts are taken
from their source code and documentation, not from running them.

    python build_table.py          # write the .tex fragments
    python build_table.py --pdf    # ... and build panels and document
"""

import argparse
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

TOOLS = ["VEP", "fastVEP", "vep-rs", "vepyr"]

# (name, release, git commit of the code described). fastVEP is described at
# its default branch, six commits after the v0.4.0 tag (d728c46); VEP is the
# ensembl-vep release/116.0 tag (plugins: VEP_plugins release/116, 59b34e5).
HEAD = {
    "VEP": ("Ensembl VEP", "116.0", "57ea5c5"),
    "fastVEP": ("fastVEP", "0.4.0+6", "5d8004a"),
    "vep-rs": ("vep-rs", "0.4.0", "f642723"),
    "vepyr": ("vepyr", "0.10.0", "3f21520"),
}

# ---------------------------------------------------------------------------
# Compact cells: Figure 1 panel. Superscripts refer to PANEL_NOTES.
# ---------------------------------------------------------------------------

COMPACT_ROWS = [
    ("lang", "Language"),
    ("parallel", "Parallelism"),
    ("cache", "Cache format"),
    ("fields", r"Fields filled\textsuperscript{a}"),
    ("transcripts", "Transcript sets"),
    ("variants", "Declared variant classes"),
    ("plugins", "External annotation sources"),
    ("concordance", "Concordance (chr22, merged)"),
    ("ui", "Interfaces"),
    ("evaluation", "Evaluation"),
    ("output", "Output"),
]

TODO = r"\textcolor{todo}{\textbf{TODO}}"

COMPACT = {
    "VEP": {
        "lang": "Perl",
        "parallel": "processes",
        "cache": "Storable, gzip per 1-Mb region",
        "fields": "80/80",
        "transcripts": "Ensembl, RefSeq, merged",
        "variants": "SNV, indel, SV",
        "plugins": "yes",
        "concordance": "reference",
        "ui": "CLI, web, REST",
        "evaluation": "streaming",
        "output": "VCF, tab, JSON",
    },
    "fastVEP": {
        "lang": "Rust",
        "parallel": "threads",
        "cache": "bincode + zstd (transcripts), OSAv2 (annotations)",
        "fields": "≤37/80",
        "transcripts": r"Ensembl, RefSeq, merged\textsuperscript{c}",
        "variants": r"SNV, indel, SV\textsuperscript{e}",
        "plugins": "yes",
        "concordance": TODO,
        "ui": "CLI, web, REST",
        "evaluation": "streaming",
        "output": "VCF, tab, JSON",
    },
    "vep-rs": {
        "lang": "Rust",
        "parallel": "threads",
        "cache": "JSON, gzip optional",
        "fields": r"≤71/80 (34/80)\textsuperscript{b}",
        "transcripts": r"Ensembl, RefSeq, merged\textsuperscript{d}",
        "variants": r"SNV, indel, SV\textsuperscript{e}",
        "plugins": "yes",
        "concordance": TODO,
        "ui": "CLI",
        "evaluation": "streaming",
        "output": "VCF, tab, JSON, Parquet",
    },
    "vepyr": {
        "lang": "Rust, Python",
        "parallel": "threads",
        "cache": "Parquet, ZSTD (core and plugins)",
        "fields": r"\textbf{80/80}",
        "transcripts": "Ensembl, RefSeq, merged",
        "variants": "SNV, indel",
        "plugins": "yes",
        "concordance": TODO,
        "ui": "Python API, SQL, CLI",
        "evaluation": r"\textbf{lazy, \mbox{pushdown}}",
        "output": r"Polars DataFrame/\allowbreak LazyFrame, VCF",
    },
}

PANEL_SMALL = r"\fontsize{5.8}{6.6}\selectfont"

PANEL_NOTES = [
    ("a", r"Of the 80 \texttt{CSQ} fields of VEP~116 \texttt{--everything} (Ensembl cache), "
          r"those the tool can fill, counted from its source code and documentation."),
    ("b", r"34/80 on the cache converted from VEP, used for all concordance results "
          r"(no known variants, SIFT or PolyPhen)."),
    ("c", r"From Ensembl and RefSeq GFF3 files (merged: both, with \texttt{SOURCE}); "
          r"RefSeq-specific fields are not written."),
    ("d", r"Through a cache converted from the VEP RefSeq or merged cache; "
          r"\texttt{SOURCE} and the RefSeq fields are not written."),
    ("e", r"As declared by the authors; SV annotation not fully validated against VEP."),
]

# ---------------------------------------------------------------------------
# Detailed cells: supplementary table.
# ---------------------------------------------------------------------------

DETAIL_ROWS = [
    ("parallel", "Parallelism"),
    ("lang", "Implementation language"),
    ("cache", "Cache format and source"),
    ("coverage", r"Cache coverage (\texttt{CSQ} fields of \texttt{--everything})"),
    ("scope", "Functional scope"),
    ("concordance", "Concordance with VEP: published evidence and metric"),
    ("chr22", r"Concordance on chr22, merged cache"),
    ("ui", "User interfaces"),
    ("lazy", "Lazy evaluation and streaming"),
    ("output", "Output formats"),
    ("transcripts", "Transcript sets"),
    ("selection", r"Transcript selection (\texttt{--pick} etc.)"),
    ("extension", "External annotation sources"),
    ("filtering", r"Filtering of results (\texttt{filter\_vep})"),
    ("distribution", "Distribution"),
    ("species", "Species, assemblies, VEP releases"),
    ("license", "Licence"),
]

DETAIL = {
    "VEP": {
        "parallel": r"multi-process (\texttt{--fork})",
        "lang": r"Perl",
        "cache": r"official cache: Storable/Sereal \texttt{.gz} files per 1-Mb region; optional tabix-indexed known variants",
        "coverage": r"reference: 80, 85 and 86 fields (\texttt{ensembl}, \texttt{refseq} and \texttt{merged} caches, release 116)",
        "scope": r"SNVs, indels, SVs and CNVs; regulatory and motif features; all \texttt{--everything} options",
        "chr22": r"reference",
        "concordance": r"reference implementation",
        "ui": r"CLI; web interface; REST API",
        "lazy": r"streaming in buffers (\texttt{--buffer\_size}, default 5,000); no lazy evaluation or pushdown",
        "output": r"VEP default (tab-delimited), VCF (\texttt{CSQ}), tab, JSON; HTML statistics",
        "transcripts": r"Ensembl, RefSeq, merged; GFF/GTF",
        "selection": r"\texttt{--pick}, \texttt{--per\_gene}, \texttt{--flag\_pick}, \texttt{--most\_severe} and others",
        "extension": r"83 Perl plugin modules (\texttt{VEP\_plugins}, release 116); \texttt{--custom} (VCF, BED, GFF/GTF, bigWig)",
        "filtering": r"\texttt{filter\_vep} (own filter language)",
        "distribution": r"GitHub (\texttt{INSTALL.pl}), Docker, Singularity, Bioconda; nf-core module",
        "species": r"all Ensembl species; GRCh37 and GRCh38",
        "license": r"Apache-2.0",
    },
    "fastVEP": {
        "parallel": r"multi-threaded (rayon, all cores, no thread option); batches of 1,024 records; known-variant lookup and output sequential",
        "lang": r"Rust; web interface in JavaScript",
        "cache": r"transcripts: own binary cache (bincode + zstd; 388\,MB for GRCh38) built from GFF3; annotations (fastSA): OSAv2 (\texttt{.osa2}, zstd-compressed chunks; v1 \texttt{.osa} still read); VEP cache read only for known variants (\texttt{all\_vars.gz})",
        "coverage": r"49 \texttt{CSQ} fields, 44 of the 80; at most 37/80 filled (motif fields have no writer, SIFT and PolyPhen always empty); no per-population frequencies, UniProt, \texttt{DOMAINS}, \texttt{VARIANT\_CLASS}",
        "scope": r"SNVs, indels, MNVs, SVs (DEL, DUP, INV, CNV, BND, INS, STR); mitochondria; ACMG classification; \texttt{--everything} accepted without effect; regulatory annotation not connected",
        "chr22": TODO,
        "concordance": r"exact match of 22 of 24 fields on 2,920 transcript--allele pairs of the 173-variant VEP example (VEP 115.1, \texttt{--gff} mode); consequences and HGVS 99.49--100\% on ClinVar (6,600 variants) and HG002 (1 in 200) samples; pairs present in one tool only not scored; 9 deliberate divergences; SVs not compared",
        "ui": r"CLI; web interface and REST API (\texttt{fastvep-web}); release binaries (Linux x86-64, macOS arm64)",
        "lazy": r"streaming in batches of 1,024 records; whole transcript set loaded (subset if the GFF3 is indexed); no region selection or pushdown",
        "output": r"VCF (\texttt{CSQ} and \texttt{FV\_*} fields), tab (17 columns), JSON (VEP and Nirvana style)",
        "transcripts": r"Ensembl, RefSeq or merged from GFF3 files (\texttt{--gff3} repeated, per-transcript \texttt{SOURCE}); RefSeq-specific fields (\texttt{REFSEQ\_MATCH}, \texttt{BAM\_EDIT}, \ldots) not written",
        "selection": r"\texttt{--pick} family (since 0.4.0)",
        "extension": r"fastSA: 18 built-in sources (ClinVar, gnomAD, dbNSFP, SpliceAI, AlphaMissense, REVEL, \ldots) and custom VCF/BED; no VEP plugins",
        "filtering": r"\texttt{fastvep filter}",
        "distribution": r"GitHub releases; conda recipe not on Bioconda; no container image",
        "species": r"any organism with GFF3 and FASTA (5 benchmarked); GRCh37, GRCh38",
        "license": r"Apache-2.0",
    },
    "vep-rs": {
        "parallel": r"multi-threaded (rayon; \texttt{--fork} sets threads, default one per CPU up to 32); bounded reader--annotator--writer pipeline",
        "lang": r"Rust",
        "cache": r"own JSON cache, one file per chromosome and 1-Mb region (\texttt{.json}; \texttt{.json.gz} also read), converted from the VEP Storable cache by a Perl script or built from Ensembl GFF3, GTF and FASTA; plugin data in memory-mapped \texttt{.vpd} files",
        "coverage": r"the 80 fields in the header; 9 always empty (motif, \texttt{DOMAINS}, \texttt{miRNA}, \texttt{UNIPARC}, \texttt{UNIPROT\_ISOFORM}): at most 71/80; converted cache has no known variants or SIFT/PolyPhen: 34/80",
        "scope": r"SNVs, indels, MNVs, SVs (DEL, INS, DUP, INV, CNV, BND, mobile elements); mitochondria; no regulatory annotation; \texttt{--custom}, \texttt{--mirna}, \texttt{--hgvsg} accepted without effect",
        "chr22": TODO,
        "concordance": r"F1 over (location, allele, feature, consequence set) tuples, default options: $\ge$0.999974 on $2.6\times10^{8}$ tuples (VEP 115.2, preprint), 0.9999984 on $1.2\times10^{10}$ tuples (VEP 116.2, repository); all formats field for field on test corpora; SVs: F1 0.90--0.98 on chromosome~21 sets, no field comparison; not byte-identical",
        "ui": r"CLI with VEP option names (\texttt{--json\_cache} required); Docker image (amd64)",
        "lazy": r"streaming in buffers through a bounded pipeline; transcripts loaded per chromosome, known variants loaded in full; no region selection or pushdown",
        "output": r"VEP default, tab, VCF, JSON; Parquet through an external DuckDB process",
        "transcripts": r"Ensembl; RefSeq and merged through a cache converted from the corresponding VEP cache (\texttt{--refseq} and \texttt{--merged} have no effect), but \texttt{SOURCE} and the RefSeq fields (\texttt{REFSEQ\_MATCH}, \texttt{BAM\_EDIT}, \ldots) are not written and the RefSeq BAM-edit correction is not implemented (open items of the project's parity matrix); no RefSeq concordance reported",
        "selection": r"\texttt{--pick} family; \texttt{--pick\_order} accepted without effect",
        "extension": r"11 built-in plugins (CADD, REVEL, AlphaMissense, SpliceAI, LoFTEE, \ldots); C-ABI shared-library plugins; no \texttt{--custom}",
        "filtering": r"none",
        "distribution": r"GitHub releases; Docker (ghcr.io)",
        "species": r"\textit{Homo sapiens} GRCh37 and GRCh38; VEP 115.2 (preprint), 116.2 (repository)",
        "license": r"Apache-2.0",
    },
    "vepyr": {
        "parallel": r"multi-threaded workers over index-defined position windows; output independent of the number of workers",
        "lang": r"Rust engine (DataFusion, Arrow); Python API (PyO3)",
        "cache": r"Apache Parquet (ZSTD), partitioned by chromosome and entity, with page indexes; converted from the official VEP cache; prebuilt caches on Hugging Face with pinned revisions and manifests",
        "coverage": r"all: 80/80, 85/85, 86/86 fields; 42 plugin fields",
        "scope": r"SNVs and short indels; regulatory and motif features; \texttt{--everything} always on; no SVs",
        "chr22": TODO,
        "concordance": r"strict md5 of all record lines identical to VEP~116.0 on HG002 chr1--22 (4,096,123 records): 3 cache types, 5 plugins + ClinVar, 7 transcript-selection profiles (chr1, chr22); 205 tests ported from VEP",
        "ui": r"Python API (Polars) and SQL; CLI with VEP option names; nf-core module and subworkflow",
        "lazy": r"lazy Polars source: projection pushdown (optional annotation work), region pushdown through the VCF index, row limit; streaming sinks",
        "output": r"Polars \texttt{LazyFrame}/\texttt{DataFrame} with typed columns; VCF (\texttt{CSQ}; plain, gzip, BGZF); Parquet and other formats through Polars sinks",
        "transcripts": r"Ensembl, RefSeq, merged",
        "selection": r"\texttt{pick}, \texttt{pick\_allele}, \texttt{per\_gene}, \texttt{pick\_allele\_gene}, 3 \texttt{flag\_pick} variants",
        "extension": r"declarative TOML manifests (TSV, CSV, Parquet, VCF, BED; SQL ingestion): CADD, SpliceAI, AlphaMissense, dbNSFP, PhenotypeOrthologous, ClinVar; no executable plugins",
        "filtering": r"Polars expressions or SQL; 18 \texttt{filter\_vep} queries reproduced exactly",
        "distribution": r"PyPI wheels (Linux, macOS, Windows), Bioconda, nf-core",
        "species": r"\textit{Homo sapiens} GRCh38; VEP 115.2 and 116.0",
        "license": r"Apache-2.0",
    },
}


def tool_head(tool: str, bold: bool = False, small: str = r"\footnotesize") -> str:
    name, version, sha = HEAD[tool]
    name = r"\textbf{%s}" % name if bold or tool == "vepyr" else name
    return r"%s\newline{%s\color{muted}%s\enspace\texttt{%s}}" % (name, small, version, sha)


def compact_table(label_w: float = 2.45, col_w: float = 2.55) -> str:
    """Tools as columns; rows are the compact attributes (widths in cm)."""
    spec = (
        r"@{}>{\raggedright\arraybackslash}p{%.2fcm}" % label_w
        + "".join(
            r">{%s\raggedright\arraybackslash}p{%.2fcm}"
            % (r"\columncolor{hl}" if t == "vepyr" else "", col_w)
            for t in TOOLS
        )
        + "@{}"
    )
    lines = [
        r"\begin{tabular}{%s}" % spec,
        r"\toprule",
        " & " + " & ".join(tool_head(t, small=PANEL_SMALL) for t in TOOLS) + r" \\",
        r"\midrule",
    ]
    for key, label in COMPACT_ROWS:
        lines.append(
            r"\textcolor{muted}{%s} & " % label
            + " & ".join(COMPACT[t][key] for t in TOOLS)
            + r" \\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    return "\n".join(lines) + "\n"


def panel_notes() -> str:
    return " ".join(r"\textsuperscript{%s}%s" % (k, v) for k, v in PANEL_NOTES) + "\n"


def detail_table() -> str:
    """Supplementary table: tools as columns, all attributes."""
    head = r"\textbf{Attribute} & " + " & ".join(tool_head(t, bold=True) for t in TOOLS) + r" \\"
    lines = [
        r"\begin{longtable}{@{}>{\raggedright\arraybackslash}p{2.2cm}"
        + r">{\raggedright\arraybackslash}p{3.55cm}" * len(TOOLS)
        + r"@{}}",
        r"\toprule", head, r"\midrule", r"\endfirsthead",
        r"\toprule", head, r"\midrule", r"\endhead",
        r"\bottomrule", r"\endfoot",
    ]
    for key, label in DETAIL_ROWS:
        lines.append(
            r"\textit{%s} & " % label + " & ".join(DETAIL[t][key] for t in TOOLS) + r" \\[3pt]"
        )
    lines.append(r"\end{longtable}")
    return "\n".join(lines) + "\n"


PANEL_TEX = r"""\documentclass{article}
\usepackage{fontspec}
\setmainfont{Arial}
\setmonofont{Courier New}[Scale=0.9]
\usepackage{array,booktabs}
\usepackage[table]{xcolor}
\definecolor{muted}{HTML}{57606A}
\definecolor{hl}{HTML}{EAF2FB}
\definecolor{todo}{HTML}{CF222E}
\setlength{\tabcolsep}{0.8mm}
\renewcommand{\arraystretch}{1.12}
\aboverulesep=0.6ex \belowrulesep=0.6ex
\heavyrulewidth=0.6pt \lightrulewidth=0.35pt
\pagestyle{empty}
\newsavebox\tablebox
\newsavebox\panelbox
\begin{document}
\sbox\tablebox{\fontsize{%(size)s}{%(lead)s}\selectfont
\input{%(table)s}}
\sbox\panelbox{\vbox{\copy\tablebox
%(notes)s}}
\pdfpagewidth=\dimexpr\wd\panelbox+2mm\relax
\pdfpageheight=\dimexpr\ht\panelbox+\dp\panelbox+2mm\relax
\hoffset=\dimexpr-1in+1mm\relax
\voffset=\dimexpr-1in+1mm\relax
\shipout\box\panelbox
\end{document}
"""

NOTES_BLOCK = r"""\vskip 3pt
\hsize=\wd\tablebox \parindent=0pt
{\fontsize{5.6}{6.8}\selectfont\color{muted}\raggedright
\input{panel-notes.tex}\par}"""


# name: (footnotes, label column, tool columns; cm). The "figure" variant is
# typeset 34 cm wide so that, scaled to the 2880-px content width of Figure 1,
# its 7-pt text is ~21 px, like the body text of the other panels.
PANELS = {
    "panel-tools": (True, 2.45, 2.55),
    "panel-tools-nonotes": (False, 2.45, 2.55),
    "panel-tools-figure": (True, 5.4, 7.0),
}


def write_fragments() -> None:
    (HERE / "panel-notes.tex").write_text(panel_notes())
    (HERE / "tab-detail.tex").write_text(detail_table())
    for name, (notes, label_w, col_w) in PANELS.items():
        table = f"tab-{name}.tex"
        (HERE / table).write_text(compact_table(label_w, col_w))
        (HERE / f"{name}.tex").write_text(
            PANEL_TEX
            % {
                "size": "7",
                "lead": "8.2",
                "table": table,
                "notes": NOTES_BLOCK if notes else "",
            }
        )
    print("wrote table fragments and panel sources")


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout[-3000:])
        raise SystemExit(f"failed: {' '.join(cmd)}")


def build_pdfs() -> None:
    build = HERE / "build"
    build.mkdir(exist_ok=True)
    for name in PANELS:
        run(["xelatex", "-interaction=nonstopmode", "-halt-on-error",
             f"-output-directory={build}", f"{name}.tex"])
        shutil.copy(build / f"{name}.pdf", HERE / f"{name}.pdf")
        run(["pdftoppm", "-png", "-r", "600", "-singlefile", f"{name}.pdf", name])
        run(["pdftocairo", "-svg", f"{name}.pdf", f"{name}.svg"])
    for _ in range(2):
        run(["pdflatex", "-interaction=nonstopmode", "-halt-on-error",
             f"-output-directory={build}", "tool-comparison.tex"])
    shutil.copy(build / "tool-comparison.pdf", HERE / "tool-comparison.pdf")
    print("built panels (PDF, PNG, SVG) and tool-comparison.pdf")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pdf", action="store_true", help="also build panels and document")
    args = parser.parse_args()
    write_fragments()
    if args.pdf:
        build_pdfs()


if __name__ == "__main__":
    main()
