"""Figure 1A: comparison of Ensembl VEP, fastVEP, vep-rs and vepyr.

The cells below are the only source of the panel A table. table_file() writes it as
\\figonetable (figures/figure1-table.tex). The figure layout needs the table's size, which
size() takes from generated/figure1-panel-a-size.txt; when the table text or the paper's
document class changes, it measures the table again with pdflatex, using the class of
vepyr-main.tex, so the reserved space matches the table as printed.

Moved from the vepyr repository (paper/tool-comparison/build_table.py, 53e037e) on 2026-11-23.
The detailed table and the working document of that script are not part of the paper and
stay there.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # the paper repository
SIZE_FILE = "generated/figure1-panel-a-size.txt"
MAIN_TEX = ROOT / "vepyr-main.tex"

TOOLS = ["VEP", "fastVEP", "vep-rs", "vepyr"]

# (name, release, git commit of the code described). Compact panel: the release
# tags compared in Section 10 of the supplement; VEP is the ensembl-vep
# release/116.0 tag (plugins: VEP_plugins release/116, 59b34e5).
HEAD = {
    "VEP": ("Ensembl VEP", "116.0", "57ea5c5"),
    "fastVEP": ("fastVEP", "0.6.1", "4ca2a6e"),
    "vep-rs": ("vep-rs", "0.5.0", "1d84873"),
    "vepyr": ("vepyr", "0.11.0", "d75efcc"),
}

# ---------------------------------------------------------------------------
# Compact cells: Figure 1 panel. Superscripts refer to PANEL_NOTES.
#
# Measured cells are \\val keys of generated/data.tex (tools/make_numbers.py; data/README.md):
# fields.* from data/raw/fields_filled_hg002, conc.* from data/raw/engine_concordance_*,
# engines.* (time and memory of fastVEP, vep-rs, vepyr) from data/raw, wgs.* (VEP time) from
# data/wgs_scaling.csv and vep.fork7.* (VEP memory) from data/measurements.csv.
# The "Evaluation" row (streaming / lazy, pushdown) was removed on 2026-11-21 at the
# request of the author: vep-rs allocates up to 39 GiB in one invocation, so
# "streaming" is not established for it.
# ---------------------------------------------------------------------------

COMPACT_ROWS = [
    ("lang", "Language"),
    ("parallel", "Parallelism"),
    ("cache", "Cache format"),
    (
        "fields",
        r"Fields filled, of \val{fields.ensembl.vep.declared} / \val{fields.merged.vep.declared} (Ensembl / merged)\textsuperscript{a}",
    ),
    ("transcripts", "Transcript sets"),
    ("variants", "Declared variant classes"),
    ("plugins", "External annotation sources"),
    ("concordance", r"Records as in VEP, Ensembl / merged\textsuperscript{d}"),
    ("time", r"Whole genome, merged cache, 8 workers\textsuperscript{e}"),
    ("memory", r"Peak memory\textsuperscript{e}"),
    ("ui", "Interfaces"),
    ("output", "Output"),
]

TODO = r"\textcolor{todo}{\textbf{TODO}}"

COMPACT = {
    "VEP": {
        "lang": "Perl",
        "parallel": "processes",
        "cache": "Storable/Sereal, gzip per 1-Mb region",
        "fields": r"\val{fields.ensembl.vep.filled} / \val{fields.merged.vep.filled}",
        "transcripts": "Ensembl, RefSeq, merged",
        "variants": "SNV, indel, MNV, SV",
        "plugins": r"plugins, \texttt{--custom}",
        "concordance": "reference",
        "time": r"\val{wgs.macos.8.vep_s} s",
        "memory": r"\val{vep.fork7.peak_rss_gib.r1} GiB",
        "ui": "CLI, web, REST",
        "output": "VCF, TSV, JSON",
    },
    "fastVEP": {
        "lang": "Rust",
        "parallel": "threads",
        "cache": r"bincode + zstd (transcripts), OSAv2 (annotations), VEP \texttt{all\_vars.gz}",
        "fields": r"\val{fields.ensembl.fastvep.filled} / \val{fields.merged.fastvep.filled}",
        "transcripts": r"Ensembl, RefSeq, merged\textsuperscript{b}",
        "variants": "SNV, indel, MNV, SV",
        "plugins": r"fastSA databases",
        "concordance": r"\val{conc.hg002.ensembl.fastvep.pct0}\% / \val{conc.hg002.merged.fastvep.pct0}\%",
        "time": r"\val{engines.hg002.merged.whole.fastvep.wall8} s",
        "memory": r"\val{engines.hg002.merged.whole.fastvep.peak8_r1} GiB",
        "ui": "CLI, web, REST",
        "output": "VCF, TSV, JSON",
    },
    "vep-rs": {
        "lang": "Rust",
        "parallel": "threads",
        "cache": r"JSON, gzip optional; VEP \texttt{all\_vars.gz}",
        "fields": r"\val{fields.ensembl.veprs.filled} / \val{fields.merged.veprs.filled}",
        "transcripts": r"Ensembl, RefSeq, merged\textsuperscript{c}",
        "variants": "SNV, indel, MNV, SV",
        "plugins": r"built-in databases, C-ABI plugins",
        "concordance": r"\val{conc.hg002.ensembl.veprs.pct0}\% / \val{conc.hg002.merged.veprs.pct0}\%",
        "time": r"\val{engines.hg002.merged.whole.veprs.wall8} s",
        "memory": r"\val{engines.hg002.merged.whole.veprs.peak8_r1} GiB",
        "ui": "CLI",
        "output": "VCF, TSV, JSON, Parquet",
    },
    "vepyr": {
        "lang": "Rust, Python",
        "parallel": "threads",
        "cache": "Parquet, zstd (core and plugins)",
        "fields": r"\textbf{\val{fields.ensembl.vepyr.filled} / \val{fields.merged.vepyr.filled}}",
        "transcripts": "Ensembl, RefSeq, merged",
        "variants": "SNV, indel, MNV",
        "plugins": r"TOML plugin manifests",
        "concordance": r"\textbf{\val{conc.hg002.ensembl.vepyr.pct0}\% / \val{conc.hg002.merged.vepyr.pct0}\%}",
        "time": r"\textbf{\val{engines.hg002.merged.whole.vepyr.wall8} s}",
        "memory": r"\val{engines.hg002.merged.whole.vepyr.peak8_r1} GiB",
        "ui": "Python API, SQL (via Polars), CLI",
        "output": r"Polars DataFrame/\allowbreak LazyFrame\textsuperscript{f}, VCF",
    },
}

PANEL_SMALL = r"\fontsize{5.8}{6.6}\selectfont"

PANEL_NOTES = [
    (
        "a",
        r"Of the \val{fields.ensembl.vep.declared} and \val{fields.merged.vep.declared} \texttt{CSQ} fields of VEP~116 \texttt{--everything} with the "
        r"Ensembl and the merged cache, those with a value in at least one record of the "
        r"output for HG002 (chromosomes 1--22). VEP fills all \val{fields.ensembl.vep.filled}; with the merged cache "
        r"it fills \val{fields.merged.vep.filled}, as \val{fields.merged.vep.empty_text} has no value in any record.",
    ),
    (
        "b",
        r"From Ensembl and RefSeq GFF3 files (merged: both, with \texttt{SOURCE}); "
        r"RefSeq-specific fields are not written.",
    ),
    (
        "c",
        r"Through a cache converted from the VEP RefSeq or merged cache; "
        r"\val{fields.merged.veprs.notdeclared_text} are not written.",
    ),
    (
        "d",
        r"Records of HG002 (chromosomes 1--22; \val{conc.hg002.merged.vepyr.withcsq} with a \texttt{CSQ} field) whose "
        r"\texttt{CSQ} entries agree with VEP in every field shared with VEP. For vepyr, and "
        r"for vep-rs with the Ensembl cache, these records are identical to VEP byte for byte.",
    ),
    (
        "e",
        r"Wall time and peak memory of the process for HG002 (chromosomes 1--22) with the merged cache in one "
        r"invocation, with 8 processes (VEP, \texttt{--fork 7}) or 8 workers, on macOS (Apple "
        r"M3~Max). For VEP, the resident memory of all its processes, summed (\val{vep.fork7.peak_pss_gib.r1}~GiB when memory shared between them is counted once).",
    ),
    (
        "f",
        r"A Polars LazyFrame can be written in any output format of Polars, including Parquet, "
        r"JSON and TSV.",
    ),
]


def tool_head(
    tool: str,
    bold: bool = False,
    small: str = r"\footnotesize",
    head: dict | None = None,
) -> str:
    name, version, sha = (head or HEAD)[tool]
    name = r"\textbf{%s}" % name if bold or tool == "vepyr" else name
    return r"%s\newline{%s\color{muted}%s\enspace\texttt{%s}}" % (
        name,
        small,
        version,
        sha,
    )


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


# The panel A table of Figure 1 is typeset 34 cm wide (label column 5.4 cm, tool columns 7.0 cm)
# so that, scaled to the content width of Figure 1, its 7-pt text matches the other panels.
LABEL_W, COL_W = 5.4, 7.0


def table_file() -> str:
    """Panel A table for pdfLaTeX, as \\figonetable (table above its notes)."""
    table, notes = compact_table(LABEL_W, COL_W), panel_notes()
    for old, new in (("≤", r"\ensuremath{\leq}"), ("×", r"\ensuremath{\times}")):
        table, notes = table.replace(old, new), notes.replace(old, new)
    return (
        "% Panel A of Figure 1, generated by tools/figure1/panel_a.py (run tools/make_numbers.py).\n"
        "\\definecolor{muted}{HTML}{57606A}\n"
        "\\definecolor{hl}{HTML}{EAF2FB}\n"
        "\\definecolor{todo}{HTML}{CF222E}\n"
        "\\newsavebox\\figonetablebox\n"
        "\\newcommand{\\figonetable}{{%\n"
        "\\sffamily\\fontsize{7}{8.2}\\selectfont\n"
        "\\setlength{\\tabcolsep}{0.8mm}\\renewcommand{\\arraystretch}{1.12}%\n"
        "\\aboverulesep=0.6ex \\belowrulesep=0.6ex\n"
        "\\heavyrulewidth=0.6pt \\lightrulewidth=0.35pt\n"
        "\\sbox\\figonetablebox{" + table.strip() + "}%\n"
        "\\vbox{\\copy\\figonetablebox\\vskip 3pt\n"
        "\\hsize=\\wd\\figonetablebox \\parindent=0pt\n"
        "{\\fontsize{5.6}{6.8}\\selectfont\\color{muted}\\raggedright\n"
        + notes.strip()
        + "\\par}}}}\n"
    )


# ---------------------------------------------------------------- size of the typeset table

warnings: list[str] = []  # read by tools/make_numbers.py
DATA_TEX = ""  # generated/data.tex as tools/make_numbers.py is about to write it (cells use \\val)


def document_class() -> str:
    """The \\documentclass line of vepyr-main.tex (the first one not commented out)."""
    for line in MAIN_TEX.read_text().splitlines():
        if line.lstrip().startswith("\\documentclass"):
            return line.split("%")[0].strip()
    raise ValueError(f"no \\documentclass in {MAIN_TEX.name}")


MEASURE_TEX = r"""%(documentclass)s
\usepackage{xcolor,colortbl,booktabs,array}
\input{data.tex}
\input{figure1-table.tex}
\begin{document}
\setbox0=\hbox{\figonetable}
\typeout{FIG1A wd=\the\wd0\space ht=\the\ht0\space dp=\the\dp0}
\end{document}
"""


def measure(table: str, documentclass: str) -> tuple[float, float]:
    """Width and height (ht + dp) in pt of \\figonetable typeset by pdflatex with the paper's class."""
    pdflatex = shutil.which("pdflatex")
    if not pdflatex:
        raise RuntimeError("pdflatex not found")
    with tempfile.TemporaryDirectory() as tmp:
        Path(tmp, "figure1-table.tex").write_text(table)
        Path(tmp, "data.tex").write_text(DATA_TEX)
        Path(tmp, "measure.tex").write_text(
            MEASURE_TEX % {"documentclass": documentclass}
        )
        # run in the paper folder so that the class file (oup-authoring-template.cls) is found
        result = subprocess.run(
            [
                pdflatex,
                "-interaction=batchmode",
                "-halt-on-error",
                f"-output-directory={tmp}",
                str(Path(tmp, "measure.tex")),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "TEXINPUTS": f"{tmp}{os.pathsep}{ROOT}{os.pathsep}"},
        )
        log = (
            Path(tmp, "measure.log").read_text(errors="replace")
            if Path(tmp, "measure.log").exists()
            else ""
        )
    m = re.search(
        r"FIG1A wd=([\d.]+)pt\s*ht=([\d.]+)pt\s*dp=([\d.]+)pt", log
    )  # TeX eats the spaces
    if result.returncode != 0 or not m:
        raise RuntimeError(
            "pdflatex could not typeset the table: "
            + (log[-400:] or result.stdout[-400:])
        )
    wd, ht, dp = map(float, m.groups())
    return wd, ht + dp


def size() -> tuple[float, float, str]:
    """(width, height) of the panel A table in pt and the text of SIZE_FILE.

    The size is kept in SIZE_FILE with the SHA-256 of the table text and document class it was
    measured from, so pdflatex runs only after a change. If it cannot run (e.g. in the Overleaf
    Python Script Runner), the stored size is used and a warning is recorded.
    """
    table, documentclass = table_file(), document_class()
    key = hashlib.sha256(
        (documentclass + "\n" + table + "\n" + DATA_TEX).encode()
    ).hexdigest()
    path = ROOT / SIZE_FILE
    stored = path.read_text() if path.exists() else ""
    m = re.search(
        r"^sha256 (\w+)\nwidth_pt ([\d.]+)\nheight_pt ([\d.]+)\n", stored, re.M
    )
    if m and m.group(1) == key:
        return float(m.group(2)), float(m.group(3)), stored
    try:
        w, h = measure(table, documentclass)
    except Exception as e:  # noqa: BLE001
        if not m:
            raise
        warnings.append(
            f"panel A table changed but could not be measured ({e}); using the stored size"
        )
        return float(m.group(2)), float(m.group(3)), stored
    text = (
        "# Size of the Figure 1A table typeset by pdflatex with the class of vepyr-main.tex,\n"
        "# written by tools/figure1/panel_a.py; measured again when the table or class changes.\n"
        f"sha256 {key}\nwidth_pt {w:.5f}\nheight_pt {h:.5f}\n"
    )
    return w, h, text
