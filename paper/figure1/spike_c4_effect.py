"""SPIKE (throwaway): panel C4 as the measured effect of the warm/cold layout.

Whole HG002 genome (chr1-22), cold file cache, warm/cold (as shipped) vs the same rows sorted
by position; sums of per-chromosome medians from vepyr branch perf/cache-tiering-gain,
performance-tests/cache-tiering/outputs/wgs_20261122 (240e030).
"""

from __future__ import annotations

import build_figure as fig

INK = "#111111"
GREY = "#777777"
SORTED = "#b6b6b6"

# (label, tiered, sorted, unit formatter)
SECTIONS = [
    (
        "CPU time (user + system)",
        [("1 worker", 359.1, 564.5), ("8 workers", 387.9, 618.2)],
        "{:.0f} s",
    ),
    (
        "Wall time",
        [("1 worker", 241.2, 397.9), ("8 workers", 103.3, 129.6)],
        "{:.0f} s",
    ),
    (
        "Pages",
        [
            ("decoded: find rows (start)", 1034960, 1014300),
            ("holding matches (start)", 245980, 1014268),
            ("decoded: read rows (26 columns)", 7474811, 34683304),
        ],
        "pages",
    ),
]


def effect_column(code: str = "C4", width: float = 520) -> fig.Scene:
    """Relative bars: the position-sorted layout is 100% in every row."""
    s = fig.Scene()
    s._counter = 36000
    s.label(0, 14, 68, 44, code, font_size=31, weight=700)
    s.label(74, 15, width - 74, 42, "Layout effect", font_size=29, weight=700)
    s.label(0, 66, width, 84,
            "Whole HG002 genome (chr1–22),\ncold file cache; bar length:\nsorted by position = 100%", font_size=20)  # fmt: skip

    bar_max = 360  # width of 100%
    top = max(t / so for _, rows, _ in SECTIONS for _, t, so in rows)
    y = 172
    for title, rows, unit in SECTIONS:
        s.label(0, y, width, 34, title, font_size=22, weight=700)
        s.edge(0, y + 38, width, y + 38, arrow=False, width=1.2)
        y += 54
        for name, tiered, sorted_ in rows:
            rel = tiered / sorted_
            s.label(0, y, 390, 30, name, font_size=20)
            s.label(width - 120, y, 120, 30, f"{rel - 1:+.0%}".replace("-", "–"), font_size=22,
                    weight=700, align="right")  # fmt: skip
            for k, (value, fill, raw) in enumerate(
                ((rel, INK, tiered), (1.0, SORTED, sorted_))
            ):
                by = y + 36 + k * 32
                w = max(2.0, bar_max * value / top)
                s.box(0, by, w, 24, "", fill=fill, stroke_width=0)
                text = f"{raw:,}" if unit == "pages" else f"{value:.0%}"
                s.label(w + 8, by - 4, 150, 30, text, font_size=19)
            y += 124
        y += 24

    s.box(0, y + 8, 22, 16, "", fill=INK, stroke_width=0)
    s.label(30, y, 300, 30, "warm/cold", font_size=19)
    s.box(0, y + 42, 22, 16, "", fill=SORTED, stroke_width=0)
    s.label(30, y + 34, 300, 30, "sorted by position", font_size=19)
    return s
