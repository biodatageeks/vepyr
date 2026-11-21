#!/usr/bin/env python3
"""Black-and-white versions of the project logos used in panel C1.

    python make_logos.py <directory with the colour logos>

The colour logos are those of the supplement of the paper
(supplementary/figures/logos: PDF or PNG, one file per project). Each is
rasterised 1400 px wide if it is a PDF, converted to grey on a white
background, darkened (gamma 2.6, so that light brand colours stay visible in
print) and written to logos-bw/<name>.png with its transparency. Needs
pdftocairo (poppler) and Pillow. Logos are trademarks of their projects.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image

HERE = Path(__file__).resolve().parent
NAMES = (
    "arrow",
    "datafusion",
    "duckdb",
    "huggingface",
    "nextflow",
    "nf-core",
    "pandas",
    "parquet",
    "polars",
    "polars-bio",
    "spark",
)
LUT = [round(255 * (v / 255) ** 2.6) for v in range(256)]


def main() -> None:
    source = Path(sys.argv[1])
    out = HERE / "logos-bw"
    out.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        for name in NAMES:
            png = source / f"{name}.png"
            if not png.exists():
                png = Path(tmp) / f"{name}.png"
                subprocess.run(
                    [
                        "pdftocairo",
                        "-png",
                        "-transp",
                        "-singlefile",
                        "-scale-to-x",
                        "1400",
                        "-scale-to-y",
                        "-1",
                        str(source / f"{name}.pdf"),
                        str(png.with_suffix("")),
                    ],
                    check=True,
                )
            image = Image.open(png).convert("RGBA")
            white = Image.new("RGBA", image.size, (255, 255, 255, 255))
            grey = Image.alpha_composite(white, image).convert("L").point(LUT)
            Image.merge("LA", (grey, image.getchannel("A"))).save(
                out / f"{name}.png", optimize=True
            )
            print(f"{name}: {image.size[0]} x {image.size[1]}")


if __name__ == "__main__":
    main()
