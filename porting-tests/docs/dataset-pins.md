# Dataset pins

`PINS.toml` pins the **annotation corpus** only:

| Pin | Artefact |
|---|---|
| `hf_cache_ensembl` | Hugging Face VEP 116 GRCh38 ensembl cache (dataset commit) |
| `hf_cache_refseq` | Hugging Face VEP 116 GRCh38 refseq cache (dataset commit) |
| `hf_cache_merged` | Hugging Face VEP 116 GRCh38 merged cache (dataset commit) |
| `grch38_fasta` | Ensembl release-116 GRCh38 primary assembly FASTA (sha256 of uncompressed `.fa`) |

The engine under test (vepyr and its dependency ladder) is **not** pinned here;
it stays a run-time argument when test runners land later.

Validate and print the table (requires [uv](https://docs.astral.sh/uv/)):

```bash
uv sync --frozen --group dev
PYTHONPATH=tools uv run --frozen python -c 'from pathlib import Path; from pins import load_pins; print(sorted(load_pins(Path("PINS.toml")).keys()))'
uv run --frozen python tools/pins.py
uv run --frozen pytest tools/test_pins.py
```

A malformed or truncated SHA is rejected by `tools/pins.py` (exit 1).
