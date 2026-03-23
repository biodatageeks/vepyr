# vepyr
vepyr (/ˈvaɪpər/) — VEP Yielding Performant Results — a blazing-fast Rust reimplementation of Ensembl's Variant Effect Predictor.

![logo.png](docs/logo.png)

## Setup with uv

1. Install `uv`.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository and enter it.

```bash
git clone git@github.com:biodatageeks/vepyr.git
cd vepyr
```

3. Sync dependencies and build the package in place.

```bash
RUSTFLAGS="-C target-cpu=native" uv sync --reinstall-package vepyr
```

4. Run Python commands inside the managed environment.

```bash
uv run python -c "import vepyr; print(vepyr.__all__)"
```

5. Run the test suite.

```bash
uv run pytest
```
