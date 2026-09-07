PYTHON ?= python3

.PHONY: all build validate preview drawio-export

all: build validate

build:
	$(PYTHON) -B build_figure.py

validate:
	xmllint --noout figure1.drawio figure1.svg

preview: build
	$(PYTHON) -B build_figure.py --render

drawio-export: build
	@DRAWIO_BIN="$$(command -v drawio 2>/dev/null)"; \
	if [ -z "$$DRAWIO_BIN" ] && [ -x "/Applications/draw.io.app/Contents/MacOS/draw.io" ]; then \
		DRAWIO_BIN="/Applications/draw.io.app/Contents/MacOS/draw.io"; \
	fi; \
	if [ -z "$$DRAWIO_BIN" ]; then \
		echo "draw.io CLI not found; install draw.io Desktop or set PATH."; \
		exit 2; \
	fi; \
	"$$DRAWIO_BIN" --export --format svg --embed-diagram --crop --output figure1.svg figure1.drawio; \
	"$$DRAWIO_BIN" --export --format pdf --crop --output figure1.pdf figure1.drawio; \
	"$$DRAWIO_BIN" --export --format png --scale 2 --crop --output figure1.png figure1.drawio
