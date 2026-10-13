#!/usr/bin/env bash
# Regenerate the architecture diagrams from the .mmd sources.
#
# Run by hand when a diagram changes -- deliberately NOT part of the mkdocs
# build, so building the docs needs no Node toolchain. Commit the resulting
# SVGs alongside the sources.
#
#   docs/diagrams/render.sh
set -euo pipefail
cd "$(dirname "$0")"

# mermaid-cli drives Chrome through puppeteer; reuse the system browser rather
# than downloading a second one.
export PUPPETEER_EXECUTABLE_PATH="${PUPPETEER_EXECUTABLE_PATH:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
cfg="$(mktemp -t puppeteer).json"
echo '{"args": ["--no-sandbox", "--disable-gpu"]}' > "$cfg"
trap 'rm -f "$cfg"' EXIT

for src in *.mmd; do
  name="${src%.mmd}"
  for variant in light dark; do
    theme=default
    [ "$variant" = dark ] && theme=dark
    npx -y @mermaid-js/mermaid-cli@11 \
      -i "$src" -o "${name}-${variant}.svg" \
      -t "$theme" -b transparent -p "$cfg" >/dev/null
    # mermaid emits width="100%" plus a max-width, which gives the SVG no
    # intrinsic size -- a lightbox then has nothing to scale. Pin width/height
    # from the viewBox instead.
    python3 - "${name}-${variant}.svg" <<'PY'
import re, sys, pathlib
p = pathlib.Path(sys.argv[1])
s = p.read_text()
m = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', s)
if m:
    w, h = m.group(1), m.group(2)
    s = s.replace('width="100%"', f'width="{w}" height="{h}"', 1)
    s = re.sub(r'max-width:\s*[\d.]+px;?\s*', '', s, count=1)
    p.write_text(s)
PY
    echo "  ${name}-${variant}.svg"
  done
done
