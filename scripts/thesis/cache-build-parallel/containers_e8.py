#!/usr/bin/env python3
"""Container builds with a pinned image and the queue's code (thesis item E8).

The ten-container build in the thesis (mean 4932.31 s) ran an unrecorded image with
vepyr 0.4.0 / datafusion-bio-functions v0.19.2, while the dynamic queue ran a newer
native build. Here every container variant uses one image built from the commits that
reconstruct the queue's build: vepyr 30e69a4 (thesis/cache-build-scaling, Cargo [patch]
to ../datafusion-bio-functions) and datafusion-bio-functions 290df85
(thesis/cache-build-parallel), maturin 1.15.0 --release in rust:1.91-bookworm, runtime
python:3.14-slim-bookworm with the .venv-lpt versions of pyarrow, polars and tqdm. The
image id is recorded.

  build        stage both trees (git archive), build the image vepyr-queue:30e69a4-290df85
  distributed  as run_distributed() of full_cache_20_runs.py: contigs split greedily into
               10 groups by all_vars.gz size, one container per group (--cpus=2
               --memory=6g, MALLOC_ARENA_MAX=1, the original worker.py: five entities,
               partitions=1), output to Docker volumes; time = first start to last exit
  queue        parallel_entity_queue.py unchanged (20 groups, 100 tasks, 10 processes,
               RAYON_NUM_THREADS=1) in one container with --cpus=10, raw cache mounted at
               the same absolute path, output to a Docker volume
  sequential   full_core_5x.py's loop (five entities, all contigs, partitions=1) in one
               container with --cpus=10
Raw cache read-only over virtiofs in all variants, as in the original series.

Usage: containers_e8.py build OUT_DIR
       containers_e8.py distributed|queue|sequential OUT_DIR [REPEATS, default 5]
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

D = Path('/Users/lukaszjezapkowicz/workspace/data_vepyr')
DATA = D / 'homo_sapiens_merged/116_GRCh38'
VEPYR_COMMIT, DF_COMMIT = '30e69a4', '290df85'
TAG = f'vepyr-queue:{VEPYR_COMMIT}-{DF_COMMIT}'
ENTITIES = ['variation', 'transcript', 'exon', 'regulatory', 'motif']
DOCKERFILE = f'''FROM rust:1.91-bookworm AS builder
RUN apt-get update -qq && apt-get install -y -qq python3-venv cmake clang pkg-config protobuf-compiler > /dev/null \\
 && python3 -m venv /m && /m/bin/pip install -q maturin==1.15.0
COPY vepyr /src/vepyr
COPY datafusion-bio-functions /src/datafusion-bio-functions
WORKDIR /src/vepyr
RUN /m/bin/maturin build --release --locked --compatibility linux -i python3 -o /wheels
FROM python:3.14-slim-bookworm
COPY --from=builder /wheels/*.whl /tmp/
RUN pip install --no-cache-dir pyarrow==25.0.1 polars==1.44.2 tqdm==4.70.1 /tmp/*.whl && rm -f /tmp/*.whl
COPY worker.py sequential.py /app/
COPY vepyr/scripts/thesis/cache-build-parallel/parallel_entity_queue.py /app/parallel_entity_queue.py
ENV MALLOC_ARENA_MAX=1
'''
SEQUENTIAL = '''import json, os, time, vepyr
raw = os.environ["CACHE_ROOT"]
cs = sorted(d for d in os.listdir(raw) if os.path.isfile(os.path.join(raw, d, "all_vars.gz")))
rows, t0 = [], time.perf_counter()
for e in %r:
    t = time.perf_counter()
    stats = vepyr._core.build_cache_entity(raw, "/output/cache", e, partitions=1, cache_source_type="merged",
                                           overwrite=True, expected_cache_version="116", chroms=cs)
    rows.append({"entity": e, "seconds": time.perf_counter() - t, "files": len(stats)})
    print(json.dumps(rows[-1]), flush=True)
print(json.dumps({"wall_seconds": time.perf_counter() - t0, "contigs": len(cs), "rows": rows}), flush=True)
''' % ENTITIES


def contigs():
    return sorted(d.name for d in DATA.iterdir() if d.is_dir() and (d / 'all_vars.gz').is_file())


def group_contigs(cs, n=10):
    bins = [(0, []) for _ in range(n)]
    for size, c in sorted((((DATA / c / 'all_vars.gz').stat().st_size, c) for c in cs), reverse=True):
        i = min(range(n), key=lambda j: bins[j][0])
        bins[i] = (bins[i][0] + size, bins[i][1] + [c])
    return [sorted(b[1]) for b in bins if b[1]]


def volume(name):
    subprocess.run(['docker', 'volume', 'rm', '-f', name], capture_output=True)
    subprocess.run(['docker', 'volume', 'create', name], check=True, capture_output=True)


def build(out: Path):
    ctx = out / 'context'
    shutil.rmtree(ctx, ignore_errors=True)
    ctx.mkdir(parents=True)
    for repo, commit, name in ((D / 'tools/vepyr', VEPYR_COMMIT, 'vepyr'),
                               (D / 'tools/datafusion-bio-functions', DF_COMMIT, 'datafusion-bio-functions')):
        (ctx / name).mkdir()
        subprocess.run(f'git -C {repo} archive {commit} | tar -x -C {ctx / name}', shell=True, check=True)
    worker = subprocess.run(['git', '-C', str(D / 'tools/vepyr'), 'show',
                             'origin/thesis/cache-build-distributed:scripts/thesis/cache-build-distributed/worker.py'],
                            capture_output=True, text=True, check=True).stdout
    (ctx / 'worker.py').write_text(worker)
    (ctx / 'sequential.py').write_text(SEQUENTIAL)
    (ctx / 'Dockerfile').write_text(DOCKERFILE)
    t0 = time.time()
    with open(out / 'build.log', 'w') as log:
        rc = subprocess.run(['docker', 'build', '-t', TAG, str(ctx)], stdout=log, stderr=subprocess.STDOUT).returncode
    if rc:
        raise SystemExit('docker build failed, see build.log')
    info = {'tag': TAG, 'vepyr': VEPYR_COMMIT, 'datafusion_bio_functions': DF_COMMIT, 'build_seconds': time.time() - t0,
            'image_id': subprocess.run(['docker', 'image', 'inspect', '--format', '{{.Id}}', TAG],
                                       capture_output=True, text=True).stdout.strip()}
    (out / 'image.json').write_text(json.dumps(info, indent=1) + '\n')
    print(json.dumps(info, indent=1))


def base(name, out_volume, limits):
    return ['docker', 'run', '--rm', '--name', name, *limits, '-e', 'MALLOC_ARENA_MAX=1',
            '-v', f'{DATA}:{DATA}:ro', '-e', f'CACHE_ROOT={DATA}', '-v', f'{out_volume}:/output']


def run_distributed(out: Path, rep: int) -> dict:
    groups = group_contigs(contigs())
    procs, started = [], time.time()
    for i, g in enumerate(groups):
        vol = f'e8-dist-{rep}-{i}'
        volume(vol)
        cmd = base(f'e8_dist_{rep}_{i}', vol, ['--cpus=2', '--memory=6g', '--memory-swap=6g']) + [
            '-e', 'WORKER_COUNT=1', '-e', 'WORKER_INDEX=0', '-e', f'CHROMS={",".join(g)}', '-e', 'OUTPUT_DIR=/output',
            TAG, 'python', '/app/worker.py']
        log = open(out / f'dist_r{rep}_w{i:02d}.log', 'w')
        log.write('COMMAND: ' + ' '.join(cmd) + '\n'); log.flush()
        procs.append((i, g, vol, log, subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT), time.time()))
    rows = []
    for i, g, vol, log, p, t0 in procs:
        rc = p.wait(); log.close()
        rows.append({'worker': i, 'contigs': len(g), 'seconds_wall': time.time() - t0, 'returncode': rc})
    wall = time.time() - started
    for i, *_ in procs:
        subprocess.run(['docker', 'volume', 'rm', '-f', f'e8-dist-{rep}-{i}'], capture_output=True)
    return {'variant': 'distributed', 'repeat': rep, 'wall_seconds': wall, 'workers': rows}


def run_single(out: Path, variant: str, rep: int) -> dict:
    vol = f'e8-{variant}-{rep}'
    volume(vol)
    if variant == 'queue':
        cmd = base(f'e8_queue_{rep}', vol, ['--cpus=10']) + ['-w', '/output', '-e', 'PARALLEL_PROCESSES=10',
                                                             '-e', 'PARALLEL_GROUPS=20', TAG, 'python',
                                                             '/app/parallel_entity_queue.py']
    else:
        cmd = base(f'e8_seq_{rep}', vol, ['--cpus=10']) + ['-e', 'RAYON_NUM_THREADS=1', TAG, 'python', '/app/sequential.py']
    t0 = time.time()
    with open(out / f'{variant}_r{rep}.log', 'w') as log:
        log.write('COMMAND: ' + ' '.join(cmd) + '\n'); log.flush()
        rc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
    wall = time.time() - t0
    lines = (out / f'{variant}_r{rep}.log').read_text().strip().splitlines()
    subprocess.run(['docker', 'volume', 'rm', '-f', vol], capture_output=True)
    return {'variant': variant, 'repeat': rep, 'wall_seconds': wall, 'returncode': rc, 'last_line': lines[-1][:2000]}


def main():
    mode, out = sys.argv[1], Path(sys.argv[2]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    if mode == 'build':
        return build(out)
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    image = subprocess.run(['docker', 'image', 'inspect', '--format', '{{.Id}}', TAG], capture_output=True,
                           text=True).stdout.strip()
    if not image:
        raise SystemExit(f'image {TAG} missing: run build first')
    raw = out / f'{mode}_runs.jsonl'
    done = {json.loads(l)['repeat'] for l in raw.read_text().splitlines()} if raw.exists() else set()
    for rep in range(1, reps + 1):
        if rep in done:
            continue
        r = run_distributed(out, rep) if mode == 'distributed' else run_single(out, mode, rep)
        r.update(image_id=image, utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
        with raw.open('a') as f:
            f.write(json.dumps(r) + '\n')
        print(json.dumps(r), flush=True)
        time.sleep(10)


if __name__ == '__main__':
    main()
