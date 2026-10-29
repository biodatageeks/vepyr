#!/usr/bin/env python3
"""Full parallel cache build: dynamic queue including translation, with RSS and merge (thesis E5).

The queue of parallel_entity_queue.py (contigs grouped greedily into GROUPS groups by
all_vars.gz size, one task per entity-group pair, ProcessPoolExecutor(PROCESSES),
RAYON_NUM_THREADS=1, partitions=1) with the sixth raw entity, translation, added as one
task per group; inside a translation task every contig is a separate call, as in
full_e2e_all.py ("one chromosome per call"). Translation tasks are submitted last.
Additionally recorded:
  * memory: every 2 s the summed RSS of the whole process tree (`ps`), its peak and series;
  * merge: per-task outputs hard-linked into one cache root with merged chrom_manifest.json
    (as assemble_parallel_cache.py, all seven generated entities), timed and included in the
    total.

Usage: full_queue_e5.py OUT_DIR   (env PARALLEL_PROCESSES, PARALLEL_GROUPS)
Run with the .venv-lpt interpreter (the queue's vepyr build).
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import vepyr

DATA = Path('/Users/lukaszjezapkowicz/workspace/data_vepyr/homo_sapiens_merged/116_GRCh38')
ENTITIES = ['variation', 'transcript', 'exon', 'regulatory', 'motif', 'translation']
GENERATED = ['variation', 'transcript', 'exon', 'regulatory', 'motif', 'translation_core', 'translation_sift']
GROUPS = int(os.environ.get('PARALLEL_GROUPS', '20'))
PROCESSES = int(os.environ.get('PARALLEL_PROCESSES', '10'))


def groups(contigs, n):
    b = [(0, []) for _ in range(n)]
    for size, c in sorted((((DATA / c / 'all_vars.gz').stat().st_size, c) for c in contigs), reverse=True):
        i = min(range(n), key=lambda j: b[j][0])
        b[i] = (b[i][0] + size, b[i][1] + [c])
    return [(i, sorted(g), size) for i, (size, g) in enumerate(b) if g]


def write(p, v):
    p.write_text(json.dumps(v, indent=2, default=str) + '\n')


def task(run, tid, gid, group, gbytes, entity):
    out = run / f'task_{tid:03d}'
    out.mkdir(parents=True, exist_ok=True)
    state = {'task': tid, 'group': gid, 'entity': entity, 'contigs': len(group), 'bytes': gbytes,
             'started': time.time(), 'status': 'running', 'pid': os.getpid()}
    write(out / 'state.json', state)
    os.environ['RAYON_NUM_THREADS'] = '1'
    os.environ['MALLOC_ARENA_MAX'] = '1'
    t0 = time.time()
    calls = [[c] for c in group] if entity == 'translation' else [group]
    stats = [vepyr._core.build_cache_entity(str(DATA), str(out / 'cache'), entity, partitions=1,
                                            cache_source_type='merged', overwrite=True,
                                            expected_cache_version='116', chroms=chroms) for chroms in calls]
    state.update(status='done', finished=time.time(), seconds=time.time() - t0, stats=stats)
    write(out / 'state.json', state)
    return state


def tree_rss_gib(root_pid):
    ps = subprocess.run(['ps', '-A', '-o', 'pid=,ppid=,rss='], capture_output=True, text=True).stdout.split('\n')
    kids, rss = {}, {}
    for line in ps:
        f = line.split()
        if len(f) == 3:
            pid, ppid, kb = map(int, f)
            kids.setdefault(ppid, []).append(pid)
            rss[pid] = kb
    total, stack = 0, [root_pid]
    while stack:
        p = stack.pop()
        total += rss.get(p, 0)
        stack += kids.get(p, [])
    return total / 1024**2


def assemble(run, out):
    started = time.perf_counter()
    manifests = {e: {} for e in GENERATED}
    linked = nbytes = 0
    for t in sorted(run.glob('task_*')):
        for entity in GENERATED:
            src_dir = t / 'cache' / entity
            if not src_dir.is_dir():
                continue
            dst_dir = out / entity
            dst_dir.mkdir(parents=True, exist_ok=True)
            for src in sorted(src_dir.glob('*.parquet')):
                dst = dst_dir / src.name
                if dst.exists():
                    raise RuntimeError(f'duplicate output: {dst}')
                os.link(src, dst)
                linked += 1
                nbytes += src.stat().st_size
            m = src_dir / 'chrom_manifest.json'
            if m.exists():
                for entry in json.loads(m.read_text()):
                    if entry['chrom'] in manifests[entity]:
                        raise RuntimeError(f"duplicate manifest entry: {entity}/{entry['chrom']}")
                    manifests[entity][entry['chrom']] = entry
    for entity, entries in manifests.items():
        if entries:
            (out / entity / 'chrom_manifest.json').write_text(
                json.dumps([entries[k] for k in sorted(entries)], indent=2) + '\n')
    shutil.copy2(DATA / 'info.txt', out / 'info.txt')
    return {'seconds': time.perf_counter() - started, 'hardlinked_parquet_files': linked, 'parquet_bytes': nbytes,
            'manifest_entries': {e: len(v) for e, v in manifests.items()}}


def main():
    root = Path(sys.argv[1]).resolve()
    run, assembled = root / 'current', root / 'assembled' / '116_GRCh38_merged'
    shutil.rmtree(root / 'current', ignore_errors=True)
    shutil.rmtree(root / 'assembled', ignore_errors=True)
    run.mkdir(parents=True)
    assembled.mkdir(parents=True)
    contigs = sorted(d.name for d in DATA.iterdir() if d.is_dir() and (d / 'all_vars.gz').is_file())
    gs = groups(contigs, GROUPS)
    tasks, tid = [], 0
    for e in ENTITIES:  # translation last
        for gid, g, gb in gs:
            tasks.append((tid, gid, g, gb, e))
            tid += 1
    write(root / 'manifest.json', {'contigs': len(contigs), 'groups': len(gs), 'tasks': len(tasks),
                                   'processes': PROCESSES, 'entities': ENTITIES, 'vepyr': vepyr.__file__})
    samples, stop = [], threading.Event()

    def sampler():
        t0 = time.time()
        while not stop.is_set():
            samples.append((round(time.time() - t0, 1), round(tree_rss_gib(os.getpid()), 3)))
            stop.wait(2)

    th = threading.Thread(target=sampler, daemon=True)
    start = time.time()
    th.start()
    with concurrent.futures.ProcessPoolExecutor(max_workers=PROCESSES) as pool:
        futures = [pool.submit(task, run, *t) for t in tasks]
        results = [f.result() for f in futures]
    build_s = time.time() - start
    stop.set()
    th.join()
    merge = assemble(run, assembled)
    total = time.time() - start
    (root / 'rss_samples.json').write_text(json.dumps(samples) + '\n')
    summary = {'seconds_build': build_s, 'seconds_merge': merge['seconds'], 'seconds_total': total,
               'peak_tree_rss_gib': max(s[1] for s in samples), 'rss_samples': len(samples),
               'groups': len(gs), 'tasks': len(tasks), 'processes': PROCESSES, 'merge': merge,
               'sum_task_seconds': sum(r['seconds'] for r in results),
               'sum_task_seconds_by_entity': {e: sum(r['seconds'] for r in results if r['entity'] == e) for e in ENTITIES},
               'assembled_bytes': sum(p.stat().st_size for p in assembled.rglob('*') if p.is_file()),
               'results': results}
    write(root / 'summary.json', summary)
    print(json.dumps({k: v for k, v in summary.items() if k != 'results'}, indent=1, default=str))


if __name__ == '__main__':
    main()
