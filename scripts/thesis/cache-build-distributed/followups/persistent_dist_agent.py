import json, os, time
from pathlib import Path
import vepyr

TASKS = Path('/tasks/tasks.json')
CLAIMS = Path('/tasks/claims')
RESULTS = Path('/tasks/results')
DATA = '/raw_cache/homo_sapiens_merged/116_GRCh38'
OUT = Path('/output')
CLAIMS.mkdir(parents=True, exist_ok=True)
RESULTS.mkdir(parents=True, exist_ok=True)
tasks = json.loads(TASKS.read_text())
while True:
    chosen = None
    for task in tasks:
        claim = CLAIMS / f"{task['id']}.lock"
        try:
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            chosen = task
            break
        except FileExistsError:
            continue
    if chosen is None:
        break
    started = time.time()
    result = {'id': chosen['id'], 'entity': chosen['entity'], 'group': chosen['group'], 'status': 'running'}
    try:
        out = OUT / f"task_{chosen['id']}"
        out.mkdir(parents=True, exist_ok=True)
        stats = vepyr._core.build_cache_entity(
            DATA, str(out), chosen['entity'], partitions=1,
            cache_source_type='merged', overwrite=True,
            expected_cache_version='116', chroms=chosen['chroms'])
        result.update(status='done', seconds=time.time()-started, stats=stats)
    except Exception as exc:
        result.update(status='error', seconds=time.time()-started, error=repr(exc))
    (RESULTS / f"{chosen['id']}.json").write_text(json.dumps(result, default=str) + '\n')
