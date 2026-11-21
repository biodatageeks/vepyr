"""Scan a VCF from standard input and report which CSQ fields ever have a value.

Usage: <plain VCF on stdin> | fieldscan.py <out.json>
Writes {"fields": [...declared CSQ fields...], "filled": [...fields with a value in at least one CSQ entry...],
"never": [...declared fields without any value...], "records", "records_without_csq", "entries",
"feature_types": {Feature_type: entries}}. Only the fields not yet seen with a value are checked per entry, so
the scan stays fast once the common fields have been found.
"""

import collections
import json
import sys

out = sys.argv[1]
fields, unseen, feat_idx = None, None, None
feat = collections.Counter()
recs = nocsq = entries = 0
for line in sys.stdin.buffer:
    if line[:1] == b"#":
        if line.startswith(b"##INFO=<ID=CSQ"):
            fields = line.split(b"Format: ")[1].rstrip().rstrip(b'">').split(b"|")
            unseen = set(range(len(fields)))
            feat_idx = fields.index(b"Feature_type")
        continue
    recs += 1
    info = line.split(b"\t", 8)[7]
    if info.startswith(b"CSQ="):
        i = 0
    else:
        i = info.find(b";CSQ=")
        if i < 0:
            nocsq += 1
            continue
        i += 1
    j = info.find(b";", i)
    csq = info[i + 4 :] if j < 0 else info[i + 4 : j]
    for e in csq.split(b","):
        entries += 1
        parts = e.split(b"|")
        feat[parts[feat_idx]] += 1
        if unseen:
            for u in tuple(unseen):
                if u < len(parts) and parts[u]:
                    unseen.discard(u)
names = [f.decode() for f in fields] if fields else []
never = sorted(unseen) if unseen else []
json.dump(
    {
        "fields": names,
        "filled": [n for k, n in enumerate(names) if k not in set(never)],
        "never": [names[k] for k in never],
        "records": recs,
        "records_without_csq": nocsq,
        "entries": entries,
        "feature_types": {k.decode(): v for k, v in feat.items()},
    },
    open(out, "w"),
    indent=1,
)
