"""Count, per CSQ field, the records with at least one non-empty value. Usage: fieldfill.py <vcf or vcf.gz> ..."""

import gzip
import sys
import json

out = {}
for path in sys.argv[1:]:
    op = gzip.open if path.endswith(".gz") else open
    fields, counts, recs, entries = None, None, 0, 0
    with op(path, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                if line.startswith("##INFO=<ID=CSQ"):
                    fields = line.split("Format: ")[1].rstrip().rstrip('">').split("|")
                    counts = [0] * len(fields)
                continue
            recs += 1
            info = line.split("\t", 8)[7]
            i = info.find("CSQ=")
            if i < 0:
                continue
            csq = info[i + 4 :].split(";", 1)[0]
            seen = [False] * len(fields)
            for entry in csq.split(","):
                entries += 1
                for k, v in enumerate(entry.split("|")):
                    if v and k < len(seen):
                        seen[k] = True
            for k, s in enumerate(seen):
                if s:
                    counts[k] += 1
    out[path] = {
        "records": recs,
        "entries": entries,
        "fields": dict(zip(fields, counts)),
    }
json.dump(out, sys.stdout, indent=1)
