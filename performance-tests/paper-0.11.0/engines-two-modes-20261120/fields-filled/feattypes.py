import gzip
import sys
import collections

for path in sys.argv[1:]:
    op = gzip.open if path.endswith(".gz") else open
    c = collections.Counter()
    idx = None
    with op(path, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                if line.startswith("##INFO=<ID=CSQ"):
                    idx = (
                        line.split("Format: ")[1]
                        .rstrip()
                        .rstrip('">')
                        .split("|")
                        .index("Feature_type")
                    )
                continue
            info = line.split("\t", 8)[7]
            i = info.find("CSQ=")
            if i < 0:
                continue
            for e in info[i + 4 :].split(";", 1)[0].split(","):
                c[e.split("|")[idx]] += 1
    print(path.split("/")[-1], sum(c.values()), dict(c))
