"""Record count and MD5 of the record lines of a VCF, per contig, in one pass.

Usage: contig_md5.py <plain.vcf> <out.json>
Prints "<records> <md5>" where the MD5 is that of all record lines (as `grep -v '^#' | md5`), and writes
{"records", "md5", "contigs": {name: {"records", "md5"}}, "order": [...]} to the JSON file. The per-contig
checksums are comparable with the checksums of per-chromosome runs. Header lines are those before the first
record; a later line starting with '#' would be counted as a record of contig '#...'.

The file is read with F_NOCACHE into a page-aligned buffer, so that reading a 30-90 GiB output does not push the
input files of the engines out of the file cache between two timed runs (measured 2026-11-20: after a cached
read 4-9 GiB of the inputs were resident before the next run, after a run without checksum 14-19 GiB).
"""

import fcntl
import hashlib
import json
import mmap
import sys

path, out = sys.argv[1], sys.argv[2]
BLOCK = 1 << 25
total = hashlib.md5()
hashes, counts, order = {}, {}, []
in_header, carry, current = True, b"", None


def contig_of(line_start, block):
    tab = block.find(b"\t", line_start)
    return block[line_start:tab]


def feed(name, data):
    if name not in hashes:
        hashes[name], counts[name] = hashlib.md5(), 0
        order.append(name)
    hashes[name].update(data)
    counts[name] += data.count(b"\n")


buf = mmap.mmap(-1, BLOCK)
with open(path, "rb", buffering=0) as fh:
    fcntl.fcntl(fh.fileno(), fcntl.F_NOCACHE, 1)
    while True:
        chunk = bytes(memoryview(buf)[: fh.readinto(buf)])
        block = carry + chunk
        if not chunk:
            if block and not block.endswith(b"\n"):
                block += b"\n"
            carry = b""
        else:
            cut = block.rfind(b"\n") + 1
            block, carry = block[:cut], block[cut:]
        if in_header:
            pos = 0
            while pos < len(block) and block[pos : pos + 1] == b"#":
                pos = block.find(b"\n", pos) + 1
            if pos < len(block):
                in_header = False
            block = block[pos:]
        if block:
            total.update(block)
            first = contig_of(0, block)
            last = contig_of(block.rfind(b"\n", 0, len(block) - 1) + 1, block)
            if first == last:
                feed(first, block)
            else:
                pos = 0
                while pos < len(block):
                    name = contig_of(pos, block)
                    # extend over the run of lines of this contig
                    end = pos
                    while True:
                        nxt = block.find(b"\n", end) + 1
                        if nxt >= len(block) or not block.startswith(name + b"\t", nxt):
                            end = nxt
                            break
                        end = nxt
                    feed(name, block[pos:end])
                    pos = end
        if not chunk:
            break
records = sum(counts.values())
result = {
    "records": records,
    "md5": total.hexdigest(),
    "order": [c.decode() for c in order],
    "contigs": {
        c.decode(): {"records": counts[c], "md5": hashes[c].hexdigest()} for c in order
    },
}
json.dump(result, open(out, "w"), indent=1)
print(records, total.hexdigest())
