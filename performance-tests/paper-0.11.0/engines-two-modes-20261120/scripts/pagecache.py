"""Page-cache residency of files, and eviction, without root (mincore and msync(MS_INVALIDATE) on a mapping).

Usage: pagecache.py resident|evict [--each] <path>...
Directories are walked and symbolic links are followed; each underlying file is counted once. Prints the
resident and total size; with --each also one line per file of at least 16 MiB.
"""

import ctypes
import os
import sys

libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_longlong,
]
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
libc.msync.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
PAGE = os.sysconf("SC_PAGESIZE")
PROT_READ, MAP_SHARED, MS_INVALIDATE, MAP_FAILED = 1, 1, 2, ctypes.c_void_p(-1).value


def files(paths):
    seen = set()
    for p in paths:
        if os.path.isdir(p):
            for root, _, names in os.walk(p, followlinks=True):
                for n in names:
                    seen.add(os.path.realpath(os.path.join(root, n)))
        else:
            seen.add(os.path.realpath(p))
    return sorted(f for f in seen if os.path.isfile(f))


def one(path, evict):
    size = os.path.getsize(path)
    if size == 0:
        return 0, 0
    fd = os.open(path, os.O_RDONLY)
    try:
        addr = libc.mmap(None, size, PROT_READ, MAP_SHARED, fd, 0)
        if addr == MAP_FAILED:
            raise OSError(ctypes.get_errno(), f"mmap {path}")
        try:
            if evict and libc.msync(addr, size, MS_INVALIDATE) != 0:
                raise OSError(ctypes.get_errno(), f"msync {path}")
            pages = (size + PAGE - 1) // PAGE
            vec = ctypes.create_string_buffer(pages)
            if libc.mincore(addr, size, vec) != 0:
                raise OSError(ctypes.get_errno(), f"mincore {path}")
            resident = sum(1 for b in vec.raw if b & 1)
        finally:
            libc.munmap(addr, size)
    finally:
        os.close(fd)
    return min(resident * PAGE, size), size


args = sys.argv[1:]
action = args.pop(0)
each = "--each" in args
paths = [a for a in args if a != "--each"]
res = tot = 0
for f in files(paths):
    r, t = one(f, action == "evict")
    res += r
    tot += t
    if each and t >= 1 << 24:
        print(f"  {r / 2**20:9.1f} of {t / 2**20:9.1f} MiB  {f}")
print(
    f"{action}: {res / 2**30:.2f} of {tot / 2**30:.2f} GiB resident ({100 * res / max(tot, 1):.0f}%)"
)
