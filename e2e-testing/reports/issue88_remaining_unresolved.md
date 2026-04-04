# Issue #88/#89 — Remaining unresolved cases

Generated: 2026-04-04
Fixes applied: PR #96 (issue #88), PR #102 (issue #89)

## Status

| Issue | Field | Match Rate | Remaining |
|-------|-------|------------|-----------|
| #88 (42 variants) | HGVSc | **100.0%** (1264/1264) | 0 |
| #88 (42 variants) | HGVSp | **99.4%** (1257/1264) | 7 empty |
| #89 (24 variants) | HGVSp | **100.0%** (259/259) | 0 |
| #89 (24 variants) | HGVSc | **100.0%** (259/259) | 0 |

## Resolved (previously listed here)

- **36 HGVSc dup-vs-ins mismatches** — all fixed by PR #102
  (dup boundary revert + shift-before-dup order + removed false fallback)
- **402 missing transcript entries** — NOT related to #88/#89
  (transcript-variant overlap detection, separate investigation needed)

## 7 remaining HGVSp empties

vepyr produces empty HGVSp where VEP has a value. All tracked in separate issues.

### incomplete_terminal_codon_variant (6 entries — issue #101)

| Variant | Feature | VEP HGVSp |
|---------|---------|-----------|
| chr5:77438259 | ENST00000511791 | `p.Ter121=` |
| chr6:31270214 | ENST00000415537 | `p.Ter262=` |
| chr7:56081717 | ENST00000446428 | `p.Ter118=` |
| chr9:76321576 | ENST00000673745 | `p.Ter185=` |
| chr19:52801310 | ENST00000596559 | `p.Ter176=` |
| chr20:13072370 | ENST00000434210 | `p.Ter140=` |

### Frameshift (1 entry — issue #90)

| Variant | Feature | VEP HGVSp |
|---------|---------|-----------|
| chr12:121626865 | ENST00000617316 | `p.Pro43ThrfsTer43` |
