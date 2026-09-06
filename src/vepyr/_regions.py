"""Turn a Polars predicate into genomic regions for engine pushdown.

The IO plugin behind ``annotate()`` receives the predicate Polars pushed down.
This module extracts the restriction it places on ``chrom``/``start``/``end``
so the engine can skip contigs and seek by position *before* annotating.

Contract: the result is always a superset of the rows the predicate accepts;
Polars still evaluates the full predicate afterwards. Anything this module
does not recognise makes it fail open (``None``, no pushdown).

Walks ``Expr.meta.serialize(format="json")``. That format is documented as
unstable, so every shape read here is pinned by ``tests/test_regions.py``
and any structural surprise is caught and reported as ``None``.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from typing import Any

import polars as pl

GENOMIC_COLUMNS = frozenset({"chrom", "start", "end"})
_RANGE_COLUMNS = frozenset({"start", "end"})
_INT_SCALAR_KEYS = frozenset(
    {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
)
_FLIP = {"Eq": "Eq", "Gt": "Lt", "GtEq": "LtEq", "Lt": "Gt", "LtEq": "GtEq"}
# The engine represents bounds as signed 64-bit integers.
_I64_MAX = 2**63 - 1
# Functions a chrom conjunct may use: elementwise only, so evaluating the
# conjunct against a one-row-per-contig frame gives the same verdict per
# contig as it would per data row. Set-dependent functions (is_duplicated,
# is_unique, is_first_distinct, ...) are deliberately absent.
_CHROM_FUNCTIONS = {
    "Boolean": frozenset({"IsIn", "Not", "IsNull", "IsNotNull"}),
    "StringExpr": frozenset(
        {
            "StartsWith",
            "EndsWith",
            "Contains",
            "Uppercase",
            "Lowercase",
            "StripPrefix",
            "StripSuffix",
            "StripChars",
        }
    ),
}


class _Unrecognised(Exception):
    """A genomic conjunct this module cannot bound safely."""


def extract_regions(
    predicate: pl.Expr, contigs: list[str] | Callable[[], list[str]]
) -> list[dict] | None:
    """Regions the predicate restricts the input to, or ``None`` for no pushdown.

    ``[]`` means the predicate can accept no row at all. ``contigs`` is the
    input's contig list or a callable producing it; the callable is only
    invoked once every conjunct has passed the shape checks, because finding
    the contigs of an unindexed input costs a scan of the file. An empty list
    means the contigs are unknown (no ``##contig`` header and no index), and
    nothing can be proven about it: no pushdown.
    """
    provider = contigs if callable(contigs) else (lambda: contigs)
    cache: dict[str, list[str]] = {}

    def known_contigs() -> list[str]:
        if "value" not in cache:
            cache["value"] = list(provider())
        if not cache["value"]:
            raise _Unrecognised("contigs unknown")
        return cache["value"]

    try:
        tree = json.loads(predicate.meta.serialize(format="json"))
        # Every group's shape is checked before any contig lookup, so no
        # unsupported predicate pays for the contig scan.
        plans = [_plan_group(group) for group in _split(tree, "Or")]
        # Normalise and vet every group before any contig lookup, so no
        # predicate that ends up not pushable pays for the contig scan.
        vetted: list[tuple[list[dict], int | None, int | None]] = []
        for chrom_nodes, lo, hi in plans:
            # Coordinates are 1-based: a lower bound at or below 1 is no
            # bound, an upper bound below 1 accepts nothing. Beyond the
            # engine's i64 range a lower bound accepts nothing and an upper
            # bound is open.
            if lo is not None and lo <= 1:
                lo = None
            if hi is not None and hi > _I64_MAX:
                hi = None
            if not chrom_nodes and lo is None and hi is None:
                # Nothing narrows this group (e.g. only `end >= v`): pushing
                # open regions for every contig would cost the contig scan
                # and the warning for no gain.
                return None
            if hi is not None and hi < 1:
                continue
            if lo is not None and lo > _I64_MAX:
                continue
            if lo is not None and hi is not None and lo > hi:
                continue
            vetted.append((chrom_nodes, lo, hi))
        regions: list[dict] = []
        for chrom_nodes, lo, hi in vetted:
            chroms = _evaluate_group(chrom_nodes, known_contigs())
            regions.extend({"chrom": c, "start": lo, "end": hi} for c in chroms)
        return regions
    except (_Unrecognised, KeyError, TypeError, ValueError, IndexError):
        return None
    except pl.exceptions.PolarsError:  # evaluating a chrom conjunct failed
        return None


def _split(node: dict, op: str) -> list[dict]:
    """Flatten a left-deep chain of ``op`` (``And``/``Or``) into its operands."""
    binary = node.get("BinaryExpr") if isinstance(node, dict) else None
    if binary is not None and binary.get("op") == op:
        return _split(binary["left"], op) + _split(binary["right"], op)
    return [node]


def _plan_group(group: dict) -> tuple[list[dict], int | None, int | None]:
    """Shape-check one conjunction: (chrom conjunct nodes, lower, upper).

    Raises ``_Unrecognised`` when the group has no recognised genomic conjunct
    or holds one it cannot bound, because an ``Or`` over such a group could
    accept any row. Touches no contig list.
    """
    chrom_nodes: list[dict] = []
    lo: int | None = None
    hi: int | None = None
    recognised = False
    for conjunct in _split(group, "And"):
        names = _column_names(conjunct)
        if names == {"chrom"}:
            _gate_chrom_shape(conjunct)
            chrom_nodes.append(conjunct)
            recognised = True
        elif names and names <= _RANGE_COLUMNS:
            c_lo, c_hi = _range_bounds(conjunct)
            if c_lo is not None:
                lo = c_lo if lo is None else max(lo, c_lo)
            if c_hi is not None:
                hi = c_hi if hi is None else min(hi, c_hi)
            recognised = True
        elif names & GENOMIC_COLUMNS:
            raise _Unrecognised(f"mixed conjunct {names}")
        # any other conjunct is a residual Polars applies after annotation
    if not recognised:
        raise _Unrecognised("no genomic conjunct in group")
    return chrom_nodes, lo, hi


def _evaluate_group(chrom_nodes: list[dict], contigs: list[str]) -> list[str]:
    """Evaluate a planned group's chrom conjuncts against the contig list and
    return the matching contigs in contig order."""
    chrom_set: set[str] | None = None
    for node in chrom_nodes:
        frame = pl.DataFrame({"chrom": contigs}, schema={"chrom": pl.String})
        matched = set(frame.filter(_deserialize(node))["chrom"].to_list())
        chrom_set = matched if chrom_set is None else chrom_set & matched
    if chrom_set is None:
        return list(contigs)
    return [c for c in contigs if c in chrom_set]


def _column_names(node: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "Column" and isinstance(value, str):
                names.add(value)
            else:
                names |= _column_names(value)
    elif isinstance(node, list):
        for item in node:
            names |= _column_names(item)
    return names


def _gate_chrom_shape(node: Any) -> None:
    """Allow only elementwise shapes so evaluation on a contig table is
    faithful: columns, literals, binary operators, and the functions listed in
    ``_CHROM_FUNCTIONS``. Aggregations, windows, casts, set-dependent
    functions and the rest raise.
    """
    if not isinstance(node, dict) or len(node) != 1:
        raise _Unrecognised("node shape")
    ((kind, payload),) = node.items()
    if kind == "Column":
        if payload != "chrom":
            raise _Unrecognised("column")
    elif kind == "Literal":
        return
    elif kind == "BinaryExpr":
        _gate_chrom_shape(payload["left"])
        _gate_chrom_shape(payload["right"])
    elif kind == "Function":
        function = payload["function"]
        if not isinstance(function, dict) or len(function) != 1:
            raise _Unrecognised("function shape")
        ((family, variant),) = function.items()
        name = next(iter(variant)) if isinstance(variant, dict) else variant
        if name not in _CHROM_FUNCTIONS.get(family, frozenset()):
            raise _Unrecognised(f"function {family}.{name}")
        for item in payload["input"]:
            _gate_chrom_shape(item)
    else:
        raise _Unrecognised(kind)


def _deserialize(node: dict) -> pl.Expr:
    return pl.Expr.deserialize(io.BytesIO(json.dumps(node).encode()), format="json")


def _literal_int(node: Any) -> int:
    literal = node["Literal"]
    if "Dyn" in literal and "Int" in literal["Dyn"]:
        return int(literal["Dyn"]["Int"])
    scalar = literal.get("Scalar")
    if isinstance(scalar, dict) and len(scalar) == 1:
        ((dtype, value),) = scalar.items()
        if dtype in _INT_SCALAR_KEYS:
            return int(value)
    raise _Unrecognised("literal")


def _range_bounds(node: dict) -> tuple[int | None, int | None]:
    """Bounds on ``start`` implied by one ``start``/``end`` conjunct."""
    if "BinaryExpr" in node:
        binary = node["BinaryExpr"]
        op = binary["op"]
        left, right = binary["left"], binary["right"]
        if "Column" in left and "Literal" in right:
            column, value = left["Column"], _literal_int(right)
        elif "Literal" in left and "Column" in right:
            column, value = right["Column"], _literal_int(left)
            op = _FLIP.get(op, op)
        else:
            raise _Unrecognised("binary operands")
        if op not in _FLIP:
            raise _Unrecognised(f"operator {op}")
        if column == "start":
            return {
                "Eq": (value, value),
                "Gt": (value + 1, None),
                "GtEq": (value, None),
                "Lt": (None, value - 1),
                "LtEq": (None, value),
            }[op]
        # column == "end": only an upper bound carries over to start
        return {
            "Eq": (None, value),
            "Lt": (None, value - 1),
            "LtEq": (None, value),
            "Gt": (None, None),
            "GtEq": (None, None),
        }[op]
    if "Function" in node:
        function = node["Function"]
        between = function["function"].get("Boolean", {}).get("IsBetween")
        inputs = function["input"]
        if between is None or len(inputs) != 3 or "Column" not in inputs[0]:
            raise _Unrecognised("function")
        low, high = _literal_int(inputs[1]), _literal_int(inputs[2])
        closed = between["closed"]
        if closed in ("Left", "None"):
            high -= 1
        if closed in ("Right", "None"):
            low += 1
        if inputs[0]["Column"] == "start":
            return low, high
        return None, high
    raise _Unrecognised("range node")
