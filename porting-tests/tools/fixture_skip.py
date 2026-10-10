"""Fixture-level annotation skip metadata shared by Python tools."""

from collections.abc import Mapping


def skip_reason(doc: Mapping[str, object]) -> str | None:
    """Return the optional reason; reject mistyped or blank skip declarations."""
    if "skip_reason" not in doc:
        return None
    reason = doc["skip_reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("skip_reason must be a non-empty string")
    return reason
