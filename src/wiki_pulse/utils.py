import math
from datetime import UTC, datetime
from typing import Any

REQUIRED_FIELDS = ("event_id", "title", "user", "type", "timestamp", "bytes_delta")
WIKIPEDIA_PERIOD = datetime(2001, 1, 15, tzinfo=UTC)
FUTURE_TOLERANCE_SECS = 300


def _percentile(N: list[int], percent: float) -> float | None:
    """Return the value at the given percentile using linear interpolation.

    N must already be sorted — the public wrapper handles that.
    percent is a fraction between 0.0 and 1.0, not 0-100.

    Returns None for an empty list.
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return (N[int(k)])
    d0 = (N[int(f)]) * (c-k)
    d1 = (N[int(c)]) * (k-f)
    return d0+d1


def is_wanted(change: dict[str, Any]) -> bool:
    """Return True for non-bot edits to English Wikipedia articles (namespace 0)."""
    if change.get('server_name') != "en.wikipedia.org":
        return False
    if change.get('bot', False):
        return False
    if change.get('namespace') != 0:
        return False
    if change.get('type') not in ("edit", "new"):  # noqa: SIM103
        return False
    return True

def to_record(change: dict[str, Any]) -> dict[str, Any]:
    """Project a raw recentchange event into the wiki-edits record shape.

    Extracts only the fields consumed downstream, discarding Wikimedia
    internals and the fields already constrained by ``is_wanted``.

    Args:
        change: A parsed ``mediawiki.recentchange`` event. Assumed to have
            passed ``is_wanted``, so ``server_name``, ``namespace``, ``bot``
            and ``type`` are known-good and not carried into the record.

    Returns:
        A flat dict with ``event_id`` as the deduplication key. ``bytes_delta``
        is the size change in bytes; for ``type == "new"`` there is no previous
        revision, so it equals the full page size. 
        on events that carry no revision block.
    """
    length = change.get("length", {})
    return {
        "event_id": change["meta"]["id"],
        "title": change["title"],
        "user": change["user"],
        "type": change["type"],
        "timestamp": change["meta"]["dt"],
        "bytes_delta": (length.get("new", 0) - length.get("old", 0)),
        "minor": change.get("minor", False),
        "title_url": change["title_url"],
        "comment": change.get("comment", ""),
    }
    
def to_insert_params(record: dict[str, Any]) -> tuple[Any, ...]:
    """Map a wiki-edits message to the edits table column order."""
    return (
        record["event_id"],
        record["title"],
        record["user"],
        record["type"],
        record["timestamp"],
        record["bytes_delta"],
        record.get("minor", False), # these 3 last columns
        record.get("title_url"), # are allowed by the schema to be empty
        record.get("comment"),
    )
    
    
#TODO: Validations
def validate(record: dict[str, Any]) -> str | None:
    """Return a failure reason, or None if the record is valid."""
    for field in REQUIRED_FIELDS:
        if record.get(field) is None:
            return f"missing required field: {field}"
    
    if not str(record["title"]).strip():
        return "title is empty"
    
        
    if not isinstance(record.get('bytes_delta'), int) or isinstance(record["bytes_delta"], bool):
        return f"bytes_delta is not an integer: {record['bytes_delta']!r}"
    
    try:
        ts = datetime.fromisoformat(record["timestamp"])
    except (ValueError, TypeError):
        return f"timestamp is not a valid ISO timestamp: {record['timestamp']!r}"

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    if ts < WIKIPEDIA_PERIOD:
        return f"timestamp predates Wikipedia: {ts.isoformat()}"
    if (ts - now).total_seconds() > FUTURE_TOLERANCE_SECS:
        return f"timestamp is in the future: {ts.isoformat()}"

    return None 
    
def threshold_at_percentile(counts: list[int], percentile: float) -> float | None:
    """Return the edit_count cutoff at the given percentile (0-100), or None if empty."""
    if not counts:
        return None
    return _percentile(sorted(counts), percentile / 100)