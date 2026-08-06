from datetime import datetime, timezone

REQUIRED_FIELDS = ("event_id", "title", "user", "type", "timestamp", "bytes_delta")

WIKIPEDIA_PERIOD = datetime(2001, 1, 15, tzinfo=timezone.utc)
FUTURE_TOLERANCE_SECS = 300

def is_wanted(change: dict) -> bool:
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

def to_record(change: dict) -> dict:
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
    
def to_insert_params(record: dict) -> tuple:
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
def validate(record: dict) -> str | None:
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
        ts = ts.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    if ts < WIKIPEDIA_PERIOD:
        return f"timestamp predates Wikipedia: {ts.isoformat()}"
    if (ts - now).total_seconds() > FUTURE_TOLERANCE_SECS:
        return f"timestamp is in the future: {ts.isoformat()}"

    return None 
    
    
    