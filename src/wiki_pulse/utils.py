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
        revision, so it equals the full page size. ``revision_id`` is ``None``
        on events that carry no revision block.
    """
    length = change.get("length", {})
    revision = change.get("revision", {})
    return {
        "event_id": change["meta"]["id"],
        "title": change["title"],
        "user": change["user"],
        "type": change["type"],
        "timestamp": change["meta"]["dt"],
        "revision_id": revision.get("new"),
        "bytes_delta": (length.get("new", 0) - length.get("old", 0)),
        "minor": change.get("minor", False),
        "title_url": change["title_url"],
        "comment": change.get("comment", ""),
    }