# wiki-pulse

Streams Wikipedia's live edit feed through Kafka into Postgres, and tracks edit velocity per
page.

Spikes in edit activity often precede news coverage — a page being edited by several people
within the same minute usually means something happened. This pipeline captures that signal as
it forms.

![Architecture](docs/wiki_pulse.svg)

## Running it

Requires Docker.

```bash
git clone https://github.com/designed7000/wiki-pulse.git
cd wiki-pulse
docker compose up -d
```

That brings up Kafka, Postgres, the topic bootstrap job, and both services. Within a few
seconds rows start landing:

```bash
docker compose exec db psql -U wiki -d wikipulse -c "SELECT count(*) FROM edits;"
```

The pages being edited most heavily right now:

```sql
SELECT * FROM topic_velocity ORDER BY edit_count DESC LIMIT 10;
```

```
 title                          | bucket           | edit_count | bytes_delta_sum | distinct_users
--------------------------------+------------------+------------+-----------------+----------------
 Genus                          | 2026-08-01 13:33 |          3 |             -19 |              3
 A Matter of Time Tour          | 2026-08-01 14:00 |          3 |              10 |              2
 Siege of Seringapatam          | 2026-08-01 13:04 |          4 |               1 |              1
```

Two web UIs are included for inspection: kafka-ui on `:8080` for topics and consumer lag,
Adminer on `:8081` for the database (`PostgreSQL / db / wiki / wiki / wikipulse`).

## What each piece does

**producer** — holds one long-lived connection to Wikimedia's SSE endpoint, filters to English
Wikipedia main-namespace non-bot edits, and publishes each one to `wiki-edits` keyed by page
title. The only component touching the outside world.

**consumer** — reads the topic, validates each record, and writes to `edits`. Commits its Kafka
offset only after the row is durably in Postgres.

**edits** — one row per edit. `event_id` is the primary key, which is what makes replayed
messages harmless.

**topic_velocity** — a view, not a table. Groups edits by page and one-minute bucket, returning
edit count, net byte change, and distinct editor count.

## Design decisions

**Kafka is overkill at this volume — deliberately.** ~55 edits/min would fit in a single script
writing straight to Postgres. Kafka buys durable buffering: if the consumer dies, edits accumulate
and resume from the last committed offset. It also lets a second consumer read the same stream
without touching the producer.

**Velocity is a view, not a maintained table.** An incrementing counter would double-count on
replay even though `edits` deduplicates correctly. Deriving it means it can't drift.

**Postgres commits before Kafka.** A crash between the two redelivers the message, which
`ON CONFLICT DO NOTHING` absorbs. The reverse loses the row. Duplicates are cheap, gaps aren't.

**Failures are classified, not caught uniformly.** Malformed records are deterministic — commit
the offset and log. Database errors are transient — leave the offset uncommitted and retry.
Treating both alike means either losing data or blocking the partition forever.

**Validation is plain Python, not Great Expectations.** GE validates batches with a persisted
results store; applying it per-message uses a fraction of the tool for a heavy dependency.

**Parsing, validation, and mapping are pure functions.** The services are thin loops around them,
so the whole suite runs without a broker or a database.

## Known limitations

**`edits` grows without bound**, at roughly 35 MB/day including indexes. There is no retention
policy yet; the view's 24-hour window bounds what is read, not what is kept.

**The topic has one partition**, so throughput is capped at a single consumer. Keying by title
means the partitioning strategy is already correct for scaling out — per-page ordering is
preserved regardless of partition count — but the topic would need repartitioning first.

**No alerting.** Velocity is queryable but nothing surfaces a spike on its own.

**Single broker, no replication.** Appropriate for local development, not for anything else.

## Development

Services run from the host during development, against the containerised infrastructure:

```bash
docker compose up -d kafka db     # infrastructure only
uv sync
python -m wiki_pulse.producer
```

```bash
uv run pytest
```

See `instructions.md` for the full command reference.

Unit tests cover parsing, filtering, validation, and column mapping all pure functions, so no broker, database, or mocking is required.

## What's next

Airflow-orchestrated anomaly detection — comparing current velocity against each page's own
baseline, rather than a fixed threshold — plus a retention policy for `edits`.