# Design notes

The README says what wiki-pulse does and how to run it. This file records the reasoning that is
not visible in the code. Several things that look like oversights are deliberate, and several that
look arbitrary were arrived at empirically.

## Pipeline

**Kafka is overkill at this volume, deliberately.** About 55 edits a minute would fit in one script
writing straight to Postgres. Kafka buys durable buffering (kill the consumer, restart it, nothing
is lost) and the ability to add a second consumer without touching the producer.

**The producer is the only component that touches the outside world.** Everything downstream reads
from Kafka. If Wikimedia changes its API, only the producer changes.

**Messages are keyed by page title.** Kafka guarantees ordering within a partition, so per-page
ordering survives adding partitions later. The topic has one partition today, which caps it at a
single consumer, but the keying already anticipates scaling out.

**`event_id` is the primary key on `edits`.** It comes from `meta.id`, and it is what makes Kafka
replays harmless: `ON CONFLICT (event_id) DO NOTHING` absorbs duplicates.

**Postgres commits before Kafka.** A crash between the two redelivers the message, which the
conflict clause swallows. The reverse order would acknowledge a message that was never stored.
Duplicates are cheap; gaps are not.

**Filtering happens in the producer, before publishing.** English Wikipedia, main namespace, not a
bot, and a `type` of `edit` or `new`. The `type` filter matters: `log` events lack the `length`
field entirely, and `categorize` is high-volume noise.

**`length.old` is absent on new pages**, so it is read with `.get()`. This was a real crash, not a
hypothetical.

## Validation and failure handling

**Validation is plain Python, not Great Expectations.** GE validates batches, with a persisted
results store and versioned suites. Applying it per message in a stream would use a fraction of the
tool for a heavy dependency and a config directory. It would fit if batch loads into a warehouse
are ever added.

**Failures are classified, not caught uniformly.**

| Failure | Kind | What the consumer does |
|---|---|---|
| Undecodable JSON, failed validation | Deterministic | Logs the record, commits the offset, drops it |
| Row rejected by Postgres (constraint, bad type) | Deterministic | The same |
| Database down, connection lost | Transient | Rolls back, seeks back to the failed message, retries with exponential backoff, and exits after five attempts so the container restarts from the last committed offset |
| Anything else | A bug | Lets it escape the loop and kill the consumer |

Leaving a deterministic failure uncommitted would block the partition behind a message that can
never succeed. Dying on anything unclassified is intentional: a bug in the mapping code should be
loud, not silently skipped one message at a time.

The subtlety is in the transient case. Offsets are committed "up to and including", so skipping a
failed message and committing a later one loses the failed one permanently. Seeking back is what
actually prevents the gap, and a long outage shows up as a visible restart loop.

**Required and nullable fields mirror the schema.** `to_insert_params` uses bracket access for
`NOT NULL` columns, which fails fast in Python with a named key, and `.get()` for nullable ones.
They are different on purpose.

## Detection

**`topic_velocity` is a view, not a maintained table.** An incrementing counter would double count
on replay even though `edits` deduplicates correctly, because the increment and the insert are
separate operations. A derived view cannot drift from its source, and the read cost is negligible
at this scale.

**Buckets are hourly, and that was arrived at empirically.** The view originally used one-minute
buckets. That produced zero multi-editor buckets across the whole dataset, while 539 pages had
multiple distinct editors overall and 378 page-hours had multiple editors. Collaboration is common;
it just does not happen inside 60 seconds. Switching to hourly immediately surfaced real events.

Wider is not better without evidence. A wider window raises recall but destroys intensity: 15 edits
in an hour means something, 15 across six hours is one edit every 24 minutes. Something between a
minute and an hour (10 to 15 minutes) is probably better, and is a tuning question for later.

The bucket-boundary problem is unsolved. Two editors at 14:59 and 15:01 land in different buckets.
A rolling window would fix it properly.

**The threshold is a global percentile, not a per-page baseline.** Most pages are edited once and
never again, so a per-page baseline has no history to work from and would flag nothing. Worth
revisiting once there is enough history.

**A bucket must also have at least two distinct editors.** This was added after the first live
dashboard showed twelve flagged rows, every one with a single editor, and one page flagged four
separate times. Edit count alone cannot tell one person doing maintenance from several people
converging. A composite weighted score was considered and rejected: tuning weights against a
distribution with no known events in it is guessing with more parameters.

**There is no byte-change condition.** It would exclude edit wars, where the net change is near
zero but the activity is real.

**`anomalies` stores the settings that produced each row**: `threshold`, `percentile`,
`lookback_hours` and `min_editors`. All four are tunable, so without them a historical flag cannot
be interpreted.

**The DAG works from `data_interval_end`, not `now()`**, so a run is defined by its schedule and
not by when it happened to execute. The view itself still filters on `now() - 24 hours`, so this
only holds for recent runs: a backfill of an older day finds nothing to read.

**The detection rule is deliberately not unit-tested.** It lives in the SQL `WHERE` clause.
Mirroring it as a Python function would create something that drifts from what actually runs. It
is verified by observation instead.

## Airflow

**`LocalExecutor`, not Celery.** Celery distributes tasks across machines; this is one machine with
two DAGs. Dropping it removed Redis and the worker containers for no loss.

**Airflow orchestrates workflows, not services.** The producer and consumer are deliberately not
Airflow tasks. A DAG run has a beginning, an end and a success state; a long-lived SSE connection
has none of those.

**Airflow has its own metadata database**, `airflow-db`, so its bookkeeping never mixes with
pipeline data.

**`src/` is mounted into the Airflow containers** with `PYTHONPATH=/opt/airflow/src`, so the DAGs
import the same tested code as the pipeline instead of a copy. This is why `utils.py` has no heavy
imports: `confluent_kafka` and `psycopg` are not in the Airflow image, and the DAG would fail to
parse.

**`airflow-init` calls `/entrypoint airflow db migrate` explicitly.** The upstream Compose file
relies on `_AIRFLOW_DB_MIGRATE`, but overriding the entrypoint is exactly what stops that variable
being read. Running as root for the volume `chown` also means bare `airflow` is not on the path,
hence the `/entrypoint` prefix.

**`config.py` holds the shared constants**, with an `assert` that retention exceeds the anomaly
lookback. Otherwise retention would delete data that detection still reads, and nothing would
error; the numbers would just be wrong. The anomaly DAG's own lookback default is still set in the
DAG, so the two have to be kept in step by hand.

**Retention deletes in batches by `ctid`**, because Postgres has no `DELETE ... LIMIT`. Each batch
commits separately, so a large backlog never holds one long transaction. `anomalies` is
deliberately not pruned: the finding outlives the raw edits.

## API and dashboard

**Routes declare `Depends(get_cursor)`**, so tests replace the data layer through
`app.dependency_overrides`. No patching, no database, and the route code is identical in test and
production.

**`limit` is bounded** with `Query(20, ge=1, le=200)`. FastAPI rejects out-of-range values with a
422 before the handler runs.

**One connection per request.** Simple, and it survives the database restarting. A pool would
matter at scale; it does not here.

**`/timeline` reads `edits` at minute resolution, not `topic_velocity`.** The pulse chart and the
detection logic ask different questions and should not share a grain.

**nginx proxies `/api` to the API service**, so the frontend is same-origin and CORS never arises.
Vite's dev proxy mirrors it locally. The Dockerfile is multi-stage: node builds, nginx serves, and
no dev server ships in the image.

**The dashboard is hand-drawn SVG, not a charting library.** One series, no axes, no legend; a
library would bring its own visual language. The design borrows Wikipedia's revision-history
vocabulary (byte deltas, editor counts, paper and ink) instead of the dark ops-dashboard
convention. Flagged pages invert to paper-on-ink, which is the one bold element; everything else
stays quiet. Colour is functional only: green and red on byte deltas. There is one animation, a
newly flagged row flashing.

## Configuration

**Every password and secret comes from `.env`.** The Compose file holds none, and a missing value
stops Compose instead of falling back to a default.

**Published ports are bound to localhost**, because the stack runs with example passwords.

## Testing

**Most of the logic is pure functions**: parsing, filtering, validation, column mapping and the
percentile calculation. They are tested directly.

**The service loops are tested with hand-written fakes passed in as arguments**, never by patching.
The consumer loop takes its Kafka consumer and its store as parameters, and the API takes its
cursor through a dependency. The whole suite runs without a broker or a database.

The consumer loop is tested because the one data-loss bug this project has had lived there: a
failed write followed by a successful one skipped a message, and no pure-function test could have
caught it.

## Known limitations

**The topic has one partition**, so only one consumer can work it. Keying by title means adding
partitions later is safe, since per-page ordering survives, but the topic would need recreating.

**Single broker, no replication.** Appropriate for local development, not for anything else. Fault
tolerance would need more brokers with replication.

**Buckets are fixed, not rolling.** Two editors at 14:59 and 15:01 land in different buckets and
neither is flagged. A rolling window would fix it.

**The 24-hour window lives in the view.** The anomaly DAG can't look back further than that, and
can't be backfilled for a past day.

**Flags say that something happened, not what.** A flagged page still needs a human to open it.

**Not yet checked against a real news cycle.** Whether multi-editor hours are a good signal needs
days of data across a period when news actually breaks.

**Schema changes are applied by hand.** `schema.sql` only runs on an empty volume. Alembic is the
answer if this grows.

**Local development only.** Example passwords, ports bound to localhost, no TLS.

What's next is in the [README](../README.md#whats-next).
