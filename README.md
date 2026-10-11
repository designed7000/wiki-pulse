# wiki-pulse

Streams Wikipedia's live edit feed through Kafka into Postgres, and tracks edit velocity per page.

Spikes in edit activity often precede news coverage, a Wikipedia page being edited by several people within the same minute usually means something happened. This pipeline captures that signal as it forms.

![Architecture](docs/wiki_pulse.svg)

## Running it

Requires Docker and Python 3 (only to generate two secrets).

```bash
git clone https://github.com/designed7000/wiki-pulse.git
cd wiki-pulse
cp .env.example .env
python3 -c "import base64, os; print('FERNET_KEY=' + base64.urlsafe_b64encode(os.urandom(32)).decode())" >> .env
python3 -c "import secrets; print('AIRFLOW__API_AUTH__JWT_SECRET=' + secrets.token_hex(32))" >> .env
docker compose up -d
```

`.env` holds every password and the two generated secrets, and is gitignored. Nothing secret
lives in `docker-compose.yml`, and Compose refuses to start if a value is missing rather than
falling back to a default. `.env.example` documents each one.

That brings up:
- Kafka and Postgres
- `bootstrap` — creates the `wiki-edits` topic, then exits
- `producer` and `consumer` — the pipeline itself

```bash
docker compose exec db psql -U wiki -d wikipulse -c "SELECT count(*) FROM edits;"
```
Within a few seconds rows start landing...

Query for the pages being edited most heavily right now on Wikipedia:

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
Adminer on `:8081` for the database (`PostgreSQL / db / wiki / <POSTGRES_PASSWORD> / wikipulse`).

## What each piece does

**producer** —> holds one long-lived connection to Wikimedia's SSE endpoint.
Filters to:
  - English Wikipedia main-namespace 
  - non-bot edits

After that, it publishes each one to `wiki-edits` keyed by page title. 

**consumer**: 
1. reads the topic, 
2. validates each record
3. writes into `edits` 

Finally, it commits its Kafka offset only after the row is durably in Postgres.

**edits** —> one row per edit. 

`event_id` is the primary key, which is what makes replayed messages harmless.

**topic_velocity**: This is a view, not a table. 

Groups edits by page and one-minute bucket, returning the following:
- edit count
- net byte change
- distinct editor count.

## Design decisions

**Kafka is overkill here** 
55 edits/min fits in one script writing straight to
Postgres. 

The reason I chose it is to have a buffer: kill the consumer, restart it, and nothing is lost.

**Velocity is a view, not a table** 
A counter would double count on replay. A view can't drift
from what it's derived from.

**Postgres commits before Kafka** 
Crash in between and the message comes back, which `ON CONFLICT DO NOTHING` swallows. Reverse the order and the row is gone. I decided we can afford duplicates but not gaps.

**Bad records and bad databases are handled differently** 
A malformed record fails the same way forever, so it's logged and dropped. So is a row Postgres
rejects outright (constraint violation, bad type). A database that is down is temporary: the
consumer rewinds to the failed message and retries with backoff, and after five attempts it exits
so Docker restarts it from the last committed offset. Treat them alike and you either lose data
or block the partition.

The rewind is what prevents a gap. Offsets are committed "up to and including", so skipping a
failed message and committing the next one would lose the failed one for good.

## Known limitations

**`edits` grows without bound**, at roughly 35 MB/day including indexes. There is no retention
policy yet (coming soon), the view's 24-hour window bounds what is read, not what is kept.

**The topic has one partition**, so only one consumer can work it. Keying by title means adding
partitions later is safe, since per-page ordering survives. The topic would need recreating
though. 

**No alerting.** Velocity is queryable but nothing surfaces a spike on its own. Will be addressed in later Phase.

**Single broker, no replication.** Appropriate for local development, not for anything else. For better safety and fault tolerance, more of them would be needed with replication.

## Development

Services run from the host during development, against the containerised infrastructure:

```bash
docker compose up -d kafka db     #infrastructure only
uv sync
python -m wiki_pulse.producer
```

```bash
uv run pytest
```

See `instructions.md` for the full command reference. 

Unit tests cover parsing, filtering, validation, column mapping and the percentile calculation, all pure functions. The consumer loop and the API are tested against small hand-written fakes passed in as arguments. No broker, database or patching is required, and the cases are built to simulate real-world ones.

## What's next

### Phase 2
Use topic_velocity to track down signals. Right now, a spike is visible only if someone runs the query from above.

- Airflow on the stack, orchestrating everything scheduled
- Anomaly detection comparing each page against its own baseline, not a fixed threshold. Three
  edits a minute means nothing on a busy page and a lot on a quiet one
- A retention policy for `edits` db, which currently grows forever
- An LLM pass over flagged spikes to label what happened (e.g. breaking news, edit war, vandalism)
  It only ever sees the flagged handful, never the stream

### Phase 3 deploys the stack to AWS.