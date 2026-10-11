# wiki-pulse — command reference

## Running the whole pipeline (containerised)

```bash
cd wiki-pulse
docker compose up -d          # Kafka, Postgres, topic bootstrap, producer, consumer
docker compose ps             # init should show "Exited (0)", the rest running/healthy
```

After changing any Python source, rebuild — the image has the code baked in:

```bash
docker compose build && docker compose up -d
docker compose up -d --build  # same thing in one command
```

## Running services on the host instead

Useful for iterating: edit and rerun in seconds, no rebuild. Stop the containerised
service first so two consumers aren't sharing the group.

```bash
docker compose stop producer consumer   # leave Kafka + Postgres up
source .venv/bin/activate
uv sync                                 # match uv.lock (after pulling changes)
python -m wiki_pulse.producer
python -m wiki_pulse.consumer
python -m wiki_pulse.bootstrap          # create topics manually
```

Host code defaults to `localhost:9092`; containers use `kafka:29092` via env vars.
The host default for Postgres assumes the `.env.example` password; if you changed it, export
`POSTGRES_DSN` before running the consumer or the API.

## Tests and linting

```bash
uv run pytest
uv run ruff check .
uv run mypy src
```

## Shutting down

```bash
docker compose down           # stops containers, keeps data in volumes
deactivate                    # exits the venv
```

## Postgres

```bash
docker compose exec db psql -U wiki -d wikipulse                          # interactive session
docker compose exec db psql -U wiki -d wikipulse -c "\d edits"            # describe a table
docker compose exec db psql -U wiki -d wikipulse -c "\d+ topic_velocity"  # describe the view
docker compose exec db psql -U wiki -d wikipulse -c "SELECT count(*) FROM edits;"
```

Inside psql: `\dt` lists tables, `\q` quits.

Hot pages right now:

```sql
SELECT * FROM topic_velocity ORDER BY edit_count DESC LIMIT 10;
```

## Applying schema changes

`initdb.d` only runs on an EMPTY volume, so after editing `sql/schema.sql`:

```bash
docker compose down -v && docker compose up -d          # wipe + re-init (loses all data)
docker compose exec -T db psql -U wiki -d wikipulse < sql/schema.sql   # or apply by hand
```

## Kafka

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group wiki-pulse-consumer --describe        # offsets and consumer lag
```

Lag is the gap between the latest offset and the committed one — the number to watch if
the consumer falls behind the producer.

## When things go wrong

```bash
docker compose logs -f consumer   # tail one service (Ctrl+C stops watching, not the service)
docker compose logs init          # did topic creation succeed?
docker compose logs db            # schema errors on first startup
docker compose ps                 # anything restart-looping?
docker compose restart consumer   # restart a single service
docker compose down -v            # nuclear: also wipes volumes — all data lost
```

## Browser

- `localhost:8080` — kafka-ui (topics, messages, consumer groups)
- `localhost:8081` — Adminer (PostgreSQL / db / wiki / `POSTGRES_PASSWORD` from `.env` / wikipulse)