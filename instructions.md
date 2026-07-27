# Terminal 1 — infrastructure
cd wiki-pulse
docker compose up -d          # starts Kafka + Postgres in background
docker compose ps             # confirm both show "healthy"

# Terminal 2 — Python work
cd wiki-pulse
source .venv/bin/activate     # activate venv — prompt shows (.venv)
uv sync                       # install deps to match uv.lock (after pulling changes)
python -m wiki_pulse.producer # run the producer
pytest                        # run the tests

# Shutting down
docker compose down           # stops containers, keeps data in volumes
deactivate                    # exits the venv

# Postgres
docker compose exec db psql -U wiki -d wikipulse        # interactive psql session
docker compose exec db psql -U wiki -d wikipulse -c "\d edits"           # describe a table
docker compose exec db psql -U wiki -d wikipulse -c "\d+ topic_velocity" # describe the view
docker compose exec db psql -U wiki -d wikipulse -c "SELECT count(*) FROM edits;"

# Applying schema changes
# initdb.d only runs on an EMPTY volume, so after editing sql/schema.sql:
docker compose down -v && docker compose up -d          # wipe + re-init (loses all data)
# or apply by hand without wiping:
docker compose exec -T db psql -U wiki -d wikipulse < sql/schema.sql

# Useful when things go wrong
docker compose logs -f kafka  # tail one service's logs (Ctrl+C to stop watching)
docker compose logs db        # check init script errors on startup
docker compose restart kafka  # restart a single service
docker compose exec db ls /docker-entrypoint-initdb.d  # confirm a bind mount landed
docker compose down -v        # nuclear: also wipes volumes — you lose all data

# Browser
# localhost:8080 — kafka-ui
# localhost:8081 — Adminer (PostgreSQL / db / wiki / wiki / wikipulse)