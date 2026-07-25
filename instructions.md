# Terminal 1 — infrastructure
cd wiki-pulse
docker compose up -d          # starts Kafka + Postgres in background
docker compose ps             # confirm both show "healthy"

# Terminal 2 — Python work
cd wiki-pulse
source .venv/bin/activate     # activate venv — prompt shows (.venv)
python ingestion/producer.py  # run whatever you're working on

# For shutting down
docker compose down           # stops containers, keeps data in volumes
deactivate                    # exits the venv

# Usefull for when things go wrong
docker compose logs -f kafka  # tail one service's logs (Ctrl+C to stop watching)
docker compose restart kafka  # restart a single service
docker compose down -v        # nuclear: also wipes volumes — you lose all data