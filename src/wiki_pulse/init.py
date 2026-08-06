'''
Create the Kafka topics the pipeline depends on, then exit.

Runs once at startup, before the producer and consumer. Relying on Kafka's
auto-creation instead would leave partition count and retention to whatever the
cluster defaults happen to be, and auto-creation is commonly disabled outside
local development.

Idempotent: an existing topic is not an error, so this runs safely on every
`docker compose up`. Exits non-zero only on a genuine failure, which is what the
`service_completed_successfully` dependency in docker-compose.yml checks.
'''

import logging
import os
import sys

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("init")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = [t.strip() for t in os.getenv("KAFKA_TOPICS", "wiki-edits").split(",") if t.strip()]
PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
REPLICATION = int(os.getenv("KAFKA_TOPIC_REPLICATION", "1"))


def create_topics(admin: AdminClient, topics: list[str]) -> bool:
    """Create topics if absent. Returns True if all topics exist afterwards."""
    new_topics = [
        NewTopic(topic, num_partitions=PARTITIONS, replication_factor=REPLICATION)
        for topic in topics
    ]
    futures = admin.create_topics(new_topics)

    ok = True
    for topic, future in futures.items():
        try:
            future.result()
            log.info("created topic %s", topic)
        except Exception as exc:
            err = exc.args[0] if exc.args else None
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("topic %s already exists", topic)
            else:
                log.error("failed to create topic %s: %s", topic, exc)
                ok = False
    return ok


def main() -> None:
    log.info("connecting to %s", BOOTSTRAP)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if not create_topics(admin, TOPICS):
        sys.exit(1)
    log.info("all topics ready")


if __name__ == "__main__":
    main()