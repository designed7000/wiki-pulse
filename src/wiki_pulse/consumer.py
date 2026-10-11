import json
import logging
import os
import sys
import time
from collections.abc import Callable
from typing import Any

import psycopg
from confluent_kafka import Consumer, TopicPartition

from wiki_pulse.utils import to_insert_params, validate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")
DSN = os.getenv("POSTGRES_DSN", "postgresql://wiki:wiki@localhost:5432/wikipulse")

LOG_EVERY = 500 # log every 500 msgs to see if everything is ok

MAX_DB_ATTEMPTS = 5 # then exit, so the container restarts from the last committed offset
BACKOFF_INITIAL = 1.0 # seconds, doubles after every failed attempt

# Database down or connection lost: the same row will succeed once it is back.
TRANSIENT_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)
# Constraint violation or bad type: the row fails the same way forever.
REJECTED_ERRORS = (psycopg.IntegrityError, psycopg.DataError)

INSERT_SQL = """
    INSERT INTO edits (
        event_id, title, username, change_type, event_ts,
        bytes_delta, minor, title_url, comment
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
""" 

class EditStore:
    """Writes edits to Postgres over one long-lived connection.

    A failed insert leaves the connection rolled back, or discarded if it is
    gone, so the next insert starts clean either way.
    """

    def __init__(
        self, dsn: str, connect: Callable[[str], psycopg.Connection[Any]] = psycopg.connect
    ) -> None:
        self._dsn = dsn
        self._connect = connect
        self._conn: psycopg.Connection[Any] | None = None

    def insert(self, record: dict[str, Any]) -> None:
        if self._conn is None:
            self._conn = self._connect(self._dsn)
        try:
            self._conn.execute(INSERT_SQL, to_insert_params(record))
            self._conn.commit()
        except psycopg.Error:
            self._recover()
            raise

    def _recover(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.rollback()
        except psycopg.Error:
            # rollback only fails when the connection itself is gone, reconnect next time
            self._conn.close()
            self._conn = None

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()


def run(
    consumer: Consumer, store: EditStore, sleep: Callable[[float], None] = time.sleep
) -> None:
    """Consume until interrupted. Exits non-zero if the database stays unavailable."""
    passed = 0
    failed = 0
    attempts = 0 # consecutive failed writes

    try:
        while True:
            msg = consumer.poll(1.0) # means it waits for 1 sec and returns either a msg or None
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue

            # --- decode: malformed JSON is deterministic, drop it ---
            try:
                record = json.loads(msg.value() or b"")
            except (ValueError, TypeError):
                failed += 1
                log.warning("dropping undecodable message: %r", msg.value())
                consumer.commit(message=msg)
                continue

            # --- validate: if returns any failures -> drop it ---
            reason = validate(record)
            if reason is not None:
                failed += 1
                log.warning("dropping invalid record (%s): %r", reason, record)
                consumer.commit(message=msg)
                continue

            # --- write: Postgres first, the Kafka offset only once the row is stored ---
            try:
                store.insert(record)
            except REJECTED_ERRORS:
                failed += 1
                log.exception("dropping record rejected by the database: %r", record)
                consumer.commit(message=msg)
                continue
            except TRANSIENT_ERRORS:
                attempts += 1
                if attempts >= MAX_DB_ATTEMPTS:
                    log.exception("database still failing after %d attempts, exiting", attempts)
                    sys.exit(1)
                delay = BACKOFF_INITIAL * 2 ** (attempts - 1)
                log.exception(
                    "database error, retrying in %.0fs (attempt %d of %d)",
                    delay, attempts, MAX_DB_ATTEMPTS,
                )
                # Offsets commit "up to and including": without rewinding, the next
                # message's commit would carry the offset past this one for good.
                # (a delivered message always has a position; the stubs can't say so)
                consumer.seek(
                    TopicPartition(msg.topic(), msg.partition(), msg.offset())  # type: ignore[arg-type]
                )
                sleep(delay)
                continue

            consumer.commit(message=msg)
            attempts = 0
            passed += 1

            if (passed + failed) % LOG_EVERY == 0:
                log.info("processed %d (passed=%d failed=%d)", passed + failed, passed, failed)

    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        log.info("final counts: passed=%d failed=%d", passed, failed)


def main() -> None:
    consumer = Consumer ({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "wiki-pulse-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC])
    store = EditStore(DSN)

    try:
        run(consumer, store)
    finally:
        store.close()
        consumer.close()

if __name__ == "__main__":
    main()