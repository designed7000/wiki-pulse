import json 
import logging
import os

import psycopg
from confluent_kafka import Consumer

from utils import to_insert_params, validate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")
DSN = os.getenv("POSTGRES_DSN", "postgresql://wiki:wiki@localhost:5432/wikipulse")

LOG_EVERY = 500 # log every 500 msgs to see if everything is ok

INSERT_SQL = """
    INSERT INTO edits (
        event_id, title, username, change_type, event_ts,
        bytes_delta, minor, title_url, comment
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
""" 

def main() -> None:
    consumer = Consumer ({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "wiki-pulse-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC])
    
    passed = 0
    failed = 0
    
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
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
                    record = json.loads(msg.value())
                except (ValueError, TypeError):
                    failed += 1
                    log.warning("dropping undecodable message: %r", msg.value())
                    consumer.commit(msg)
                    continue
                
                # --- validate: if returns any failures -> drop it ---
                reason = validate(record)
                if reason is not None:
                    failed += 1
                    log.warning("dropping invalid record (%s): %r", reason, record)
                    consumer.commit(msg)
                    continue
                
                # --- commit the msg
                try:
                    cur.execute(INSERT_SQL, to_insert_params(record))
                    conn.commit()
                except psycopg.Error:
                    conn.rollback()
                    log.exception("database error, message will be retried")
                    continue
                
                consumer.commit(msg)
                passed += 1

                if (passed + failed) % LOG_EVERY == 0:
                    log.info("processed %d (passed=%d failed=%d)", passed + failed, passed, failed)
                    
        except KeyboardInterrupt:
            log.info("shutting down")
        finally:
            log.info("final counts: passed=%d failed=%d", passed, failed)
            consumer.close()

if __name__ == "__main__":
    main()