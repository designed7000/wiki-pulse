import json 
import logging
import os

import psycopg
from confluent_kafka import Consumer

from utils import to_insert_params

log = logging.getLogger("consumer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")
DSN = os.getenv("POSTGRES_DSN", "postgresql://wiki:wiki@localhost:5432/wikipulse")

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
    
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        try:
            while True:
                msg = consumer.poll(1.0) # means it waits for 1 sec and returns either a msg or None
                if msg is None:
                    continue
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    continue
                
                try: 
                    record = json.loads(msg.value())
                    cur.execute(INSERT_SQL, to_insert_params(record))
                    conn.commit()
                except Exception:
                    conn.rollback()
                    log.exception("skipping unprocessable message")
                    continue
                
                consumer.commit(msg)
        
        except KeyboardInterrupt:
            log.info("shutting down")
        finally:
            consumer.close()

if __name__ == "__main__":
    main()