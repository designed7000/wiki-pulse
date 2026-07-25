import json
import logging
import os
import time

from confluent_kafka import Producer
from requests_sse import EventSource

from wiki_pulse.utils import is_wanted, to_record

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-edits")

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "wiki-pulse/0.1 (alex@example.com)"}

BACKOFF_INITIAL = 1.0
BACKOFF_MAX = 60.0
HEALTHY_AFTER = 60.0
BUFFER_RETRIES = 3
FLUSH_TIMEOUT = 10.0
LOG_EVERY = 1000

producer = Producer({"bootstrap.servers": BOOTSTRAP})
published = 0


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed for %s: %s", msg.key(), err)


def publish(change: dict) -> None:
    """Project and publish one event. Never raises for a single bad record."""
    global published
    try:
        record = to_record(change)
        key = record["title"].encode("utf-8")
        value = json.dumps(record).encode("utf-8")
    except Exception:
        log.exception("skipping unprocessable event")
        return

    for attempt in range(BUFFER_RETRIES):
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
        except BufferError:
            log.warning("local queue full, draining (attempt %d)", attempt + 1)
            producer.poll(1)
        except Exception:
            log.exception("produce failed, dropping event")
            return
        else:
            published += 1
            if published % LOG_EVERY == 0:
                log.info("published %d events", published)
            return

    log.error("dropping event after %d buffer retries: %s", BUFFER_RETRIES, key)


def consume_stream() -> None:
    """Read the SSE stream and publish matching events until it ends."""
    with EventSource(URL, headers=HEADERS) as stream:
        for event in stream:
            if event.type != "message":
                continue
            try:
                change = json.loads(event.data)
            except ValueError:
                log.warning("skipping malformed event payload")
                continue

            if not is_wanted(change):
                continue

            publish(change)
            producer.poll(0)


def main() -> None:
    backoff = BACKOFF_INITIAL
    try:
        while True:
            started = time.monotonic()
            try:
                log.info("connecting to %s", URL)
                consume_stream()
                log.warning("stream ended cleanly")
            except Exception:
                log.exception("stream failed")

            if time.monotonic() - started >= HEALTHY_AFTER:
                backoff = BACKOFF_INITIAL

            log.info("reconnecting in %.1fs", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        log.info("flushing %d queued messages", len(producer))
        remaining = producer.flush(FLUSH_TIMEOUT)
        if remaining:
            log.error("%d messages undelivered at shutdown", remaining)
            
if __name__ == "__main__":
    main()