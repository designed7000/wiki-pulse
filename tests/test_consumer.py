import json

import psycopg
import pytest

from wiki_pulse.consumer import EditStore, run

ETNA = "d7f8ec26-316d-4003-b407-7dd7165ad6a3"
CRICKET = "0b0c2c5e-6a4e-4a0e-9a3d-2f1f0c7b9e11"


def record(event_id, title):
    """A valid wiki-edits record, as the producer publishes it."""
    return {
        "event_id": event_id,
        "title": title,
        "user": "T. E. Meeks",
        "type": "edit",
        "timestamp": "2026-07-25T23:22:14.453Z",
        "bytes_delta": 25,
        "minor": False,
        "title_url": "https://en.wikipedia.org/wiki/" + title.replace(" ", "_"),
        "comment": "copyedit",
    }


class FakeMessage:
    """Stands in for confluent_kafka.Message."""

    def __init__(self, offset, value, topic="wiki-edits", partition=0):
        self._offset = offset
        self._value = value
        self._topic = topic
        self._partition = partition

    def value(self):
        return self._value

    def error(self):
        return None

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


def message(offset, event_id, title):
    return FakeMessage(offset, json.dumps(record(event_id, title)).encode("utf-8"))


def etna_then_cricket():
    return [message(100, ETNA, "Mount Etna"), message(101, CRICKET, "Cricket")]


class FakeConsumer:
    """A one-partition log with Kafka's position semantics.

    poll() hands out the message at the current position and advances past it,
    whether or not the caller managed to process it. Only seek() moves the
    position back. Draining the log raises KeyboardInterrupt, which stops the loop.
    """

    def __init__(self, messages):
        self._log = {m.offset(): m for m in messages}
        self._position = min(self._log)
        self.committed = None

    def poll(self, timeout):
        msg = self._log.get(self._position)
        if msg is None:
            raise KeyboardInterrupt
        self._position += 1
        return msg

    def seek(self, partition):
        self._position = partition.offset

    def commit(self, message):
        # Kafka stores the offset of the next message to read, not the one just read.
        self.committed = message.offset() + 1


class FakeStore:
    """Raises the scripted errors for an event, in order, then accepts the row."""

    def __init__(self, failures=None):
        self._failures = {k: list(v) for k, v in (failures or {}).items()}
        self.attempts = []
        self.rows = []

    def insert(self, record):
        event_id = record["event_id"]
        self.attempts.append(event_id)
        pending = self._failures.get(event_id)
        if pending:
            raise pending.pop(0)
        self.rows.append(event_id)


def outage(times):
    return [psycopg.OperationalError("connection refused") for _ in range(times)]


class TestRun:
    def test_stores_and_commits_each_message(self):
        consumer = FakeConsumer(etna_then_cricket())
        store = FakeStore()

        run(consumer, store, sleep=lambda _: None)

        assert store.rows == [ETNA, CRICKET]
        assert consumer.committed == 102

    def test_undecodable_message_is_dropped_and_committed(self):
        consumer = FakeConsumer([FakeMessage(100, b"{not json")])
        store = FakeStore()

        run(consumer, store, sleep=lambda _: None)

        assert store.attempts == []
        assert consumer.committed == 101

    def test_invalid_record_is_dropped_and_committed(self):
        untitled = {**record(ETNA, "Mount Etna"), "title": "  "}
        consumer = FakeConsumer([FakeMessage(100, json.dumps(untitled).encode("utf-8"))])
        store = FakeStore()

        run(consumer, store, sleep=lambda _: None)

        assert store.attempts == []
        assert consumer.committed == 101

    @pytest.mark.parametrize("error", [psycopg.OperationalError, psycopg.InterfaceError])
    def test_message_that_failed_to_write_is_retried_not_skipped(self, error):
        """The next message's commit must not carry the offset past a failed one."""
        consumer = FakeConsumer(etna_then_cricket())
        store = FakeStore({ETNA: [error("connection lost")]})

        run(consumer, store, sleep=lambda _: None)

        assert store.rows == [ETNA, CRICKET]
        assert consumer.committed == 102

    def test_backoff_doubles_between_attempts(self):
        consumer = FakeConsumer([message(100, ETNA, "Mount Etna")])
        store = FakeStore({ETNA: outage(4)})
        sleeps = []

        run(consumer, store, sleep=sleeps.append)

        assert sleeps == [1.0, 2.0, 4.0, 8.0]
        assert store.rows == [ETNA]

    def test_exits_nonzero_after_five_failed_attempts(self):
        consumer = FakeConsumer([message(100, ETNA, "Mount Etna")])
        store = FakeStore({ETNA: outage(99)})

        with pytest.raises(SystemExit) as exit_info:
            run(consumer, store, sleep=lambda _: None)

        assert exit_info.value.code == 1
        assert store.attempts == [ETNA] * 5

    def test_nothing_is_committed_while_the_database_is_down(self):
        """Restarting from the committed offset must redeliver the failed message."""
        consumer = FakeConsumer(etna_then_cricket())
        store = FakeStore({ETNA: outage(99)})

        with pytest.raises(SystemExit):
            run(consumer, store, sleep=lambda _: None)

        assert consumer.committed is None

    def test_successful_write_resets_the_attempt_budget(self):
        consumer = FakeConsumer(etna_then_cricket())
        store = FakeStore({ETNA: outage(4), CRICKET: outage(4)})

        run(consumer, store, sleep=lambda _: None)

        assert store.rows == [ETNA, CRICKET]

    @pytest.mark.parametrize("error", [psycopg.IntegrityError, psycopg.DataError])
    def test_row_the_database_rejects_is_dropped_and_committed(self, error):
        """A constraint or type failure repeats forever, so retrying would block the partition."""
        consumer = FakeConsumer([message(100, ETNA, "Mount Etna")])
        store = FakeStore({ETNA: [error("rejected") for _ in range(99)]})

        run(consumer, store, sleep=lambda _: None)

        assert store.attempts == [ETNA]
        assert consumer.committed == 101

    def test_unclassified_database_error_stops_the_consumer_uncommitted(self):
        """A missing table is neither a blip nor a bad row, so it must be loud."""
        consumer = FakeConsumer(etna_then_cricket())
        store = FakeStore({ETNA: [psycopg.ProgrammingError('relation "edits" does not exist')]})

        with pytest.raises(psycopg.ProgrammingError):
            run(consumer, store, sleep=lambda _: None)

        assert consumer.committed is None


class FakeConnection:
    """Mimics the parts of psycopg.Connection the store relies on.

    Like Postgres, a failed statement aborts the transaction until rollback(),
    and a lost connection refuses everything afterwards.
    """

    def __init__(self, fail_with=None, lost=False):
        self.closed = False
        self.committed = []
        self._pending = []
        self._fail_with = fail_with
        self._lost = lost
        self._aborted = False

    def execute(self, sql, params):
        if self.closed:
            raise psycopg.OperationalError("the connection is closed")
        if self._aborted:
            raise psycopg.errors.InFailedSqlTransaction("current transaction is aborted")
        if self._fail_with is not None:
            error, self._fail_with = self._fail_with, None
            if self._lost:
                self.closed = True
            else:
                self._aborted = True
            raise error
        self._pending.append(params)

    def commit(self):
        self.committed += self._pending
        self._pending = []

    def rollback(self):
        if self.closed:
            raise psycopg.OperationalError("the connection is closed")
        self._aborted = False
        self._pending = []

    def close(self):
        self.closed = True


def stored_ids(conn):
    return [params[0] for params in conn.committed]


class TestEditStore:
    def test_reconnects_after_the_connection_is_lost(self):
        """Without a fresh connection, no number of retries can succeed."""
        dead = FakeConnection(fail_with=psycopg.OperationalError("server closed"), lost=True)
        fresh = FakeConnection()
        pool = [dead, fresh]
        store = EditStore("postgresql://unused", connect=lambda dsn: pool.pop(0))

        with pytest.raises(psycopg.OperationalError):
            store.insert(record(ETNA, "Mount Etna"))
        store.insert(record(ETNA, "Mount Etna"))

        assert stored_ids(fresh) == [ETNA]

    def test_keeps_the_connection_usable_after_a_rejected_row(self):
        conn = FakeConnection(fail_with=psycopg.IntegrityError("violates check constraint"))
        pool = [conn]
        store = EditStore("postgresql://unused", connect=lambda dsn: pool.pop(0))

        with pytest.raises(psycopg.IntegrityError):
            store.insert(record(ETNA, "Mount Etna"))
        store.insert(record(CRICKET, "Cricket"))

        assert stored_ids(conn) == [CRICKET]
