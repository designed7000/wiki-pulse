import pytest
from fastapi.testclient import TestClient

from wiki_pulse.api import app, get_cursor


class FakeCursor:
    """Returns canned rows in the column order the queries select."""

    def __init__(self, rows):
        self._rows = rows

    def execute(self, sql, params=None):
        self._limit = params[0] if params else None

    def fetchall(self):
        return self._rows[: self._limit]


VELOCITY_ROWS = [
    ("Mount Etna", "2026-08-31T14:03:00Z", 5, 317, 3),
    ("Cricket", "2026-08-31T14:07:00Z", 2, 12, 1),
]

ANOMALY_ROWS = [
    ("Mount Etna", "2026-08-31T14:03:00Z", 5, 317, 3, 2.0, 99.5, 24, "2026-08-31T15:00:00Z"),
]


@pytest.fixture
def client():
    yield TestClient(app)
    app.dependency_overrides.clear()


def override_with(rows):
    def _fake():
        yield FakeCursor(rows)
    return _fake


class TestVelocityEndpoint:
    def test_returns_rows(self, client):
        app.dependency_overrides[get_cursor] = override_with(VELOCITY_ROWS)
        response = client.get("/velocity")
        assert response.status_code == 200
        assert response.json()[0]["title"] == "Mount Etna"

    def test_respects_limit(self, client):
        app.dependency_overrides[get_cursor] = override_with(VELOCITY_ROWS)
        assert len(client.get("/velocity?limit=1").json()) == 1

    @pytest.mark.parametrize("limit", [0, -1, 201, "abc"])
    def test_rejects_invalid_limit(self, client, limit):
        """Bounds are enforced by FastAPI before the handler runs."""
        app.dependency_overrides[get_cursor] = override_with(VELOCITY_ROWS)
        assert client.get(f"/velocity?limit={limit}").status_code == 422


class TestAnomaliesEndpoint:
    def test_returns_rows(self, client):
        app.dependency_overrides[get_cursor] = override_with(ANOMALY_ROWS)
        body = client.get("/anomalies").json()
        assert body[0]["threshold"] == 2.0
        assert body[0]["percentile"] == 99.5

    def test_empty_table_returns_empty_list(self, client):
        app.dependency_overrides[get_cursor] = override_with([])
        assert client.get("/anomalies").json() == []