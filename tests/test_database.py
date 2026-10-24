import pytest
from mysql.connector import Error

from src import database


class FakeCursor:
    def __init__(self, rowcount=0, failure=None):
        self.rowcount = rowcount
        self.failure = failure
        self.calls = 0
        self.closed = False

    def execute(self, *_args):
        self.calls += 1
        if self.failure:
            raise self.failure

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def is_connected(self):
        return True

    def close(self):
        pass


def event():
    return {
        "event_id": "dedupe-event", "station_id": "BLR", "station_name": "Bengaluru",
        "event_timestamp": "2026-10-24T06:30:00+00:00", "temperature_c": 28.0,
        "humidity_pct": 60.0, "wind_speed_kmh": 8.0,
    }


def test_duplicate_insert_is_committed_without_metrics_update(monkeypatch):
    cursor = FakeCursor(rowcount=0)
    connection = FakeConnection(cursor)
    monkeypatch.setattr(database, "get_connection", lambda: connection)
    database.save_reading(event())
    assert cursor.calls == 1
    assert connection.committed


def test_database_write_error_rolls_back_and_propagates(monkeypatch):
    cursor = FakeCursor(failure=Error("database unavailable"))
    connection = FakeConnection(cursor)
    monkeypatch.setattr(database, "get_connection", lambda: connection)
    with pytest.raises(Error, match="database unavailable"):
        database.save_reading(event())
    assert connection.rolled_back
    assert cursor.closed
