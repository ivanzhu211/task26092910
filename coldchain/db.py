"""SQLite 连接与表结构。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier  TEXT NOT NULL,
    sensor    TEXT NOT NULL,
    type      TEXT NOT NULL,
    ts        REAL NOT NULL,
    value     REAL NOT NULL,
    exceeding INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_key
    ON readings (supplier, sensor, type, ts);
CREATE INDEX IF NOT EXISTS idx_readings_sensor_ts
    ON readings (supplier, sensor, ts);
CREATE INDEX IF NOT EXISTS idx_readings_supplier_ts
    ON readings (supplier, ts);

CREATE TABLE IF NOT EXISTS thresholds (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier       TEXT NOT NULL,
    effective_from REAL NOT NULL,
    low            REAL NOT NULL,
    high           REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_thresholds_supplier
    ON thresholds (supplier, effective_from);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier   TEXT NOT NULL,
    sensor     TEXT NOT NULL,
    start_ts   REAL NOT NULL,
    end_ts     REAL NOT NULL,
    count      INTEGER NOT NULL,
    status     TEXT NOT NULL DEFAULT 'active',
    created_id INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_supplier_status
    ON events (supplier, status);
CREATE INDEX IF NOT EXISTS idx_events_sensor
    ON events (supplier, sensor, status);

CREATE TABLE IF NOT EXISTS event_readings (
    event_id   INTEGER NOT NULL REFERENCES events(id),
    reading_id INTEGER NOT NULL,
    PRIMARY KEY (event_id, reading_id)
);
CREATE INDEX IF NOT EXISTS idx_event_readings_reading
    ON event_readings (reading_id);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn
