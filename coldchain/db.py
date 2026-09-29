"""SQLite 连接与建表。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id TEXT NOT NULL,
    sensor_id   TEXT NOT NULL,
    type        TEXT NOT NULL,
    ts          INTEGER NOT NULL,          -- Unix 毫秒
    value       REAL NOT NULL,
    exceeding   INTEGER                  -- 仅温度：1 超限 / 0 未超限；入库时按当时阈值判定，之后不变
);
-- 去重与按传感器+时间窗口查询都走这个索引
CREATE UNIQUE INDEX IF NOT EXISTS ux_readings ON readings (supplier_id, sensor_id, type, ts);

CREATE TABLE IF NOT EXISTS thresholds (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id    TEXT NOT NULL,
    effective_from INTEGER NOT NULL,       -- Unix 毫秒，生效时刻本身用新阈值
    low            REAL NOT NULL,
    high           REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_thresholds ON thresholds (supplier_id, effective_from);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,  -- id 即生成顺序
    supplier_id   TEXT NOT NULL,
    sensor_id     TEXT NOT NULL,
    start_ts      INTEGER NOT NULL,
    end_ts        INTEGER NOT NULL,
    count         INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',     -- active / revoked
    revoke_reason TEXT                                -- merged / no_longer_valid
);
-- 按供应商查当前有效事件：即时返回
CREATE INDEX IF NOT EXISTS ix_events_supplier_status ON events (supplier_id, status);
-- 按传感器查有效事件（批内调和用）
CREATE INDEX IF NOT EXISTS ix_events_sensor_status ON events (supplier_id, sensor_id, status);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn
