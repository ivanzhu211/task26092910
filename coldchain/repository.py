"""数据访问层：读数、阈值、事件的存取。所有查询都走索引，耗时不随历史变长而增加。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .models import EventStatus, ReadingType, RevokeReason, RiskEvent, TimestampMs

TEMP = ReadingType.TEMPERATURE.value


@dataclass
class ReadingRow:
    ts: TimestampMs
    value: float
    exceeding: bool


class Repository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ---------- 读数 ----------

    def get_reading(self, supplier: str, sensor: str, rtype: str, ts: TimestampMs) -> float | None:
        """按唯一键查已收读数的数值，不存在返回 None。"""
        row = self._conn.execute(
            "SELECT value FROM readings WHERE supplier_id=? AND sensor_id=? AND type=? AND ts=?",
            (supplier, sensor, rtype, ts),
        ).fetchone()
        return None if row is None else row["value"]

    def latest_reading_ts(self, supplier: str, sensor: str, rtype: str) -> TimestampMs | None:
        row = self._conn.execute(
            "SELECT MAX(ts) AS m FROM readings WHERE supplier_id=? AND sensor_id=? AND type=?",
            (supplier, sensor, rtype),
        ).fetchone()
        return row["m"]

    def supplier_latest_ts(self, supplier: str) -> TimestampMs | None:
        """供应商已收到的最新读数时间（不限类型、不限传感器）。"""
        row = self._conn.execute(
            "SELECT MAX(ts) AS m FROM readings WHERE supplier_id=?", (supplier,)
        ).fetchone()
        return row["m"]

    def insert_reading(
        self, supplier: str, sensor: str, rtype: str, ts: TimestampMs, value: float, exceeding: bool | None
    ) -> None:
        self._conn.execute(
            "INSERT INTO readings (supplier_id, sensor_id, type, ts, value, exceeding) VALUES (?,?,?,?,?,?)",
            (supplier, sensor, rtype, ts, value, None if exceeding is None else int(exceeding)),
        )

    def temp_readings_window(
        self, supplier: str, sensor: str, lo: TimestampMs, hi: TimestampMs
    ) -> list[ReadingRow]:
        """[lo, hi] 闭区间内的温度读数，按时间升序。"""
        rows = self._conn.execute(
            "SELECT ts, value, exceeding FROM readings"
            " WHERE supplier_id=? AND sensor_id=? AND type=? AND ts BETWEEN ? AND ? ORDER BY ts",
            (supplier, sensor, TEMP, lo, hi),
        ).fetchall()
        return [ReadingRow(ts=r["ts"], value=r["value"], exceeding=bool(r["exceeding"])) for r in rows]

    def prev_temp_reading(self, supplier: str, sensor: str, ts: TimestampMs) -> ReadingRow | None:
        row = self._conn.execute(
            "SELECT ts, value, exceeding FROM readings"
            " WHERE supplier_id=? AND sensor_id=? AND type=? AND ts<? ORDER BY ts DESC LIMIT 1",
            (supplier, sensor, TEMP, ts),
        ).fetchone()
        return None if row is None else ReadingRow(ts=row["ts"], value=row["value"], exceeding=bool(row["exceeding"]))

    def next_temp_reading(self, supplier: str, sensor: str, ts: TimestampMs) -> ReadingRow | None:
        row = self._conn.execute(
            "SELECT ts, value, exceeding FROM readings"
            " WHERE supplier_id=? AND sensor_id=? AND type=? AND ts>? ORDER BY ts LIMIT 1",
            (supplier, sensor, TEMP, ts),
        ).fetchone()
        return None if row is None else ReadingRow(ts=row["ts"], value=row["value"], exceeding=bool(row["exceeding"]))

    def has_exceeding_between(
        self, supplier: str, sensor: str, lo: TimestampMs, hi: TimestampMs
    ) -> bool:
        """[lo, hi] 内是否存在超限温度读数（用于判断事件与段是否有共同读数）。"""
        if lo > hi:
            return False
        row = self._conn.execute(
            "SELECT 1 FROM readings WHERE supplier_id=? AND sensor_id=? AND type=?"
            " AND ts BETWEEN ? AND ? AND exceeding=1 LIMIT 1",
            (supplier, sensor, TEMP, lo, hi),
        ).fetchone()
        return row is not None

    # ---------- 阈值 ----------

    def add_threshold(self, supplier: str, effective_from: TimestampMs, low: float, high: float) -> None:
        self._conn.execute(
            "INSERT INTO thresholds (supplier_id, effective_from, low, high) VALUES (?,?,?,?)",
            (supplier, effective_from, low, high),
        )

    def latest_threshold_effective_from(self, supplier: str) -> TimestampMs | None:
        row = self._conn.execute(
            "SELECT MAX(effective_from) AS m FROM thresholds WHERE supplier_id=?", (supplier,)
        ).fetchone()
        return row["m"]

    def threshold_at(self, supplier: str, ts: TimestampMs) -> tuple[float, float] | None:
        """ts 时刻生效的阈值（生效时刻本身用新阈值）；无则 None。"""
        row = self._conn.execute(
            "SELECT low, high FROM thresholds WHERE supplier_id=? AND effective_from<=?"
            " ORDER BY effective_from DESC LIMIT 1",
            (supplier, ts),
        ).fetchone()
        return None if row is None else (row["low"], row["high"])

    # ---------- 事件 ----------

    @staticmethod
    def _to_event(row: sqlite3.Row) -> RiskEvent:
        return RiskEvent(
            id=row["id"],
            supplier_id=row["supplier_id"],
            sensor_id=row["sensor_id"],
            start_ts=row["start_ts"],
            end_ts=row["end_ts"],
            count=row["count"],
            status=EventStatus(row["status"]),
            revoke_reason=None if row["revoke_reason"] is None else RevokeReason(row["revoke_reason"]),
        )

    def active_events_for_sensor(self, supplier: str, sensor: str) -> list[RiskEvent]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE supplier_id=? AND sensor_id=? AND status='active' ORDER BY id",
            (supplier, sensor),
        ).fetchall()
        return [self._to_event(r) for r in rows]

    def active_events_for_supplier(self, supplier: str) -> list[RiskEvent]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE supplier_id=? AND status='active' ORDER BY id", (supplier,)
        ).fetchall()
        return [self._to_event(r) for r in rows]

    def create_event(
        self, supplier: str, sensor: str, start: TimestampMs, end: TimestampMs, count: int
    ) -> RiskEvent:
        cur = self._conn.execute(
            "INSERT INTO events (supplier_id, sensor_id, start_ts, end_ts, count) VALUES (?,?,?,?,?)",
            (supplier, sensor, start, end, count),
        )
        return RiskEvent(id=cur.lastrowid, supplier_id=supplier, sensor_id=sensor,
                         start_ts=start, end_ts=end, count=count)

    def update_event(self, event_id: int, start: TimestampMs, end: TimestampMs, count: int) -> None:
        self._conn.execute(
            "UPDATE events SET start_ts=?, end_ts=?, count=? WHERE id=?", (start, end, count, event_id)
        )

    def revoke_event(self, event_id: int, reason: RevokeReason) -> None:
        self._conn.execute(
            "UPDATE events SET status='revoked', revoke_reason=? WHERE id=?", (reason.value, event_id)
        )
