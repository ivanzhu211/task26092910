"""上层服务入口：冷链接入服务调用：批处理、阈值管理、事件查询。"""
from __future__ import annotations

import bisect
from collections import defaultdict

from .db import connect
from .models import (
    BatchResult,
    EventStatus,
    Reading,
    ReadingType,
    Rejection,
    RiskEvent,
)
from .segments import MAX_GAP_SECONDS, compute_segments

MAX_BATCH_SIZE = 10000
MAX_LATENESS_SECONDS = 72 * 3600  # 比最新读数早72小时以上拒收，正好72小时照收

_TEMP = ReadingType.TEMPERATURE.value


class ColdChainService:
    def __init__(self, db_path: str) -> None:
        self._conn = connect(db_path)

    def close(self) -> None:
        self._conn.close()

    # ---------------------------------------------------------------- 阈值

    def set_threshold(
        self, supplier: str, low: float, high: float, effective_from: float
    ) -> None:
        if not low < high:
            raise ValueError("阈值下限必须小于上限")
        row = self._conn.execute(
            "SELECT MAX(ts) FROM readings WHERE supplier = ?", (supplier,)
        ).fetchone()
        latest = row[0]
        if latest is not None and effective_from <= latest:
            raise ValueError("阈值生效时间必须晚于该供应商已收到的最新读数")
        with self._conn:
            self._conn.execute(
                "INSERT INTO thresholds (supplier, effective_from, low, high)"
                " VALUES (?, ?, ?, ?)",
                (supplier, effective_from, low, high),
            )

    # ---------------------------------------------------------------- 批处理

    def submit_batch(self, readings: list[Reading]) -> BatchResult:
        if len(readings) > MAX_BATCH_SIZE:
            raise ValueError(f"一批最多 {MAX_BATCH_SIZE} 条读数")
        result = BatchResult()
        with self._conn:
            accepted = self._filter_batch(readings, result)
            self._insert_readings(accepted)
            self._recompute_events(accepted)
            result.accepted = len(accepted)
        return result

    def _filter_batch(
        self, readings: list[Reading], result: BatchResult
    ) -> list[Reading]:
        """批内去重、与库内冲突检查、72小时迟到检查，返回可入库的读数。"""
        by_key: dict[tuple, Reading] = {}
        conflict_keys: set[tuple] = set()
        for r in readings:
            key = (r.supplier, r.sensor, r.type.value, r.ts)
            if key in conflict_keys:
                result.rejections.append(Rejection(r, "同批内同一时间戳数值冲突"))
                continue
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = r
            elif prev.value != r.value:
                conflict_keys.add(key)
                del by_key[key]
                result.rejections.append(Rejection(prev, "同批内同一时间戳数值冲突"))
                result.rejections.append(Rejection(r, "同批内同一时间戳数值冲突"))
            else:
                result.duplicates += 1  # 同批重发，只留一条

        # 按 (supplier, sensor) 分组，查库内已收读数
        groups: dict[tuple, list[Reading]] = defaultdict(list)
        for r in by_key.values():
            groups[(r.supplier, r.sensor)].append(r)

        candidates: list[Reading] = []
        for (supplier, sensor), group in groups.items():
            lo = min(r.ts for r in group)
            hi = max(r.ts for r in group)
            existing = {
                (typ, ts): val
                for typ, ts, val in self._conn.execute(
                    "SELECT type, ts, value FROM readings"
                    " WHERE supplier = ? AND sensor = ? AND ts BETWEEN ? AND ?",
                    (supplier, sensor, lo, hi),
                )
            }
            row = self._conn.execute(
                "SELECT MAX(ts) FROM readings WHERE supplier = ? AND sensor = ?",
                (supplier, sensor),
            ).fetchone()
            latest_before = row[0]
            for r in group:
                key = (r.type.value, r.ts)
                if key in existing:
                    if existing[key] == r.value:
                        result.duplicates += 1  # 重发，只留一条
                    else:
                        result.rejections.append(
                            Rejection(r, "与已收读数同一时间戳数值不同")
                        )
                    continue
                if (
                    latest_before is not None
                    and latest_before - r.ts > MAX_LATENESS_SECONDS
                ):
                    result.rejections.append(
                        Rejection(r, "比该传感器已收到的最新读数早72小时以上")
                    )
                    continue
                candidates.append(r)
        return candidates

    def _insert_readings(self, accepted: list[Reading]) -> None:
        thresholds: dict[str, list[tuple[float, float, float]]] = {}
        rows = []
        for r in accepted:
            exceeding = 0
            if r.type is ReadingType.TEMPERATURE:
                if r.supplier not in thresholds:
                    thresholds[r.supplier] = [
                        (eff, low, high)
                        for eff, low, high in self._conn.execute(
                            "SELECT effective_from, low, high FROM thresholds"
                            " WHERE supplier = ? ORDER BY effective_from",
                            (r.supplier,),
                        )
                    ]
                thr = thresholds[r.supplier]
                idx = bisect.bisect_right(thr, (r.ts, float("inf"), float("inf"))) - 1
                if idx >= 0:
                    _, low, high = thr[idx]
                    if r.value > high or r.value < low:
                        exceeding = 1
            rows.append(
                (r.supplier, r.sensor, r.type.value, r.ts, r.value, exceeding)
            )
        self._conn.executemany(
            "INSERT INTO readings (supplier, sensor, type, ts, value, exceeding)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    # ---------------------------------------------------------------- 事件重算

    def _recompute_events(self, accepted: list[Reading]) -> None:
        affected: dict[tuple, list[float]] = defaultdict(list)
        for r in accepted:
            if r.type is ReadingType.TEMPERATURE:
                affected[(r.supplier, r.sensor)].append(r.ts)
        for supplier, sensor in sorted(affected):
            ts_list = affected[(supplier, sensor)]
            self._recompute_sensor(supplier, sensor, min(ts_list), max(ts_list))

    def _recompute_sensor(
        self, supplier: str, sensor: str, lo: float, hi: float
    ) -> None:
        # 向左扩展窗口：覆盖与新区间相连的整段超限串
        left, newer = lo, lo
        for ts, exc in self._conn.execute(
            "SELECT ts, exceeding FROM readings"
            " WHERE supplier = ? AND sensor = ? AND type = ? AND ts < ?"
            " ORDER BY ts DESC",
            (supplier, sensor, _TEMP, lo),
        ):
            if not exc or newer - ts > MAX_GAP_SECONDS:
                break
            left, newer = ts, ts
        # 向右扩展窗口
        right, older = hi, hi
        for ts, exc in self._conn.execute(
            "SELECT ts, exceeding FROM readings"
            " WHERE supplier = ? AND sensor = ? AND type = ? AND ts > ?"
            " ORDER BY ts ASC",
            (supplier, sensor, _TEMP, hi),
        ):
            if not exc or ts - older > MAX_GAP_SECONDS:
                break
            right, older = ts, ts

        window = [
            (rid, ts, exc)
            for rid, ts, exc in self._conn.execute(
                "SELECT id, ts, exceeding FROM readings"
                " WHERE supplier = ? AND sensor = ? AND type = ?"
                " AND ts BETWEEN ? AND ? ORDER BY ts",
                (supplier, sensor, _TEMP, left, right),
            )
        ]
        segments = compute_segments(window)
        self._reconcile_events(supplier, sensor, left, right, segments)

    def _reconcile_events(
        self, supplier: str, sensor: str, left: float, right: float, segments
    ) -> None:
        active = [
            (eid, start, end)
            for eid, start, end in self._conn.execute(
                "SELECT id, start_ts, end_ts FROM events"
                " WHERE supplier = ? AND sensor = ? AND status = ? ORDER BY id",
                (supplier, sensor, EventStatus.ACTIVE.value),
            )
            if end >= left and start <= right  # 段不跨窗口边界，相交即全在窗内
        ]
        event_readings: dict[int, set[int]] = {}
        if active:
            marks = ",".join("?" * len(active))
            for eid, rid in self._conn.execute(
                f"SELECT event_id, reading_id FROM event_readings"
                f" WHERE event_id IN ({marks})",
                [e[0] for e in active],
            ):
                event_readings.setdefault(eid, set()).add(rid)

        claimed: set[int] = set()
        for seg in segments:  # compute_segments 已按起始时间升序
            seg_ids = set(seg.reading_ids)
            chosen = next(
                (
                    eid
                    for eid, _, _ in active
                    if eid not in claimed and event_readings.get(eid, set()) & seg_ids
                ),
                None,
            )
            if chosen is None:
                cur = self._conn.execute(
                    "INSERT INTO events"
                    " (supplier, sensor, start_ts, end_ts, count, status, created_id)"
                    " VALUES (?, ?, ?, ?, ?, ?, 0)",
                    (supplier, sensor, seg.start_ts, seg.end_ts, seg.count,
                     EventStatus.ACTIVE.value),
                )
                eid = cur.lastrowid
                self._conn.execute(
                    "UPDATE events SET created_id = ? WHERE id = ?", (eid, eid)
                )
            else:
                eid = chosen
                claimed.add(eid)
                self._conn.execute(
                    "UPDATE events SET start_ts = ?, end_ts = ?, count = ?"
                    " WHERE id = ?",
                    (seg.start_ts, seg.end_ts, seg.count, eid),
                )
                self._conn.execute(
                    "DELETE FROM event_readings WHERE event_id = ?", (eid,)
                )
            self._conn.executemany(
                "INSERT INTO event_readings (event_id, reading_id) VALUES (?, ?)",
                [(eid, rid) for rid in seg.reading_ids],
            )

        seg_reading_union: set[int] = set()
        for seg in segments:
            seg_reading_union.update(seg.reading_ids)
        for eid, _, _ in active:
            if eid in claimed:
                continue
            status = (
                EventStatus.MERGED
                if event_readings.get(eid, set()) & seg_reading_union
                else EventStatus.INVALID
            )
            self._conn.execute(
                "UPDATE events SET status = ? WHERE id = ?", (status.value, eid)
            )

    # ---------------------------------------------------------------- 查询

    def active_events(self, supplier: str) -> list[RiskEvent]:
        return self._query_events(supplier, only_active=True)

    def all_events(self, supplier: str) -> list[RiskEvent]:
        return self._query_events(supplier, only_active=False)

    def _query_events(self, supplier: str, only_active: bool) -> list[RiskEvent]:
        sql = "SELECT id, supplier, sensor, start_ts, end_ts, count, status FROM events WHERE supplier = ?"
        if only_active:
            sql += " AND status = 'active'"
        sql += " ORDER BY id"
        return [
            RiskEvent(
                id=row[0],
                supplier=row[1],
                sensor=row[2],
                start_ts=row[3],
                end_ts=row[4],
                count=row[5],
                status=EventStatus(row[6]),
            )
            for row in self._conn.execute(sql, (supplier,))
        ]
