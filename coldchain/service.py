"""对上层暴露的库入口：阈值设置、批提交、有效事件查询。

批处理顺序无关性的保证方式：先把整批规范化、去重、校验，再统一落库，
最后按传感器重算事件；任何一步都不依赖批内原始顺序。
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict

from .db import connect
from .engine import Engine
from .models import (
    BatchResult,
    Reading,
    ReadingType,
    Rejection,
    RejectReason,
    RiskEvent,
    TimeLike,
    to_ms,
)
from .repository import Repository

MAX_BATCH_SIZE = 10000
OLD_LIMIT_MS = 72 * 3600 * 1000   # 早于本批前最新读数 72 小时以上拒收，正好 72 小时照收


class ColdChainService:
    def __init__(self, db_path: str) -> None:
        self._conn: sqlite3.Connection = connect(db_path)
        self._repo = Repository(self._conn)
        self._engine = Engine(self._repo)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "ColdChainService":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------- 阈值 ----------

    def set_threshold(self, supplier_id: str, low: float, high: float, effective_from: TimeLike) -> None:
        if not low < high:
            raise ValueError("阈值下限必须小于上限")
        eff = to_ms(effective_from)
        latest_reading = self._repo.supplier_latest_ts(supplier_id)
        if latest_reading is not None and eff <= latest_reading:
            raise ValueError("阈值生效时间必须晚于该供应商已收到的最新读数")
        last_eff = self._repo.latest_threshold_effective_from(supplier_id)
        if last_eff is not None and eff <= last_eff:
            raise ValueError("阈值生效时间必须晚于该供应商已有阈值的生效时间")
        with self._conn:
            self._repo.add_threshold(supplier_id, eff, low, high)

    # ---------- 批提交 ----------

    def submit_batch(self, readings: list[Reading]) -> BatchResult:
        if len(readings) > MAX_BATCH_SIZE:
            raise ValueError(f"每批最多 {MAX_BATCH_SIZE} 条读数，本批 {len(readings)} 条")

        result = BatchResult()
        # 1) 规范化 + 批内去重：同 (供应商, 传感器, 类型, 时间戳) 数值不同的全部拒收
        groups: dict[tuple[str, str, str, int], list[Reading]] = defaultdict(list)
        for r in readings:
            groups[(r.supplier_id, r.sensor_id, r.type.value, to_ms(r.ts))].append(r)

        candidates: list[tuple[str, str, str, int, float, Reading]] = []
        for (supplier, sensor, rtype, ts), group in groups.items():
            values = {r.value for r in group}
            if len(values) > 1:
                for r in group:
                    result.rejected.append(Rejection(reading=r, reason=RejectReason.CONFLICT_IN_BATCH))
            else:
                candidates.append((supplier, sensor, rtype, ts, group[0].value, group[0]))
        # 固定处理顺序，保证结果与批内顺序无关
        candidates.sort(key=lambda c: (c[0], c[1], c[2], c[3]))

        # 2) 72 小时规则的基准：本批之前各传感器已收到的最新温度读数（落库前一次性取好）
        temp_sensors = {(s, n) for s, n, t, _, _, _ in candidates if t == ReadingType.TEMPERATURE.value}
        latest_before: dict[tuple[str, str], int | None] = {
            key: self._repo.latest_reading_ts(key[0], key[1], ReadingType.TEMPERATURE.value)
            for key in temp_sensors
        }

        # 3) 校验并落库
        new_temp_ts: dict[tuple[str, str], list[int]] = defaultdict(list)
        with self._conn:
            for supplier, sensor, rtype, ts, value, original in candidates:
                stored = self._repo.get_reading(supplier, sensor, rtype, ts)
                if stored is not None:
                    if stored == value:
                        result.duplicates += 1  # 重发：只留一条，静默跳过
                    else:
                        result.rejected.append(Rejection(
                            reading=original,
                            reason=RejectReason.CONFLICT_WITH_STORED,
                        ))
                    continue
                if rtype == ReadingType.TEMPERATURE.value:
                    latest = latest_before[(supplier, sensor)]
                    if latest is not None and latest - ts > OLD_LIMIT_MS:
                        result.rejected.append(Rejection(
                            reading=original,
                            reason=RejectReason.TOO_OLD,
                        ))
                        continue
                    threshold = self._repo.threshold_at(supplier, ts)
                    exceeding = threshold is not None and (value < threshold[0] or value > threshold[1])
                    self._repo.insert_reading(supplier, sensor, rtype, ts, value, exceeding)
                    new_temp_ts[(supplier, sensor)].append(ts)
                else:
                    self._repo.insert_reading(supplier, sensor, rtype, ts, value, None)
                result.accepted += 1

            # 4) 逐传感器重算超限段并调和事件
            for (supplier, sensor), ts_list in sorted(new_temp_ts.items()):
                rec = self._engine.reconcile(supplier, sensor, sorted(ts_list))
                result.events_created.extend(rec.created)
                result.events_updated.extend(rec.updated)
                result.events_revoked.extend(rec.revoked)

        return result

    # ---------- 查询 ----------

    def active_events(self, supplier_id: str) -> list[RiskEvent]:
        """供应商当前有效事件：走 (supplier_id, status) 索引，即时返回。"""
        return self._repo.active_events_for_supplier(supplier_id)
