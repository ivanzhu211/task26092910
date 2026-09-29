"""判定引擎：新读数落库后，对受影响传感器按时间重算超限段并调和风险事件。

核心不变量：每批处理完后，有效事件与该传感器当前"满 3 条的超限段"一一对应。
重算只覆盖受影响窗口（新读数向两侧沿超限链扩展到段边界），不扫描全量历史。
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import RevokeReason, RiskEvent, TimestampMs
from .repository import Repository

GAP_MS = 15 * 60 * 1000        # 相邻温度读数间隔超过 15 分钟即断开，正好 15 分钟不断
MIN_SEGMENT = 3                # 连续超限满 3 条成段


@dataclass
class Segment:
    start: TimestampMs
    end: TimestampMs
    count: int


@dataclass
class ReconcileResult:
    created: list[RiskEvent]
    updated: list[RiskEvent]
    revoked: list[RiskEvent]


class Engine:
    def __init__(self, repo: Repository) -> None:
        self._repo = repo

    # ---------- 窗口扩展 ----------

    def _expand_window(self, supplier: str, sensor: str, lo: TimestampMs, hi: TimestampMs) -> tuple[int, int]:
        """从 [lo, hi] 向两侧扩展：只要相邻读数间隔 <=15 分钟且相邻侧读数超限就继续，
        直到段边界（无读数 / 间隔 >15 分钟 / 相邻读数未超限）。窗口因此完整覆盖
        所有与新读数同属一条超限链的历史读数。"""
        while True:
            prev = self._repo.prev_temp_reading(supplier, sensor, lo)
            if prev is None or lo - prev.ts > GAP_MS or not prev.exceeding:
                break
            lo = prev.ts
        while True:
            nxt = self._repo.next_temp_reading(supplier, sensor, hi)
            if nxt is None or nxt.ts - hi > GAP_MS or not nxt.exceeding:
                break
            hi = nxt.ts
        return lo, hi

    # ---------- 段重算 ----------

    def _compute_segments(self, supplier: str, sensor: str, lo: TimestampMs, hi: TimestampMs) -> list[Segment]:
        rows = self._repo.temp_readings_window(supplier, sensor, lo, hi)
        segments: list[Segment] = []
        run: list[TimestampMs] = []

        def flush() -> None:
            if len(run) >= MIN_SEGMENT:
                segments.append(Segment(start=run[0], end=run[-1], count=len(run)))
            run.clear()

        for row in rows:
            if not row.exceeding:
                flush()          # 正常读数打断连续
                continue
            if run and row.ts - run[-1] > GAP_MS:
                flush()          # 间隔超过 15 分钟断开
            run.append(row.ts)
        flush()
        return segments

    # ---------- 事件调和 ----------

    def reconcile(self, supplier: str, sensor: str, new_ts: list[TimestampMs]) -> ReconcileResult:
        """new_ts 为本批新落库的温度读数时间戳（已排序去重）。空列表表示无变化。"""
        result = ReconcileResult(created=[], updated=[], revoked=[])
        if not new_ts:
            return result

        lo, hi = self._expand_window(supplier, sensor, new_ts[0], new_ts[-1])
        segments = self._compute_segments(supplier, sensor, lo, hi)

        # 与窗口有交集的原有效事件才可能受影响；窗口外的保持不动
        old_events = [
            e for e in self._repo.active_events_for_sensor(supplier, sensor)
            if e.start_ts <= hi and e.end_ts >= lo
        ]
        continued: set[int] = set()

        def shares_readings(event: RiskEvent, seg: Segment) -> bool:
            return self._repo.has_exceeding_between(
                supplier, sensor, max(event.start_ts, seg.start), min(event.end_ts, seg.end)
            )

        # 按段起始时间先后逐段确定事件
        for seg in segments:
            candidates = [e for e in old_events if e.id not in continued and shares_readings(e, seg)]
            if candidates:
                event = min(candidates, key=lambda e: e.id)  # 最早生成的优先
                continued.add(event.id)
                if (event.start_ts, event.end_ts, event.count) != (seg.start, seg.end, seg.count):
                    self._repo.update_event(event.id, seg.start, seg.end, seg.count)
                    event.start_ts, event.end_ts, event.count = seg.start, seg.end, seg.count
                    result.updated.append(event)
            else:
                result.created.append(self._repo.create_event(supplier, sensor, seg.start, seg.end, seg.count))

        # 没被延续的原事件撤销：有读数落在满 3 条的段里记为合并，否则不再成立
        for event in old_events:
            if event.id in continued:
                continue
            merged = any(shares_readings(event, seg) for seg in segments)
            reason = RevokeReason.MERGED if merged else RevokeReason.NO_LONGER_VALID
            self._repo.revoke_event(event.id, reason)
            event.status = event.status.__class__.REVOKED
            event.revoke_reason = reason
            result.revoked.append(event)

        return result
