"""纯函数：由有序温度读数计算超限段。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

MAX_GAP_SECONDS = 15 * 60  # 相邻两条间隔超过15分钟即断开，正好15分钟不断
MIN_RUN_LENGTH = 3         # 连续超限满3条成段


@dataclass(frozen=True)
class Segment:
    start_ts: float
    end_ts: float
    count: int
    reading_ids: tuple[int, ...]


def compute_segments(readings: Sequence[tuple[int, float, int]]) -> list[Segment]:
    """由按 ts 升序的 (reading_id, ts, exceeding) 序列计算满3条的超限段。

    段是极大的连续超限读数串：相邻温度读数间隔 <=15 分钟且均超限。
    非超限读数或间隔 >15 分钟都会断开。
    """
    segments: list[Segment] = []
    run_ids: list[int] = []
    run_start = 0.0
    prev_ts: float | None = None

    def flush(end_ts: float) -> None:
        if len(run_ids) >= MIN_RUN_LENGTH:
            segments.append(
                Segment(
                    start_ts=run_start,
                    end_ts=end_ts,
                    count=len(run_ids),
                    reading_ids=tuple(run_ids),
                )
            )

    for reading_id, ts, exceeding in readings:
        if prev_ts is not None and ts - prev_ts > MAX_GAP_SECONDS and run_ids:
            flush(prev_ts)
            run_ids = []
        if exceeding:
            if not run_ids:
                run_start = ts
            run_ids.append(reading_id)
        elif run_ids:
            flush(prev_ts if prev_ts is not None else ts)
            run_ids = []
        prev_ts = ts
    if run_ids and prev_ts is not None:
        flush(prev_ts)
    return segments
