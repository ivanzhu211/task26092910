"""数据类型定义：读数、阈值、风险事件、批处理结果。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Union

# 时间戳在库内部统一为 Unix 毫秒整数
TimestampMs = int
# 外部传入的时间：datetime 或 Unix 秒（int/float）
TimeLike = Union[datetime, int, float]


class ReadingType(str, enum.Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    VIBRATION = "vibration"
    POSITION = "position"


class RejectReason(str, enum.Enum):
    BATCH_TOO_LARGE = "batch_too_large"            # 批超过 10000 条
    CONFLICT_IN_BATCH = "conflict_in_batch"        # 同批同传感器同时间戳数值不一致
    CONFLICT_WITH_STORED = "conflict_with_stored"  # 与已收读数数值不同
    TOO_OLD = "too_old"                            # 早于本批前最新读数 72 小时以上


class EventStatus(str, enum.Enum):
    ACTIVE = "active"
    REVOKED = "revoked"


class RevokeReason(str, enum.Enum):
    MERGED = "merged"                # 读数落在其他满 3 条的段里
    NO_LONGER_VALID = "no_longer_valid"  # 不再成立


def to_ms(t: TimeLike) -> TimestampMs:
    """把外部时间统一成 Unix 毫秒。datetime 按 UTC 处理，数字按 Unix 秒处理。"""
    if isinstance(t, datetime):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return int(t.timestamp() * 1000)
    return int(round(float(t) * 1000))


@dataclass(frozen=True)
class Reading:
    supplier_id: str
    sensor_id: str
    type: ReadingType
    ts: TimeLike
    value: float


@dataclass
class Rejection:
    reading: Reading
    reason: RejectReason


@dataclass
class RiskEvent:
    id: int
    supplier_id: str
    sensor_id: str
    start_ts: TimestampMs
    end_ts: TimestampMs
    count: int
    status: EventStatus = EventStatus.ACTIVE
    revoke_reason: RevokeReason | None = None


@dataclass
class BatchResult:
    accepted: int = 0                    # 新入库的读数条数
    duplicates: int = 0                  # 重发（数值相同）被跳过的条数
    rejected: list[Rejection] = field(default_factory=list)
    events_created: list[RiskEvent] = field(default_factory=list)
    events_updated: list[RiskEvent] = field(default_factory=list)
    events_revoked: list[RiskEvent] = field(default_factory=list)
