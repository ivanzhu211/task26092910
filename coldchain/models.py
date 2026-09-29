"""数据类型定义：读数、阈值、风险事件、批处理结果。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class ReadingType(enum.Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    VIBRATION = "vibration"
    LOCATION = "location"


class EventStatus(enum.Enum):
    ACTIVE = "active"        # 当前有效
    MERGED = "merged"        # 被撤销，读数并入了其他满3条的段
    INVALID = "invalid"      # 被撤销，不再成立


@dataclass(frozen=True)
class Reading:
    supplier: str
    sensor: str
    type: ReadingType
    ts: float          # epoch 秒
    value: float

    def __post_init__(self) -> None:
        if not isinstance(self.type, ReadingType):
            object.__setattr__(self, "type", ReadingType(self.type))
        object.__setattr__(self, "ts", float(self.ts))
        object.__setattr__(self, "value", float(self.value))


@dataclass(frozen=True)
class Rejection:
    reading: Reading
    reason: str


@dataclass
class RiskEvent:
    id: int
    supplier: str
    sensor: str
    start_ts: float
    end_ts: float
    count: int
    status: EventStatus


@dataclass
class BatchResult:
    accepted: int = 0
    duplicates: int = 0
    rejections: list[Rejection] = field(default_factory=list)
