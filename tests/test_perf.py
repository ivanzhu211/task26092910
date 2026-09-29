"""性能验收：1000 万条读数、2000 个传感器的历史下，一批 10000 条 3 秒内处理完；
查询当前有效事件即时返回。用 COLDCHAIN_PERF_ROWS 可缩小规模便于快速跑。"""
import os
import time

import pytest

from coldchain import ColdChainService, Reading, ReadingType
from coldchain.db import connect

ROWS = int(os.environ.get("COLDCHAIN_PERF_ROWS", "10000000"))
SENSORS = 2000
BATCH = 10000
MIN = 60


@pytest.fixture()
def big_db(tmp_path):
    db = str(tmp_path / "perf.db")
    conn = connect(db)
    per_sensor = ROWS // SENSORS
    rows = []
    t0 = time.monotonic()
    for s in range(SENSORS):
        supplier = f"sup{s % 20}"
        sensor = f"sen{s}"
        for i in range(per_sensor):
            # 间隔 10 分钟，正常值为主，不打成超限段
            rows.append((supplier, sensor, "temperature", i * 10 * MIN * 1000, 5.0, 0))
            if len(rows) >= 200_000:
                conn.executemany(
                    "INSERT INTO readings (supplier_id, sensor_id, type, ts, value, exceeding)"
                    " VALUES (?,?,?,?,?,?)", rows)
                rows.clear()
    if rows:
        conn.executemany(
            "INSERT INTO readings (supplier_id, sensor_id, type, ts, value, exceeding)"
            " VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()
    print(f"\n[perf] 历史 {ROWS} 条 / {SENSORS} 传感器，建库 {time.monotonic() - t0:.1f}s")
    yield db


def test_batch_10000_under_3s(big_db):
    svc = ColdChainService(big_db)
    try:
        per_sensor = ROWS // SENSORS
        base = per_sensor * 10 * MIN  # 紧跟历史最新读数之后
        for s in range(20):
            svc.set_threshold(f"sup{s}", 2.0, 8.0, base)  # 生效于新批起始
        batch = [
            Reading(f"sup{s % 20}", f"sen{s}", ReadingType.TEMPERATURE,
                    base + (i + 1) * 10 * MIN, 9.0)  # 超限
            for s in range(SENSORS) for i in range(BATCH // SENSORS)
        ]
        t0 = time.monotonic()
        result = svc.submit_batch(batch)
        elapsed = time.monotonic() - t0
        print(f"[perf] 一批 {BATCH} 条耗时 {elapsed:.2f}s")
        assert result.accepted == BATCH
        assert len(result.events_created) == SENSORS  # 每个传感器 5 条超限成一段
        assert elapsed < 3.0

        t0 = time.monotonic()
        events = svc.active_events("sup0")
        query_elapsed = time.monotonic() - t0
        print(f"[perf] 查询有效事件耗时 {query_elapsed * 1000:.1f}ms，{len(events)} 条")
        assert query_elapsed < 0.1
    finally:
        svc.close()
