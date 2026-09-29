"""性能：大历史下批处理耗时有界、有效事件查询即时。"""
import os
import time

import pytest

from coldchain import ColdChainService, Reading, ReadingType

N_HISTORY = 1_000_000
N_SENSORS = 2000
N_SUPPLIERS = 20
BATCH_SIZE = 10_000
BATCH_TIME_LIMIT = 3.0
QUERY_TIME_LIMIT = 0.1


@pytest.fixture(scope="module")
def big_service(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("perf") / "perf.db")
    svc = ColdChainService(path)
    conn = svc._conn
    with conn:
        conn.executemany(
            "INSERT INTO readings (supplier, sensor, type, ts, value, exceeding)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                ("S%d" % (i % N_SUPPLIERS), "sen%d" % (i % N_SENSORS),
                 "temperature", 10000.0 + i, 5.0, 0)
                for i in range(N_HISTORY)
            ),
        )
    for i in range(N_SUPPLIERS):
        svc.set_threshold("S%d" % i, 2.0, 8.0, 2_000_000.0)
    yield svc
    svc.close()


def test_batch_10k_under_3s_with_1m_history(big_service):
    base_ts = 3_000_000.0
    batch = [
        Reading(
            "S%d" % (i % N_SUPPLIERS),
            "sen%d" % (i % N_SENSORS),
            ReadingType.TEMPERATURE,
            base_ts + i,
            9.0,  # 超限
        )
        for i in range(BATCH_SIZE)
    ]
    start = time.perf_counter()
    result = big_service.submit_batch(batch)
    elapsed = time.perf_counter() - start
    assert result.accepted == BATCH_SIZE
    assert elapsed < BATCH_TIME_LIMIT, f"批处理耗时 {elapsed:.2f}s"


def test_active_events_query_instant(big_service):
    start = time.perf_counter()
    for i in range(N_SUPPLIERS):
        big_service.active_events("S%d" % i)
    elapsed = time.perf_counter() - start
    assert elapsed < QUERY_TIME_LIMIT, f"查询耗时 {elapsed:.3f}s"
