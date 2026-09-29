"""性质测试：同一份随机数据，无论怎么切批、批内怎么排，
最终有效事件都与"把实际收下的全部读数排序后从头算"完全一致；
同一种切批无论批内怎么排，事件延续与撤销记录完全一致。
"""
import random
import sqlite3

import pytest

from coldchain import ColdChainService, Reading, ReadingType
from coldchain.db import connect
from coldchain.engine import GAP_MS, MIN_SEGMENT

MIN = 60
HOUR = 3600
TEMP = ReadingType.TEMPERATURE


def reference_active_events(db_path):
    """参考实现：直接读库里的温度读数，按传感器排序后从头算段。刻意简单直白。"""
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT supplier_id, sensor_id, ts, exceeding FROM readings"
        " WHERE type='temperature' ORDER BY supplier_id, sensor_id, ts"
    ).fetchall()
    conn.close()
    expected = set()
    current_key, run = None, []
    for supplier, sensor, ts, exceeding in rows + [("", "", 0, 0)]:
        key = (supplier, sensor)
        if key != current_key or not exceeding or (run and ts - run[-1] > GAP_MS):
            if len(run) >= MIN_SEGMENT:
                expected.add((current_key[1], run[0], run[-1], len(run)))
            run = []
            current_key = key
        if key == current_key and exceeding:
            run.append(ts)
    return expected


def actual_active_events(db_path, suppliers):
    svc = ColdChainService(db_path)
    try:
        return {
            (e.sensor_id, e.start_ts, e.end_ts, e.count)
            for s in suppliers for e in svc.active_events(s)
        }
    finally:
        svc.close()


def all_event_records(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT id, supplier_id, sensor_id, start_ts, end_ts, count, status, revoke_reason"
        " FROM events ORDER BY id"
    ).fetchall()
    conn.close()
    return rows


def make_readings(seed, n_sensors=8, span_days=10):
    """随机读数：随机间隔（含 <=15 分钟的链与长间隔），温度簇状超限，混少量其他类型与重发。"""
    rng = random.Random(seed)
    suppliers = ["s1", "s2"]
    readings = []
    for i in range(n_sensors):
        supplier = suppliers[i % len(suppliers)]
        sensor = f"sen{i}"
        ts = rng.randrange(0, 2 * HOUR)
        end = span_days * 24 * HOUR
        while ts < end:
            # 簇状超限：偶尔进入一段连续超限
            if rng.random() < 0.25:
                for _ in range(rng.randrange(1, 8)):
                    if ts >= end:
                        break
                    readings.append(Reading(supplier, sensor, TEMP, ts, rng.choice([9.0, 1.5, 12.0])))
                    if rng.random() < 0.1:  # 夹一条其他类型
                        readings.append(Reading(supplier, sensor, ReadingType.HUMIDITY, ts, 55.0))
                    ts += rng.randrange(1, 15) * MIN
            else:
                readings.append(Reading(supplier, sensor, TEMP, ts, rng.uniform(3.0, 7.0)))
            ts += rng.randrange(5, 600) * MIN
    # 撒一些重发（同传感器同时间戳同数值）
    for _ in range(len(readings) // 20):
        readings.append(rng.choice(readings))
    return readings, suppliers


def run_scenario(db_path, batches, suppliers):
    svc = ColdChainService(db_path)
    try:
        for s in suppliers:
            svc.set_threshold(s, 2.0, 8.0, 0)
        for batch in batches:
            svc.submit_batch(batch)
    finally:
        svc.close()


def chunk(readings, sizes):
    batches, i = [], 0
    for size in sizes:
        batches.append(readings[i:i + size])
        i += size
    batches.append(readings[i:])
    return [b for b in batches if b]


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_different_batchings_match_reference(tmp_path, seed):
    readings, suppliers = make_readings(seed)
    rng = random.Random(seed * 100)

    # 切批方式 A：全局打乱后均分；方式 B：随机块大小；方式 C：按时间排序后分两块再乱序
    shuffled = readings[:]
    rng.shuffle(shuffled)
    batchings = [
        chunk(shuffled, [500] * (len(shuffled) // 500)),
        chunk(shuffled, [rng.randrange(50, 800) for _ in range(len(shuffled) // 300)]),
        chunk(readings[::2] + readings[1::2], [len(readings) // 3]),
    ]
    for n, batches in enumerate(batchings):
        db = str(tmp_path / f"p{seed}_{n}.db")
        run_scenario(db, batches, suppliers)
        assert actual_active_events(db, suppliers) == reference_active_events(db), \
            f"seed={seed} batching={n}"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_same_batching_any_in_batch_order_identical_events(tmp_path, seed):
    readings, suppliers = make_readings(seed)
    rng = random.Random(seed * 100 + 7)
    shuffled = readings[:]
    rng.shuffle(shuffled)
    batches = chunk(shuffled, [400] * (len(shuffled) // 400))

    records = []
    for n in range(3):
        reordered = [rng.sample(b, len(b)) for b in batches]  # 只打乱批内顺序
        db = str(tmp_path / f"o{seed}_{n}.db")
        run_scenario(db, reordered, suppliers)
        records.append(all_event_records(db))
    assert records[0] == records[1] == records[2]


def test_resubmitted_batches_produce_no_changes(tmp_path):
    readings, suppliers = make_readings(99)
    rng = random.Random(99)
    rng.shuffle(readings)
    batches = chunk(readings, [500] * (len(readings) // 500))
    db = str(tmp_path / "idem.db")
    run_scenario(db, batches, suppliers)
    before = all_event_records(db)
    svc = ColdChainService(db)
    try:
        for batch in batches:  # 整批重复提交
            svc.submit_batch(batch)
    finally:
        svc.close()
    assert all_event_records(db) == before
