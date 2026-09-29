"""随机一致性：不同切批/批内顺序的最终事件与从头计算一致；同切批的事件延续记录一致。"""
import random
from collections import defaultdict

import pytest

from coldchain import ColdChainService, Reading, ReadingType
from coldchain.segments import MAX_GAP_SECONDS, MIN_RUN_LENGTH

T = ReadingType.TEMPERATURE


def reference_events(conn):
    """独立参照实现：从库中实际收下的读数 + 阈值，从头算出每个传感器应有的段。"""
    thresholds = defaultdict(list)
    for supplier, eff, low, high in conn.execute(
        "SELECT supplier, effective_from, low, high FROM thresholds"
        " ORDER BY effective_from"
    ):
        thresholds[supplier].append((eff, low, high))

    by_sensor = defaultdict(list)
    for supplier, sensor, ts, value in conn.execute(
        "SELECT supplier, sensor, ts, value FROM readings"
        " WHERE type = 'temperature' ORDER BY ts"
    ):
        by_sensor[(supplier, sensor)].append((ts, value))

    expected = {}
    for (supplier, sensor), rows in by_sensor.items():
        thr = thresholds[supplier]
        segments = []
        run = []
        prev_ts = None
        for ts, value in rows:
            eff = [t for t in thr if t[0] <= ts]
            exceeding = bool(eff) and (value > eff[-1][2] or value < eff[-1][1])
            if prev_ts is not None and ts - prev_ts > MAX_GAP_SECONDS:
                run = []
            if exceeding:
                run.append(ts)
                if len(run) >= MIN_RUN_LENGTH:
                    seg = (run[0], ts, len(run))
                    if segments and segments[-1][0] == seg[0]:
                        segments[-1] = seg
                    else:
                        segments.append(seg)
            else:
                run = []
            prev_ts = ts
        if segments:
            expected[(supplier, sensor)] = sorted(segments)
    return expected


def active_map(svc, suppliers):
    got = {}
    for supplier in suppliers:
        for ev in svc.active_events(supplier):
            got.setdefault((ev.supplier, ev.sensor), []).append(
                (ev.start_ts, ev.end_ts, ev.count)
            )
    return {k: sorted(v) for k, v in got.items()}


def lineage(svc, suppliers):
    """全部事件的延续/撤销记录（含事件id，反映生成顺序）。"""
    rows = []
    for supplier in suppliers:
        for ev in svc.all_events(supplier):
            rows.append(
                (ev.id, ev.supplier, ev.sensor, ev.start_ts, ev.end_ts,
                 ev.count, ev.status.value)
            )
    return sorted(rows)


def make_dataset(seed, n=1500):
    """3供应商 x 2传感器，48小时内的随机温度/湿度读数（时间窗<72h，排除迟到拒收干扰）。"""
    rng = random.Random(seed)
    suppliers = ["SA", "SB", "SC"]
    readings = []
    seen = set()
    for supplier in suppliers:
        for sensor in ("s1", "s2"):
            ts = 10000.0
            while ts < 10000.0 + 48 * 3600 and len(readings) < n:
                ts += rng.uniform(60, 1500)
                key = (supplier, sensor, round(ts, 3))
                if key in seen:
                    continue
                seen.add(key)
                value = round(rng.uniform(0, 12), 2)
                rtype = T if rng.random() < 0.85 else ReadingType.HUMIDITY
                readings.append(Reading(supplier, sensor, rtype, key[2], value))
    # 掺入若干完全相同的重发
    for _ in range(n // 20):
        readings.append(rng.choice(readings))
    rng.shuffle(readings)
    return readings, suppliers


def partition(readings, rng, max_size=300):
    batches, cur = [], []
    for r in readings:
        cur.append(r)
        if len(cur) >= rng.uniform(50, max_size):
            batches.append(cur)
            cur = []
    if cur:
        batches.append(cur)
    return batches


def run_pipeline(readings, batches, shuffle_within=False, seed=0):
    svc = ColdChainService(":memory:")
    for i, s in enumerate(("SA", "SB", "SC")):
        svc.set_threshold(s, 2.0 + i, 8.0 + i, 0.0)
    rng = random.Random(seed)
    for batch in batches:
        if shuffle_within:
            batch = list(batch)
            rng.shuffle(batch)
        svc.submit_batch(batch)
    return svc


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_final_events_match_from_scratch(seed):
    readings, suppliers = make_dataset(seed)
    rng = random.Random(seed * 10)

    svc_a = run_pipeline(readings, partition(readings, random.Random(1)))
    svc_b = run_pipeline(readings, partition(readings, random.Random(2)))

    expected = reference_events(svc_a._conn)
    assert active_map(svc_a, suppliers) == expected
    # 不同切批、不同批内顺序：最终有效事件一致（传感器、起止、条数）
    assert active_map(svc_b, suppliers) == expected
    assert reference_events(svc_b._conn) == expected


@pytest.mark.parametrize("seed", [4, 5])
def test_same_partition_any_order_same_lineage(seed):
    readings, suppliers = make_dataset(seed)
    batches = partition(readings, random.Random(3))

    svc_a = run_pipeline(readings, batches, shuffle_within=False)
    svc_b = run_pipeline(readings, batches, shuffle_within=True, seed=seed)

    # 同一种切批无论批内怎么排：事件延续与撤销记录完全一致
    assert lineage(svc_a, suppliers) == lineage(svc_b, suppliers)


def test_resubmission_is_noop():
    readings, suppliers = make_dataset(9)
    batches = partition(readings, random.Random(4))
    svc = run_pipeline(readings, batches)
    before = lineage(svc, suppliers)
    for batch in batches:  # 全部重放一遍
        svc.submit_batch(batch)
    assert lineage(svc, suppliers) == before
