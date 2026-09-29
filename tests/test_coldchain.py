"""业务规则单元测试：乱序、重发、跨批接段、拆段、阈值切换、15 分钟与 72 小时边界。"""
import pytest

from coldchain import (
    ColdChainService,
    EventStatus,
    Reading,
    ReadingType,
    RejectReason,
    RevokeReason,
)

MIN = 60
HOUR = 3600
GAP = 15 * MIN          # 15 分钟
OLD = 72 * HOUR         # 72 小时
TEMP = ReadingType.TEMPERATURE


def temp(sensor, ts, value=9.0, supplier="s1"):
    return Reading(supplier, sensor, TEMP, ts, value)


@pytest.fixture()
def svc():
    service = ColdChainService(":memory:")
    service.set_threshold("s1", 2.0, 8.0, 0)   # 上限 8，读数 9 为超限
    yield service
    service.close()


def active(svc, supplier="s1"):
    return svc.active_events(supplier)


# ---------- 基本判定 ----------

def test_three_consecutive_exceedings_create_one_event(svc):
    r = svc.submit_batch([temp("a", 0), temp("a", 10 * MIN), temp("a", 20 * MIN)])
    assert r.accepted == 3
    assert len(r.events_created) == 1
    ev = r.events_created[0]
    assert (ev.start_ts, ev.end_ts, ev.count) == (0, 20 * MIN * 1000, 3)


def test_two_exceedings_no_event(svc):
    r = svc.submit_batch([temp("a", 0), temp("a", 10 * MIN)])
    assert r.events_created == []
    assert active(svc) == []


def test_equal_to_threshold_is_not_exceeding(svc):
    svc.submit_batch([temp("a", 0, 8.0), temp("a", 10 * MIN, 2.0), temp("a", 20 * MIN, 8.0)])
    assert active(svc) == []


def test_high_low_alternation_counts_as_continuous(svc):
    # 高超限与低超限交替，仍然连续
    svc.submit_batch([
        temp("a", 0, 9.0), temp("a", 10 * MIN, 1.0), temp("a", 20 * MIN, 10.0),
    ])
    events = active(svc)
    assert len(events) == 1 and events[0].count == 3


def test_long_segment_is_single_event_and_extends(svc):
    svc.submit_batch([temp("a", i * 10 * MIN) for i in range(3)])
    r = svc.submit_batch([temp("a", 30 * MIN), temp("a", 40 * MIN)])
    assert len(r.events_updated) == 1
    events = active(svc)
    assert len(events) == 1
    assert (events[0].start_ts, events[0].end_ts, events[0].count) == (0, 40 * MIN * 1000, 5)


# ---------- 乱序 ----------

def test_out_of_order_arrival_same_result(svc):
    svc.submit_batch([temp("a", 20 * MIN)])
    svc.submit_batch([temp("a", 0)])
    svc.submit_batch([temp("a", 10 * MIN)])
    events = active(svc)
    assert len(events) == 1
    assert (events[0].start_ts, events[0].end_ts, events[0].count) == (0, 20 * MIN * 1000, 3)


# ---------- 重发与冲突 ----------

def test_resend_same_value_is_deduplicated(svc):
    svc.submit_batch([temp("a", 0), temp("a", 10 * MIN), temp("a", 20 * MIN)])
    r = svc.submit_batch([temp("a", 0), temp("a", 20 * MIN)])
    assert r.duplicates == 2 and r.accepted == 0
    assert len(active(svc)) == 1


def test_resubmit_same_batch_no_change(svc):
    batch = [temp("a", 0), temp("a", 10 * MIN), temp("a", 20 * MIN)]
    svc.submit_batch(batch)
    before = active(svc)
    r = svc.submit_batch(batch)
    assert r.accepted == 0 and r.events_created == [] and r.events_revoked == []
    assert active(svc) == before


def test_conflicting_value_against_stored_is_rejected(svc):
    svc.submit_batch([temp("a", 0, 9.0)])
    r = svc.submit_batch([temp("a", 0, 5.0)])
    assert r.accepted == 0
    assert [x.reason for x in r.rejected] == [RejectReason.CONFLICT_WITH_STORED]


def test_conflicting_values_within_batch_all_rejected(svc):
    r = svc.submit_batch([temp("a", 0, 9.0), temp("a", 0, 5.0), temp("a", 10 * MIN, 9.0)])
    assert r.accepted == 1  # 10 分钟那条不受影响
    assert len(r.rejected) == 2
    assert all(x.reason == RejectReason.CONFLICT_IN_BATCH for x in r.rejected)


def test_rejection_does_not_affect_other_readings(svc):
    r = svc.submit_batch([
        temp("a", 0, 9.0), temp("a", 0, 5.0),      # 冲突，全拒
        temp("b", 0), temp("b", 10 * MIN), temp("b", 20 * MIN),  # 正常成段
    ])
    assert r.accepted == 3
    assert len(active(svc)) == 1


# ---------- 跨批接段 ----------

def test_late_reading_joins_segments_across_batches(svc):
    svc.submit_batch([temp("a", 0), temp("a", 10 * MIN)])            # 2 条，不成段
    svc.submit_batch([temp("a", 30 * MIN), temp("a", 40 * MIN)])     # 又 2 条，不成段
    assert active(svc) == []
    r = svc.submit_batch([temp("a", 20 * MIN)])                      # 晚到一条接上
    assert len(r.events_created) == 1
    events = active(svc)
    assert len(events) == 1
    assert (events[0].start_ts, events[0].end_ts, events[0].count) == (0, 40 * MIN * 1000, 5)


def test_merge_two_events_continues_earliest(svc):
    svc.submit_batch([temp("a", 0), temp("a", 10 * MIN), temp("a", 20 * MIN)])       # 事件 1
    svc.submit_batch([temp("a", 40 * MIN), temp("a", 50 * MIN), temp("a", 60 * MIN)])  # 事件 2
    first_id = active(svc)[0].id
    r = svc.submit_batch([temp("a", 30 * MIN)])                      # 桥接两段
    assert len(r.events_updated) == 1 and r.events_updated[0].id == first_id
    assert len(r.events_revoked) == 1
    assert r.events_revoked[0].revoke_reason == RevokeReason.MERGED
    events = active(svc)
    assert len(events) == 1 and events[0].count == 7
    assert (events[0].start_ts, events[0].end_ts) == (0, 60 * MIN * 1000)


# ---------- 插入正常读数拆段 ----------

def test_normal_reading_splits_segment(svc):
    svc.submit_batch([temp("a", i * 10 * MIN) for i in range(7)])    # 0..60 共 7 条超限
    r = svc.submit_batch([temp("a", 35 * MIN, 5.0)])                 # 中间插入正常读数
    events = active(svc)
    assert len(events) == 2
    assert [(e.start_ts, e.end_ts, e.count) for e in events] == [
        (0, 30 * MIN * 1000, 4),
        (40 * MIN * 1000, 60 * MIN * 1000, 3),
    ]
    # 前段延续原事件，后段为新建
    assert len(r.events_updated) == 1 and len(r.events_created) == 1


def test_split_with_short_tail_revokes_as_no_longer_valid(svc):
    svc.submit_batch([temp("a", i * 10 * MIN) for i in range(4)])    # 0..30 共 4 条
    r = svc.submit_batch([temp("a", 15 * MIN, 5.0)])                 # 拆成 2 + 2，都不满 3
    assert active(svc) == []
    assert len(r.events_revoked) == 1
    assert r.events_revoked[0].revoke_reason == RevokeReason.NO_LONGER_VALID


# ---------- 其他类型不打断温度连续 ----------

def test_other_types_saved_but_do_not_break_continuity(svc):
    r = svc.submit_batch([
        temp("a", 0),
        Reading("s1", "a", ReadingType.HUMIDITY, 5 * MIN, 60.0),
        Reading("s1", "a", ReadingType.VIBRATION, 8 * MIN, 0.3),
        temp("a", 10 * MIN),
        temp("a", 20 * MIN),
    ])
    assert r.accepted == 5
    events = active(svc)
    assert len(events) == 1 and events[0].count == 3


# ---------- 15 分钟边界 ----------

def test_exactly_15_minutes_gap_does_not_break(svc):
    svc.submit_batch([temp("a", 0), temp("a", GAP), temp("a", 2 * GAP)])
    events = active(svc)
    assert len(events) == 1 and events[0].count == 3


def test_over_15_minutes_gap_breaks(svc):
    svc.submit_batch([temp("a", 0), temp("a", GAP + 1), temp("a", 2 * (GAP + 1))])
    assert active(svc) == []


# ---------- 72 小时边界 ----------

def test_exactly_72_hours_old_is_accepted(svc):
    svc.submit_batch([temp("a", OLD)])
    r = svc.submit_batch([temp("a", 0)])
    assert r.accepted == 1 and r.rejected == []


def test_over_72_hours_old_is_rejected(svc):
    svc.submit_batch([temp("a", OLD + 1)])
    r = svc.submit_batch([temp("a", 0)])
    assert r.accepted == 0
    assert [x.reason for x in r.rejected] == [RejectReason.TOO_OLD]


def test_72h_rule_compares_against_pre_batch_latest(svc):
    # 批内更晚的读数不影响同批其他读数的判定
    r = svc.submit_batch([temp("a", 0), temp("a", OLD + 100 * HOUR)])
    assert r.accepted == 2


# ---------- 阈值切换 ----------

def test_threshold_change_applies_by_reading_timestamp(svc):
    # 阈值 2~8 生效于 0；在 t=100min 改为 4~8（生效时刻本身用新阈值）
    svc.submit_batch([temp("a", 90 * MIN)])
    svc.set_threshold("s1", 4.0, 8.0, 100 * MIN)
    # 晚到的读数：99min 用旧阈值（3 不超限），100min 用新阈值（3 超限）
    svc.submit_batch([temp("a", 99 * MIN, 3.0), temp("a", 100 * MIN, 3.0), temp("a", 110 * MIN, 3.0)])
    svc.submit_batch([temp("a", 120 * MIN, 3.0)])
    events = active(svc)
    assert len(events) == 1
    assert (events[0].start_ts, events[0].end_ts, events[0].count) == (
        100 * MIN * 1000, 120 * MIN * 1000, 3)


def test_threshold_effective_time_must_be_after_latest_reading(svc):
    svc.submit_batch([temp("a", 50 * MIN)])
    with pytest.raises(ValueError):
        svc.set_threshold("s1", 4.0, 8.0, 50 * MIN)   # 等于最新读数，不允许
    with pytest.raises(ValueError):
        svc.set_threshold("s1", 4.0, 8.0, 10 * MIN)   # 早于最新读数，不允许
    svc.set_threshold("s1", 4.0, 8.0, 60 * MIN)       # 晚于最新读数，允许


def test_threshold_must_be_increasing_and_valid(svc):
    svc.set_threshold("s1", 4.0, 8.0, 100 * MIN)
    with pytest.raises(ValueError):
        svc.set_threshold("s1", 5.0, 8.0, 50 * MIN)   # 生效时间回退
    with pytest.raises(ValueError):
        svc.set_threshold("s1", 9.0, 8.0, 200 * MIN)  # 下限不小于上限


# ---------- 批大小 ----------

def test_batch_size_limit(svc):
    with pytest.raises(ValueError):
        svc.submit_batch([temp("a", i) for i in range(10001)])
    r = svc.submit_batch([temp("a", i * GAP) for i in range(10000)])
    assert r.accepted == 10000
