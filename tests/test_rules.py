"""业务规则测试：乱序、重发、接段、拆段、阈值切换、15分钟与72小时边界。"""
import pytest

from coldchain import ColdChainService, EventStatus, Reading, ReadingType

T = ReadingType.TEMPERATURE
H = ReadingType.HUMIDITY


def r(sensor, ts, value, supplier="S1", rtype=T):
    return Reading(supplier, sensor, rtype, float(ts), float(value))


@pytest.fixture
def svc():
    s = ColdChainService(":memory:")
    s.set_threshold("S1", 2.0, 8.0, 1000.0)
    yield s
    s.close()


def make_run(sensor, start, n, step=600, value=9.0, supplier="S1"):
    return [r(sensor, start + i * step, value, supplier) for i in range(n)]


class TestBasicDetection:
    def test_three_consecutive_exceeding_creates_event(self, svc):
        res = svc.submit_batch(make_run("T1", 2000, 3))
        assert res.accepted == 3
        (ev,) = svc.active_events("S1")
        assert (ev.sensor, ev.start_ts, ev.end_ts, ev.count) == ("T1", 2000.0, 3200.0, 3)

    def test_two_exceeding_not_enough(self, svc):
        svc.submit_batch(make_run("T1", 2000, 2))
        assert svc.active_events("S1") == []

    def test_equal_to_threshold_not_exceeding(self, svc):
        svc.submit_batch([r("T1", 2000, 8.0), r("T1", 2600, 2.0), r("T1", 3200, 8.0)])
        assert svc.active_events("S1") == []

    def test_high_low_alternating_is_continuous(self, svc):
        svc.submit_batch([r("T1", 2000, 9.0), r("T1", 2600, 1.0), r("T1", 3200, 9.0)])
        (ev,) = svc.active_events("S1")
        assert ev.count == 3

    def test_out_of_order_within_batch(self, svc):
        readings = make_run("T1", 2000, 3)
        res = svc.submit_batch([readings[2], readings[0], readings[1]])
        assert res.accepted == 3
        assert len(svc.active_events("S1")) == 1

    def test_non_temperature_saved_but_not_judged(self, svc):
        res = svc.submit_batch([
            r("T1", 2000, 9.0), r("T1", 2300, 99.0, rtype=H),
            r("T1", 2600, 9.0), r("T1", 3200, 9.0),
        ])
        assert res.accepted == 4
        (ev,) = svc.active_events("S1")
        assert ev.count == 3  # 湿度读数不打断温度连续

    def test_batch_size_limit(self, svc):
        with pytest.raises(ValueError):
            svc.submit_batch(make_run("T1", 2000, 10001))


class TestGapBoundary:
    def test_exactly_15_minutes_not_broken(self, svc):
        svc.submit_batch(make_run("T1", 2000, 3, step=900))
        assert len(svc.active_events("S1")) == 1

    def test_over_15_minutes_broken(self, svc):
        svc.submit_batch(make_run("T1", 2000, 4, step=901))
        assert svc.active_events("S1") == []


class TestDedupAndRejection:
    def test_resend_same_value_kept_once(self, svc):
        svc.submit_batch(make_run("T1", 2000, 3))
        res = svc.submit_batch(make_run("T1", 2000, 3))
        assert res.accepted == 0 and res.duplicates == 3 and not res.rejections
        (ev,) = svc.active_events("S1")
        assert ev.count == 3

    def test_conflicting_value_rejected(self, svc):
        svc.submit_batch([r("T1", 2000, 9.0)])
        res = svc.submit_batch([r("T1", 2000, 7.0)])
        assert res.accepted == 0
        assert len(res.rejections) == 1 and "数值不同" in res.rejections[0].reason

    def test_in_batch_conflict_all_rejected(self, svc):
        res = svc.submit_batch([r("T1", 2000, 9.0), r("T1", 2000, 7.0), r("T1", 2600, 9.0)])
        assert res.accepted == 1
        assert len(res.rejections) == 2

    def test_rejection_does_not_affect_others(self, svc):
        svc.submit_batch([r("T1", 2000, 9.0)])
        res = svc.submit_batch([r("T1", 2000, 7.0), r("T1", 2600, 9.0), r("T1", 3200, 9.0)])
        assert res.accepted == 2 and len(res.rejections) == 1
        (ev,) = svc.active_events("S1")
        assert ev.count == 3


class TestLatenessBoundary:
    def test_exactly_72h_accepted(self, svc):
        svc.submit_batch([r("T1", 100000, 5.0)])
        res = svc.submit_batch([r("T1", 100000 - 72 * 3600, 9.0)])
        assert res.accepted == 1

    def test_over_72h_rejected(self, svc):
        svc.submit_batch([r("T1", 100000, 5.0)])
        res = svc.submit_batch([r("T1", 100000 - 72 * 3600 - 1, 9.0)])
        assert res.accepted == 0
        assert "72小时" in res.rejections[0].reason

    def test_lateness_reference_is_before_batch(self, svc):
        # 批内新读数不更新72小时参照点
        svc.submit_batch([r("T1", 100000, 5.0)])
        res = svc.submit_batch([
            r("T1", 200000, 5.0),                      # 先入库的新读数
            r("T1", 200000 - 72 * 3600 - 60, 9.0),     # 相对10万在72h内，相对20万超72h
        ])
        assert res.accepted == 2, res.rejections


class TestCrossBatchSegments:
    def test_segment_extended_across_batches(self, svc):
        svc.submit_batch(make_run("T1", 2000, 3))
        svc.submit_batch([r("T1", 3800, 9.0), r("T1", 4400, 9.0)])
        (ev,) = svc.active_events("S1")
        assert (ev.start_ts, ev.end_ts, ev.count) == (2000.0, 4400.0, 5)

    def test_late_reading_extends_backward(self, svc):
        svc.submit_batch(make_run("T1", 2600, 3))
        svc.submit_batch([r("T1", 2000, 9.0)])
        (ev,) = svc.active_events("S1")
        assert (ev.start_ts, ev.count) == (2000.0, 4)

    def test_normal_reading_splits_segment(self, svc):
        svc.submit_batch(make_run("T1", 2000, 5))
        svc.submit_batch([r("T1", 3200 + 300, 5.0)])  # 在中间插入正常读数
        # 拆成 3+2：前段仍满3条，原事件延续并缩短
        (ev,) = svc.active_events("S1")
        assert (ev.id, ev.start_ts, ev.end_ts, ev.count) == (1, 2000.0, 3200.0, 3)

    def test_split_into_two_valid_segments(self, svc):
        svc.submit_batch(make_run("T1", 2000, 7))  # 7条一段
        svc.submit_batch([r("T1", 2000 + 3 * 600 + 300, 5.0)])  # 拆成 4+3
        evs = svc.active_events("S1")
        assert len(evs) == 2
        assert sorted(e.count for e in evs) == [3, 4]
        # 原事件被其中一段延续，另一段新建
        allev = svc.all_events("S1")
        assert len(allev) == 2 and all(e.status is EventStatus.ACTIVE for e in allev)

    def test_bridge_merges_two_events(self, svc):
        svc.submit_batch(make_run("T1", 2000, 3))
        svc.submit_batch(make_run("T1", 4400, 3))
        assert len(svc.active_events("S1")) == 2
        svc.submit_batch([r("T1", 3800, 9.0)])  # 桥接
        (ev,) = svc.active_events("S1")
        assert (ev.start_ts, ev.end_ts, ev.count) == (2000.0, 5600.0, 7)
        allev = svc.all_events("S1")
        # 最早生成的事件被延续，另一个记为合并
        assert allev[0].status is EventStatus.ACTIVE
        assert allev[1].status is EventStatus.MERGED

    def test_segment_shrinks_to_invalid(self, svc):
        svc.submit_batch(make_run("T1", 2000, 3))
        # 插入正常读数使段不再满3条 -> 不再成立
        svc.submit_batch([r("T1", 2300, 5.0)])
        (ev,) = svc.all_events("S1")
        assert ev.status is EventStatus.INVALID


class TestThresholds:
    def test_threshold_must_follow_latest_reading(self, svc):
        svc.submit_batch([r("T1", 5000, 5.0)])
        with pytest.raises(ValueError):
            svc.set_threshold("S1", 0.0, 10.0, 5000.0)
        svc.set_threshold("S1", 0.0, 10.0, 5001.0)  # 更晚即可

    def test_late_reading_uses_historical_threshold(self):
        s = ColdChainService(":memory:")
        s.set_threshold("S1", 2.0, 8.0, 1000.0)
        s.submit_batch([r("T1", 5000, 5.0)])
        s.set_threshold("S1", 4.0, 6.0, 6000.0)  # 收紧阈值
        # 晚到读数落在旧阈值时段：7.0 在旧阈值(2,8)下正常，虽在新阈值下超限也不算
        # 6000 之后的读数按新阈值：7.0 超限
        s.submit_batch([
            r("T1", 2000, 9.0), r("T1", 2600, 9.0), r("T1", 3200, 9.0),
            r("T1", 5500, 7.0), r("T1", 6100, 7.0),
        ])
        (ev,) = s.active_events("S1")
        assert (ev.start_ts, ev.end_ts, ev.count) == (2000.0, 3200.0, 3)
        rows = dict(s._conn.execute(
            "SELECT ts, exceeding FROM readings WHERE type = 'temperature'"))
        assert rows[5500.0] == 0 and rows[6100.0] == 1
        s.close()

    def test_effective_moment_uses_new_threshold(self):
        s = ColdChainService(":memory:")
        s.set_threshold("S1", 2.0, 8.0, 1000.0)
        s.submit_batch([r("T1", 5000, 5.0)])
        s.set_threshold("S1", 2.0, 4.0, 6000.0)  # 上限降到4
        # 生效时刻本身用新阈值：6000 时刻 5.0 超限
        s.submit_batch([r("T1", 6000, 5.0), r("T1", 6600, 5.0), r("T1", 7200, 5.0)])
        (ev,) = s.active_events("S1")
        assert ev.count == 3
        s.close()

    def test_no_threshold_not_exceeding(self):
        s = ColdChainService(":memory:")
        res = s.submit_batch(make_run("T1", 2000, 3, supplier="S2"))
        assert res.accepted == 3
        assert s.active_events("S2") == []
        s.close()


class TestIdempotency:
    def test_resubmit_same_batch_no_change(self, svc):
        batch = make_run("T1", 2000, 4)
        svc.submit_batch(batch)
        before = svc.all_events("S1")
        res = svc.submit_batch(batch)
        assert res.accepted == 0 and res.duplicates == 4
        after = svc.all_events("S1")
        assert before == after
