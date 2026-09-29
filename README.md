# coldchain — 冷链温度超限判定库

供上层传感器数据接入服务直接调用的 Python 库（无接口层），读数、阈值与风险事件持久化在 SQLite。

## 用法

```python
from coldchain import ColdChainService, Reading, ReadingType

svc = ColdChainService("coldchain.db")

# 设置阈值：生效时间必须晚于该供应商已收到的最新读数
svc.set_threshold("supplier-1", low=2.0, high=8.0, effective_from=0.0)

# 提交一批读数（<=10000 条）；乱序、重发、迟到都安全
result = svc.submit_batch([
    Reading("supplier-1", "sensor-9", ReadingType.TEMPERATURE, 1735689600.0, 9.3),
    Reading("supplier-1", "sensor-9", "humidity", 1735689600.0, 61.0),  # 类型可传字符串
])
result.accepted     # 入库条数
result.duplicates   # 重发去重条数
result.rejections   # [Rejection(reading, reason), ...] 逐条拒收原因

# 查询当前有效事件（P3），即时返回
for ev in svc.active_events("supplier-1"):
    print(ev.sensor, ev.start_ts, ev.end_ts, ev.count)

svc.all_events("supplier-1")  # 含已撤销事件（status: active/merged/invalid）
svc.close()
```

## 判定规则要点

- 只有温度读数参与判定；其他类型照常保存，不打断温度连续性。
- 超出（严格大于上限或严格小于下限）算超限，等于不算；高低交替算连续超限。
- 连续 = 同一传感器温度读数按时间戳相邻且间隔 ≤15 分钟；满 3 条生成一个 P3 事件。
- 同一传感器同一时间戳：数值相同视为重发（幂等），不同则拒收；同批冲突全部拒收。
- 比该传感器批前最新读数早 72 小时以上拒收，正好 72 小时照收。
- 晚到读数会使段变长、接起来或拆开；事件按规则延续（最早生成者优先）、合并或撤销，只撤销不删除。
- 批结果只取决于读数内容，与批内顺序无关；同批重复提交不产生任何变化。

## 测试

```bash
python3 -m pytest tests/ -q
```

- `tests/test_rules.py` — 乱序、重发、跨批接段、插入正常读数拆段、阈值切换前后的晚到读数、15 分钟与 72 小时边界。
- `tests/test_consistency.py` — 随机数据按不同切批/批内顺序提交，最终有效事件与从头计算完全一致；同切批任意批内顺序的事件延续/撤销记录完全一致；整批重放幂等。
- `tests/test_perf.py` — 100 万条历史读数、2000 个传感器下，1 万条批次处理 <3 秒、有效事件查询 <100ms（实测约 0.2s / <5ms）。
