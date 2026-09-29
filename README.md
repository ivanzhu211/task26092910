# coldchain — 冷链温度超限判定库

供传感器数据接入服务直接调用的 Python 库（无接口层）。读数、阈值、风险事件持久化在 SQLite。
判定只依据读数自身时间戳：晚到、重发、乱序、跨批拆分都不影响最终结论。

## 用法

```python
from coldchain import ColdChainService, Reading, ReadingType

svc = ColdChainService("coldchain.db")
svc.set_threshold("supplier-1", low=2.0, high=8.0, effective_from=0)   # 生效时刻本身用新阈值

result = svc.submit_batch([
    Reading("supplier-1", "sensor-9", ReadingType.TEMPERATURE, 1727600000, 9.5),
    # ... 每批最多 10000 条；时间戳为 Unix 秒或 datetime
])
# result.accepted / duplicates / rejected(逐条原因) / events_created / events_updated / events_revoked

events = svc.active_events("supplier-1")   # 当前有效 P3 事件，索引直查，即时返回
```

## 规则实现要点

- 只有温度参与判定；湿度、振动、位置照常保存，不打断温度连续。
- 连续 = 同一传感器温度读数按时间排序相邻且间隔 ≤15 分钟（正好 15 分钟不断）；
  正常读数会打断连续。高低超限交替仍算连续；等于阈值不算超限。
- 连续超限满 3 条成一段，对应一个 P3 事件；段变长只更新起止与条数。
- 去重：同 (供应商, 传感器, 类型, 时间戳) 数值相同视为重发只留一条；
  与已收数值不同的拒收；同批内数值冲突的全部拒收。
- 温度读数早于"本批之前该传感器最新温度读数"72 小时以上拒收，正好 72 小时照收；
  其他类型不受此限。被拒逐条返回原因，不影响同批其余读数。
- 阈值按读数时间戳所在时段判定；新阈值生效时间必须晚于该供应商已收到的最新读数，
  因此历史读数的超限判定永不回改（库内直接存 `exceeding` 标记）。
- 每批处理后对受影响传感器按时间重算超限段：每段按起始时间先后，延续与它有共同读数、
  尚未被延续的原有效事件中最早生成的一个；没有则新建。未被延续的原事件撤销
  （有读数落在满 3 条的段里记 `merged`，否则记 `no_longer_valid`），事件只撤销不删除。
- 批内先统一去重校验再落库，结果与批内顺序无关；重复提交同一批不产生任何变化。

## 性能设计

- 重算只覆盖受影响窗口：从新读数向两侧沿"间隔 ≤15 分钟且超限"的链扩展到段边界，
  耗时不随历史总量增长。所有查询走 `(供应商, 传感器, 类型, 时间戳)` 等索引。
- 有效事件查询走 `(supplier_id, status)` 索引，即时返回。
- 实测：1000 万条读数 / 2000 传感器，一批 10000 条约 0.24s（要求 3s），
  有效事件查询约 0.3ms。

## 测试

```bash
python3 -m pytest                 # 全部（含 1000 万条性能测试，建库约 20s）
COLDCHAIN_PERF_ROWS=1000000 python3 -m pytest   # 缩小性能测试规模
```

- `tests/test_coldchain.py`：乱序、重发、跨批接段、插入正常读数拆段、阈值切换前后晚到读数、
  15 分钟与 72 小时边界等规则单测。
- `tests/test_property.py`：同一份随机数据按不同切批与批内顺序提交，最终有效事件与
  "实际收下读数排序后从头算"的参考结果完全一致；同切批任意批内顺序，事件延续/撤销记录完全一致。
- `tests/test_perf.py`：性能验收。
