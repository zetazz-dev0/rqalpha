# 分区 Runtime 回测运行说明

本文档说明如何让 RQAlpha 回测直接加载按 `year + symbol` 分区的 1 分钟 runtime 数据，并给出一套已经在本仓库验证通过的最小运行实例。

适用前提:

- 本地 sqlite 已经包含:
  - `stock_daily`
  - `stock_5_min`
  - `stock_1_min_mock`
  - `stock_1_min_fake`
  - `runtime_partition_registry`
- runtime 分区表名类似:
  - `stock_1_min_runtime_p_y2024_s600519`
  - `stock_1_min_runtime_p_y2025_s000725`
- 已使用:
  - [`scripts/build_partitioned_runtime.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/scripts/build_partitioned_runtime.py)

当前仓库中，分区 runtime 的读取入口是:

- [`rqalpha/examples/data_source/rqalpha_mod_legacy_1m_source.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/rqalpha/examples/data_source/rqalpha_mod_legacy_1m_source.py)

它现在支持两种模式:

- 旧模式: 单表 `minute_table`
- 新模式: `runtime_registry_table`

当 `runtime_registry_table` 被设置时，mod 会优先从注册表加载分区表，不再依赖单一 `stock_1_min_runtime` 大表。

## 回测前检查

先校验目标窗口内的 runtime 覆盖是否完整:

```bash
python scripts/validate_minute_runtime_data.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --runtime-registry-table runtime_partition_registry \
  --from-date 2024-03-28 \
  --to-date 2026-03-27
```

如果只想先检查个别 symbol:

```bash
python scripts/validate_minute_runtime_data.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --runtime-registry-table runtime_partition_registry \
  --symbols 600519,000725,601222 \
  --from-date 2024-03-28 \
  --to-date 2026-03-27
```

校验通过后再跑回测。不要跳过这一步。

## 最小可运行回测命令

下面这条命令已经在本仓库实际跑通:

```bash
python -m rqalpha run \
  -f strategies/minute_source_probe.py \
  -s 2026-03-03 \
  -e 2026-03-04 \
  -fq 1m \
  --account stock 1000000 \
  -mc legacy_1m_source.enabled True \
  -mc legacy_1m_source.lib rqalpha.examples.data_source.rqalpha_mod_legacy_1m_source \
  -mc legacy_1m_source.sqlite_path /Users/zeta/Projects/zetazz-dev0/rqalpha/outputs/turso_runtime/turso_2y_all.db \
  -mc legacy_1m_source.runtime_registry_table runtime_partition_registry
```

对应的策略文件:

- [`strategies/minute_source_probe.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/strategies/minute_source_probe.py)

本次实测输出的关键日志:

```text
[2026-03-03 09:31:00.000000] INFO: user_log: minute probe dt=2026-03-03 09:31:00, close=1440.1, last5=[1440.0, 1441.0, 1440.93, 1440.11, 1440.1]
```

命令退出码为 `0`，说明:

- 自定义 mod 已成功加载
- 分区 registry 已成功解析
- `600519.XSHG` 的 1 分钟历史已被回测引擎正常消费
- `history_bars(..., "1m", ...)` 可以直接读取分区 runtime

## 命令参数说明

- `legacy_1m_source.enabled`
  - 启用自定义分钟数据源 mod
- `legacy_1m_source.lib`
  - 指向分区读取版 mod
- `legacy_1m_source.sqlite_path`
  - 指向本地 sqlite
- `legacy_1m_source.runtime_registry_table`
  - 指向分区注册表

这里最关键的是:

- 用 `legacy_1m_source.*`
- 不要写成 `mod.legacy_1m_source.*`

错误写法:

```bash
-mc mod.legacy_1m_source.enabled True
```

这样会在 `config.mod` 下面再包一层 `mod`，启动时会触发:

```text
AttributeError: 'RqAttrDict' object has no attribute 'enabled'
```

## 使用经验

1. 分区 runtime 模式下，回测入口不需要再维护单一 `stock_1_min_runtime` 总表。
2. `runtime_partition_registry` 比单表更适合窗口化回测，库体积和重建成本都更可控。
3. 回测前先跑校验脚本，能提前发现缺 symbol、缺交易日、缺分钟线的问题。
4. `minute_table` 可保留给旧流程调试，但在设置了 `runtime_registry_table` 后，实际读取路径会走 registry 模式。
5. 建议把回测日期压在 registry 可覆盖的窗口内。当前实测库 [`outputs/turso_runtime/turso_2y_all.db`](/Users/zeta/Projects/zetazz-dev0/rqalpha/outputs/turso_runtime/turso_2y_all.db) 的可用范围是 `2024-03-28 -> 2026-03-27`。

## 常用排查

看有哪些 runtime 分区:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT table_name, symbol, partition_value, row_count, trading_day_count
FROM runtime_partition_registry
ORDER BY symbol, partition_value;
"
```

看某个 symbol 的分区:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT table_name, from_date, to_date, row_count, trading_day_count
FROM runtime_partition_registry
WHERE symbol = '600519'
ORDER BY partition_value;
"
```

看回测窗口是否超出当前库覆盖范围:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT MIN(from_date), MAX(to_date)
FROM runtime_partition_registry;
"
```

## 推荐流程

1. 从 Turso 拉取 `stock_daily`、`stock_5_min`、`stock_1_min_mock`
2. 在本地生成 `stock_1_min_fake`
3. 在本地写入分区 runtime 表和 `runtime_partition_registry`
4. 用 `validate_minute_runtime_data.py` 检查目标回测窗口
5. 用 `legacy_1m_source.runtime_registry_table` 启动 `rqalpha run`

上游数据准备说明见:

- [`docs/personal/TURSO_RUNTIME_LOCAL_SQLITE.md`](/Users/zeta/Projects/zetazz-dev0/rqalpha/docs/personal/TURSO_RUNTIME_LOCAL_SQLITE.md)
