# Turso 存量源数据拉取与本地 Runtime 分区生成

本文档说明如何把 Turso 里的源数据拉回本地 sqlite，并在本地生成可回测用的 runtime 分区表。

目标数据流:

```text
Turso(stock_daily, stock_5_min, stock_1_min_mock)
  -> 本地 sqlite 缓存
  -> 本地 stretch 生成 stock_1_min_fake
  -> 本地 runtime 分区表(按 date + symbol)
```

这里的 runtime 分区不是单一大表，而是:

- 按 `symbol`
- 再按日期分区
- 当前脚本支持 `year` 或 `month`

推荐默认:

- 日期分区粒度: `year`
- 表名格式: `stock_1_min_runtime_p_y<YYYY>_s<SYMBOL>`

例如:

- `stock_1_min_runtime_p_y2024_s600519`
- `stock_1_min_runtime_p_y2025_s000725`

## 前提

1. 本地已安装 `libsql-client`
2. 已设置 Turso 环境变量

```bash
export TURSO_DATABASE_URL="https://your-db-host.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

如果你手里是 `libsql://...`，脚本会自动转成 `https://...`

## 相关脚本

- 从 Turso 拉源表到本地:
  - [`scripts/pull_turso_source_tables.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/scripts/pull_turso_source_tables.py)
- 用本地 `daily + mock` 生成 `fake` 并写 runtime 分区:
  - [`scripts/build_partitioned_runtime.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/scripts/build_partitioned_runtime.py)

## 第一步：从 Turso 拉源表到本地 sqlite

示例命令:

```bash
python scripts/pull_turso_source_tables.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --from-date 2024-03-28 \
  --to-date 2026-03-27 \
  --tables stock_daily,stock_5_min,stock_1_min_mock \
  --batch-size 5000
```

说明:

- 默认从远端 `stock_daily` 自动解析 symbol 列表
- 会把 Turso 中这三张表的数据写进本地 sqlite
- 写入方式是 `INSERT OR REPLACE`
- 如果目标 sqlite 不存在，会自动创建

拉取完成后，可以快速看一下本地计数:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT 'stock_daily', COUNT(*) FROM stock_daily
UNION ALL
SELECT 'stock_5_min', COUNT(*) FROM stock_5_min
UNION ALL
SELECT 'stock_1_min_mock', COUNT(*) FROM stock_1_min_mock;
"
```

## 第二步：在本地生成 partitioned runtime

示例命令:

```bash
python scripts/build_partitioned_runtime.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --from-date 2024-03-28 \
  --to-date 2026-03-27 \
  --date-partition year \
  --runtime-prefix stock_1_min_runtime_p
```

脚本内部会做这些事:

1. 读取本地 `stock_daily`
2. 使用本地 `stock_1_min_mock` 作为 stretch 模板
3. 生成本地 `stock_1_min_fake`
4. 以 `fake -> mock` 的顺序写入 runtime 分区表
5. 用 `mock` 覆盖同 `(symbol, timestamp)` 的 `fake`
6. 维护一张注册表 `runtime_partition_registry`

其中 runtime 的优先级仍然是:

```text
mock > fake
```

## 分区表与注册表

脚本会生成:

- 多张 runtime 分区表
- 1 张注册表 `runtime_partition_registry`

注册表字段:

- `table_name`
- `symbol`
- `partition_kind`
- `partition_value`
- `from_date`
- `to_date`
- `row_count`
- `trading_day_count`
- `created_at`

查看所有 runtime 分区:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT table_name, symbol, partition_value, row_count, trading_day_count
FROM runtime_partition_registry
ORDER BY symbol, partition_value;
"
```

## 2 年全量操作实例

下面是一套直接可执行的 2 年全量样例。

时间窗口:

- `2024-03-28 -> 2026-03-27`

说明:

- 这是“按 2 年窗口拉 Turso 源表 + 本地生成 runtime 分区”的全流程
- 如果 `stock_1_min_mock` 在窗口前半段没有覆盖，脚本会依赖 `stock_daily + stock_1_min_mock` 自动生成 `stock_1_min_fake`
- 最终 runtime 以分区表形式落地，不会生成单一超大 runtime 总表

### 2 年全量拉取

```bash
python scripts/pull_turso_source_tables.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --from-date 2024-03-28 \
  --to-date 2026-03-27 \
  --tables stock_daily,stock_5_min,stock_1_min_mock \
  --batch-size 5000
```

### 2 年全量 runtime 分区生成

```bash
python scripts/build_partitioned_runtime.py \
  --sqlite-path outputs/turso_runtime/turso_2y_all.db \
  --from-date 2024-03-28 \
  --to-date 2026-03-27 \
  --date-partition year \
  --runtime-prefix stock_1_min_runtime_p
```

### 2 年样例验证

看本地源表计数:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT 'stock_daily', COUNT(*) FROM stock_daily
UNION ALL
SELECT 'stock_5_min', COUNT(*) FROM stock_5_min
UNION ALL
SELECT 'stock_1_min_mock', COUNT(*) FROM stock_1_min_mock
UNION ALL
SELECT 'stock_1_min_fake', COUNT(*) FROM stock_1_min_fake;
"
```

看 runtime 分区数量和总行数:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT COUNT(*) AS partition_count, COALESCE(SUM(row_count), 0) AS total_rows
FROM runtime_partition_registry;
"
```

看某个 symbol 的分区情况:

```bash
sqlite3 outputs/turso_runtime/turso_2y_all.db "
SELECT table_name, row_count, trading_day_count
FROM runtime_partition_registry
WHERE symbol = '600519'
ORDER BY partition_value;
"
```

## 本次实测验证结果

截至 2026-04-07，以上 2 年实例已经在本仓库环境中实际执行过。

本地 2 年缓存库:

- `outputs/turso_runtime/turso_2y_all.db`

实测得到的本地源表计数:

```text
stock_daily      = 14476
stock_5_min      = 211704
stock_1_min_mock = 997210
stock_1_min_fake = 3473520
```

实测生成的 runtime 分区:

- 分区粒度: `year`
- 运行后写入 `runtime_partition_registry`
- 分区表数量: `90`
- runtime 总行数: `3473520`

实测 `600519` 的分区结果:

```text
stock_1_min_runtime_p_y2024_s600519  44640  186
stock_1_min_runtime_p_y2025_s600519  58320  243
stock_1_min_runtime_p_y2026_s600519  12960  54
```

如果你后面要改成按月分区，把 `--date-partition year` 改成 `--date-partition month` 即可。
