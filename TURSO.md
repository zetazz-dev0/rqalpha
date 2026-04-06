# Turso 连接说明

本文档记录本项目中连接 Turso/libSQL 云数据库的推荐方式，以及一个已验证可用的最小示例。

## 已验证环境

- 日期: 2026-04-06
- 项目根目录: `/Users/zeta/Projects/ricequant/rqalpha`
- Python: `3.11.7`
- 客户端库: `libsql-client`

安装依赖:

```bash
python -m pip install libsql-client
```

## 连接地址格式

如果你拿到的是下面这种 Turso 地址:

```text
libsql://<your-db-host>.turso.io
```

在当前 Python 客户端 `libsql-client` 中，推荐改成 `https://` 再连接:

```text
https://<your-db-host>.turso.io
```

### 原因

本项目环境里，直接使用 `libsql://...` 时，客户端会默认转成 websocket 连接，实测返回:

```text
505 Invalid response status
```

改用同一主机的 `https://...` 后，可以正常执行 `CREATE TABLE`、`INSERT` 和 `SELECT`。

## 推荐做法

不要把 token 硬编码进脚本，优先使用环境变量:

```bash
export TURSO_DATABASE_URL="https://your-db-host.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

如果你手上只有 `libsql://...` 地址，可以手动改成:

```bash
export TURSO_DATABASE_URL="https://your-db-host.turso.io"
```

## 最小可用 Python 示例

```python
import os
from datetime import datetime, timezone

import libsql_client


def main():
    url = os.environ["TURSO_DATABASE_URL"]
    auth_token = os.environ["TURSO_AUTH_TOKEN"]

    client = libsql_client.create_client_sync(url, auth_token=auth_token)
    try:
        client.execute(
            """
            CREATE TABLE IF NOT EXISTS codex_connectivity_test (
                id INTEGER PRIMARY KEY,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        created_at = datetime.now(timezone.utc).isoformat()
        client.execute(
            "INSERT INTO codex_connectivity_test (note, created_at) VALUES (?, ?)",
            ["connected-and-inserted", created_at],
        )

        result = client.execute(
            "SELECT id, note, created_at "
            "FROM codex_connectivity_test "
            "ORDER BY id DESC LIMIT 3"
        )

        for row in result.rows:
            print(row.asdict())
    finally:
        client.close()


if __name__ == "__main__":
    main()
```

运行方式:

```bash
python your_script.py
```

## 一次性命令示例

如果只是想快速验证连通性，也可以直接执行:

```bash
python - <<'PY'
import os
from datetime import datetime, timezone

import libsql_client

url = os.environ["TURSO_DATABASE_URL"]
token = os.environ["TURSO_AUTH_TOKEN"]

client = libsql_client.create_client_sync(url, auth_token=token)
try:
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS codex_connectivity_test (
            id INTEGER PRIMARY KEY,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    created_at = datetime.now(timezone.utc).isoformat()
    client.execute(
        "INSERT INTO codex_connectivity_test (note, created_at) VALUES (?, ?)",
        ["connected-and-inserted", created_at],
    )

    rows = client.execute(
        "SELECT id, note, created_at "
        "FROM codex_connectivity_test "
        "ORDER BY id DESC LIMIT 3"
    )

    for row in rows.rows:
        print(row.asdict())
finally:
    client.close()
PY
```

## 源表同步脚本

仓库内提供了一个可重复执行的同步脚本:

[`scripts/sync_turso_source_tables.py`](/Users/zeta/Projects/zetazz-dev0/rqalpha/scripts/sync_turso_source_tables.py)

它会把以下表同步到 Turso:

- `stock_daily`
- `stock_5_min`
- `stock_1_min_mock`

其中 `stock_5_min` 会按下面的顺序合并:

- 先写入 legacy 库 `/Users/zeta/Projects/python/stock-back-trader/archive/data/db/stock_data.db`
- 再写入当前库 `outputs/minute_data/stock_data.db`
- 如果 `(symbol, timestamp)` 冲突，以当前库为准覆盖 legacy

### 先做本地 dry-run

```bash
python scripts/sync_turso_source_tables.py --dry-run
```

### 正式同步

先准备环境变量:

```bash
export TURSO_DATABASE_URL="https://your-db-host.turso.io"
export TURSO_AUTH_TOKEN="your-token"
```

然后执行:

```bash
python scripts/sync_turso_source_tables.py --remote-count-check
```

### 可选参数

- `--batch-size 500`
- `--tables stock_daily,stock_5_min,stock_1_min_mock`
- `--skip-legacy-5min`
- `--dry-run`
- `--remote-count-check`

### 当前本地数据量参考

截至 2026-04-06，这批源表在本地的大致规模是:

- `stock_daily`: `72,110` 行
- `stock_5_min`:
  - 当前库 `201,524` 行
  - legacy 库 `190,200` 行
  - 两库合并去重后约 `211,704` 行
- `stock_1_min_mock`: `997,210` 行

这套数据量适合作为 Turso 中的“源数据层”，不建议把 `stock_1_min_fake` / `stock_1_min_runtime` 这类高频重建的大表也放进去。

## 本次实测结果

以下操作已经在该项目环境中完成并验证通过:

- 成功连接 Turso 数据库
- 成功创建表 `codex_connectivity_test`
- 成功插入 1 行测试数据
- 成功读回插入结果

读回的首条记录如下:

```text
{'id': 1, 'note': 'connected-and-inserted', 'created_at': '2026-04-06T07:47:03.984333+00:00'}
```

另外，本仓库后续还实际完成了一次源表同步到 Turso:

- `stock_daily`
- `stock_5_min`
- `stock_1_min_mock`

同步完成后的远端计数为:

```text
stock_daily      = 72110
stock_5_min      = 211704
stock_1_min_mock = 997210
```

## 常见问题

### 1. `libsql://...` 连不上

优先检查是否直接把 `libsql://` 用在了 Python 客户端里。若报 websocket 相关错误，改成同 host 的 `https://...` 再试。

### 2. token 泄漏风险

- 不要把 token 提交到 Git
- 优先使用环境变量
- 如 token 已经在聊天、日志或脚本里暴露，建议尽快去 Turso 后台轮换

### 3. `CREATE TABLE IF NOT EXISTS` 成功了，但后续 `INSERT` 很奇怪

这次实测里踩到一个很容易误判的点:

- `CREATE TABLE IF NOT EXISTS ...` 只会“在表不存在时创建”
- 如果远端已经有同名表，但 schema 和你当前脚本期待的不一致，它不会帮你修正
- 后续 `INSERT` 可能因为旧 schema 失败

例如这次远端已经存在一个更早创建的 `codex_connectivity_test`，其中有 `created_at NOT NULL` 列；后来再次执行:

```sql
CREATE TABLE IF NOT EXISTS codex_connectivity_test (
    id INTEGER PRIMARY KEY,
    note TEXT NOT NULL
)
```

SQL 本身会成功，但这不会把旧表改成新 schema。随后执行:

```sql
INSERT INTO codex_connectivity_test (note) VALUES ('x')
```

真正报错是:

```text
SQLite error: NOT NULL constraint failed: codex_connectivity_test.created_at
```

所以如果你碰到“明明建表成功，但插入失败”，先查远端实际 schema，不要只看当前脚本里的 `CREATE TABLE IF NOT EXISTS`。

### 4. `libsql-client` 旧版本可能把真实错误隐藏成 `KeyError: 'result'`

本机环境里的 `libsql-client` 版本是:

```text
0.3.1
```

这次实测发现，当 Turso 返回正常错误体:

```json
{"message":"SQLite error: ...","code":"SQLITE_CONSTRAINT"}
```

`libsql-client 0.3.1` 在某些情况下不会把这个错误正常包装出来，而是抛出类似:

```text
KeyError: 'result'
```

这会让人误以为是网络问题、协议问题或者地址问题，但实际常常只是 SQL 本身失败了。

如果遇到这种情况，推荐排查顺序是:

1. 先用最简单的 `SELECT 1` 测试连通性
2. 再直接执行一条确定不会冲突的 `CREATE TABLE` / `INSERT`
3. 如果客户端报 `KeyError: 'result'`，优先怀疑“真实 SQL 错误被旧客户端吃掉了”
4. 必要时直接请求 `https://<host>/v1/execute` 看原始 JSON 返回

### 5. 如何看原始 HTTP 返回

当怀疑 Python 客户端把真实错误吞掉时，可以直接请求 Turso 的 `v1/execute`:

```bash
python - <<'PY'
import json
import urllib.request

url = "https://your-db-host.turso.io/v1/execute"
token = "your-token"
sql = "SELECT 1 AS x"

req = urllib.request.Request(
    url,
    data=json.dumps({"stmt": {"sql": sql, "args": []}}).encode(),
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token,
    },
)

with urllib.request.urlopen(req, timeout=20) as resp:
    print(resp.read().decode())
PY
```

这次实测里，直接看原始返回能清楚区分:

- 正常成功:
  - `{"result": ...}`
- SQL 失败:
  - `{"message": "...", "code": "..."}`

### 6. 如何做读写权限验证

最直接的方法就是执行一组最小 SQL:

- `CREATE TABLE IF NOT EXISTS ...`
- `INSERT INTO ...`
- `SELECT ...`

如果三步都成功，说明当前 token 至少具备可用的读写权限。
