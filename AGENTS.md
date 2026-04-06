# AGENTS.md

RQAlpha is an algorithmic trading system for quantitative trading with backtesting and live trading capabilities.

**License**: Non-commercial use only (Apache 2.0). Commercial use requires authorization from Ricequant.

## Quick Commands

```bash
# Run backtest
rqalpha run -f strategy.py -s 2014-01-01 -e 2016-01-01 --account stock 100000

# With RQData connection
rqalpha run --rqdatac-uri tcp://user:password@host:port -f strategy.py -s 2014-01-01 -e 2016-01-01 --account stock 100000

# Download bundle data
rqalpha download-bundle

# Update bundle
rqalpha update-bundle --rqdatac-uri tcp://user:password@host:port

# Generate examples
rqalpha examples -d ./examples

# Run tests
pytest
pytest tests/unittest/
pytest tests/integration_tests/
```

## Minute Data Pipeline (Legacy 5m + 1m Simulation)

Use these files:
- `scripts/legacy_minute_data_builder.py`: fetch 5m, build 1m (`basic`/`stretch`/`both`)
- `rqalpha/examples/data_source/rqalpha_mod_legacy_1m_source.py`: custom 1m data source mod for RQAlpha

Default sqlite output path (current repo):
- `outputs/minute_data/stock_data.db`

Simulation modes:
- `basic`: split 5m into 1m (preferred when source 5m exists)
- `stretch`: generate 1m via similar-pattern matching + stretch (fallback synthetic data)
- `both`: build both and merge to runtime table with priority `basic > stretch`

Recommended runtime storage:
- partitioned runtime tables produced by `scripts/build_partitioned_runtime.py`
- registry table: `runtime_partition_registry`
- default partition naming: `stock_1_min_runtime_p_y<YYYY>_s<SYMBOL>`
- If same `(symbol, timestamp)` exists in both tables, runtime keeps the `basic/mock` row
- partitioned runtime is **runtime/backtest-only** and should be regenerated for the target date window.
- **Never** backfill full-history simulated data into a single runtime table under any circumstance; runtime must stay window-scoped.

### Minute Build Commands

```bash
# 1) Basic mode only (5m -> 1m mock)
python scripts/legacy_minute_data_builder.py \
  --sqlite-path /path/to/stock_data.db \
  --symbols 600519,000725 \
  --mock-mode basic

# 2) Stretch mode only (daily target OHLC -> synthetic 1m)
python scripts/legacy_minute_data_builder.py \
  --sqlite-path /path/to/stock_data.db \
  --symbols 600519,000725 \
  --skip-fetch \
  --mock-mode stretch \
  --stretch-source-table stock_1_min_mock \
  --stretch-output-table stock_1_min_fake

# 3) Legacy single-table merge (runtime priority: basic > stretch)
python scripts/legacy_minute_data_builder.py \
  --sqlite-path /path/to/stock_data.db \
  --symbols 600519,000725 \
  --skip-fetch \
  --mock-mode both \
  --one-min-table stock_1_min_mock \
  --stretch-output-table stock_1_min_fake \
  --runtime-table stock_1_min_runtime

# 4) Recommended current flow: build partitioned runtime by date + symbol
python scripts/build_partitioned_runtime.py \
  --sqlite-path /path/to/stock_data.db \
  --from-date 2024-03-27 \
  --to-date 2026-03-27 \
  --date-partition year \
  --runtime-prefix stock_1_min_runtime_p
```

### Runtime Validation Before Backtest

Always validate runtime 1m coverage before running a backtest:

```bash
python scripts/validate_minute_runtime_data.py \
  --sqlite-path /path/to/stock_data.db \
  --runtime-registry-table runtime_partition_registry \
  --from-date 2024-03-27 \
  --to-date 2026-03-27
```

Validation rules:
- every daily trading day in the target window must exist in the partitioned runtime referenced by `runtime_partition_registry`
- each runtime symbol-day must have the expected number of minute bars across all matching partitions
- if validation fails, do not treat the backtest as trustworthy

### RQAlpha With Minute Data Source Mod

```bash
python -m rqalpha run \
  -f strategies/minute_source_probe.py \
  -s 2026-03-03 -e 2026-03-04 \
  -fq 1m \
  --account stock 1000000 \
  -mc legacy_1m_source.enabled True \
  -mc legacy_1m_source.lib rqalpha.examples.data_source.rqalpha_mod_legacy_1m_source \
  -mc legacy_1m_source.sqlite_path /path/to/stock_data.db \
  -mc legacy_1m_source.runtime_registry_table runtime_partition_registry
```

Notes:
- If environment has `rqalpha` executable in PATH, `rqalpha run ...` is equivalent to `python -m rqalpha run ...`.
- In production/backtest mode, prefer `legacy_1m_source.runtime_registry_table runtime_partition_registry`.
- `legacy_1m_source.minute_table` can still be switched to `stock_1_min_mock` or `stock_1_min_fake` for mode-specific debugging.

## Interval Cursor Analysis (Legacy Migration)

Migrated script:
- `scripts/interval_cursor_analysis.py`

What it computes (ported from legacy `analysis.py`):
- Interval-wise `max` / `min`
- `max_rate_percent`
- `min_rate_percent`
- `cursor_rate`
- `monthly_annual_profit_rate`

Data source:
- Reads `stock_daily` from sqlite (default: `outputs/minute_data/stock_data.db`)
- Aggregates daily bars into monthly high/low and runs legacy interval split logic (`49,37,25,13,7,3`)

Optional cache table:
- `interval_analysis_results`

Example:

```bash
python scripts/interval_cursor_analysis.py \
  --sqlite-path outputs/minute_data/stock_data.db \
  --symbols 600519,000725 \
  --period-days 1825 \
  --output-csv outputs/analysis/interval_cursor_sample.csv
```

## Project-Specific Rules

### When Writing Strategies

1. **Always consult documentation first**: Read `docs/source/intro/tutorial.rst` and `docs/source/api/base_api.rst` before writing strategies
2. **Use correct API signatures**: Check `docs/source/api/base_api.rst` for function parameters and return types
3. **Follow strategy lifecycle**: Implement `init()`, `before_trading()`, `handle_bar()`, `after_trading()` in correct order
4. **Stock code format**: Always use format like "000001.XSHE" (code + exchange)
5. **Date format**: Use 'YYYY-MM-DD' format for dates

### When Debugging/Testing

1. **Write minimal reproducible examples**: Create smallest possible strategy that reproduces the bug
2. **Use logger, not print**: Use `logger.info()` instead of `print()` in strategies
3. **Add assertions**: Verify expected behavior with assertions in test code
4. **Short date ranges**: Use 1-3 month ranges for faster iteration during testing

### Code Modifications

1. **Chinese comments OK**: Domain-specific logic can use Chinese comments
2. **Follow PEP 8**: Standard Python style guide
3. **Test before commit**: Run pytest to ensure tests pass

## Key Architecture Points

- **Environment singleton**: Access via `Environment.get_instance()` - central registry for all components
- **Event-driven**: Mods subscribe to events (BAR, TICK, BEFORE_TRADING, etc.)
- **Mod system**: Extensibility through `AbstractMod` interface
- **Data bundle**: HDF5 format stored in `~/.rqalpha/bundle/`
- **Config hierarchy**: CLI args > strategy `__config__` > config file > defaults

## Detailed Documentation

For detailed information, see:
- **Architecture**: `docs/Codex/architecture.md` - Core components and system design
- **Strategy Writing**: `docs/Codex/strategy-guide.md` - How to write strategies with API reference
- **Bug Reproduction**: `docs/Codex/bug-reproduction.md` - Writing backtests to reproduce bugs
- **Development**: `docs/Codex/development.md` - Development guidelines and debugging

Official documentation:
- Tutorial: `docs/source/intro/tutorial.rst`
- API Reference: `docs/source/api/base_api.rst`
- Examples: `docs/source/intro/examples.rst`
