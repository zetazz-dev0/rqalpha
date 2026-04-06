# Cursor Rate Layered Strategy: Backtest Build Summary

## 1. Purpose

This document summarizes the current build state of the `cursor_rate_layered_strategy`, the minute-data preparation pipeline used by the strategy, the current verified backtest status in this repository, and the main engineering lessons learned during debugging and validation.

The goal is not to restate the trading spec in full, but to explain:

- what the strategy currently depends on
- how the minute backtest data is prepared
- what has been verified to be working
- which conclusions are already credible
- where the remaining risks still are

## 2. Current Strategy Layout

Core strategy files:

- `strategies/cursor_rate_layered_strategy.py`
- `strategies/cursor_rate_layered/logic.py`
- `strategies/cursor_rate_layered/models.py`
- `strategies/cursor_rate_layered/params.py`
- `tests/unittest/strategy/test_cursor_rate_layered_logic.py`

Supporting scripts:

- `scripts/legacy_minute_data_builder.py`
- `scripts/validate_minute_runtime_data.py`
- `scripts/cursor_layered_component_report.py`
- `scripts/equal_weight_benchmark_report.py`
- `rqalpha/examples/data_source/rqalpha_mod_legacy_1m_source.py`

## 3. Trading Logic Snapshot

This strategy is a multi-symbol stock strategy built around `cursor rate` signals and minute-level execution.

At a high level:

- Day0 entry requires a stricter low-position signal.
- Day1+ allows continued execution under a looser low-position signal.
- A stock-level `buy_permission` gate can freeze all future buys after a clear breakdown.
- Day0 and Day1+ share one minute-level buy engine.
- Forward T uses `buy first, sell old inventory later`.
- Reverse T uses `sell first, buy back later` only for positions already entering an exit stage.
- Pending intraday actions are tracked explicitly instead of being inferred from end-of-day holdings.

Operationally, the strategy records two debug ledgers on each run:

- event ledger: `outputs/backtest/cursor_layered_debug/cursor_layered_events_<window>.csv`
- day summary ledger: `outputs/backtest/cursor_layered_debug/cursor_layered_day_summary_<window>.csv`

These ledgers are essential for understanding which part of the strategy generated or lost money.

## 4. Minute Data Pipeline

The strategy depends on legacy-style minute simulation built from a sqlite database.

Current database path:

- `outputs/minute_data/stock_data.db`

Key tables:

- `stock_daily`: daily bars used for signal calculation
- `stock_5_min`: legacy 5-minute source bars
- `stock_1_min_mock`: 5m -> 1m split result
- `stock_1_min_fake`: stretched minute patterns matched from similar days
- `stock_1_min_synthetic`: direct synthetic 1m fallback from daily OHLCV
- `stock_1_min_runtime`: legacy single runtime table
- `runtime_partition_registry`: partition registry for runtime tables
- `stock_1_min_runtime_p_y<YYYY>_s<SYMBOL>`: current partitioned runtime table pattern

Current partitioned runtime db facts in this repository:

- sqlite file: `outputs/minute_data/stock_data.db`
- registry partition count: `90`
- runtime rows via registry: `3,474,240`
- runtime date range: `2024-03-28` to `2026-03-27`
- runtime symbols in registry: `30`

There is also a legacy single-table runtime path in this repository:

- `stock_1_min_runtime`

In the main database, that legacy runtime table is now intentionally cleared, and the active backtest path is the partitioned registry-based runtime.

But the current RQAlpha minute source mod has already been updated to support partitioned runtime through:

- `legacy_1m_source.runtime_registry_table`

So the repository now has two runnable paths:

- legacy single-table runtime
- partitioned runtime through `runtime_partition_registry`

## 5. What Was Broken Before

The main reliability issue was not initially strategy alpha, but minute-data quality.

Earlier failures came from:

- partial trading days in mock/fake/runtime minute tables
- fake generation matching daily templates that had no complete minute template behind them
- synthetic fallback being blocked by partial source days that were incorrectly treated as complete coverage
- backtests being run without a hard validation gate between data preparation and execution

This made early long-window results unreliable. The system could appear to run, while many symbol-days actually had incomplete or structurally distorted minute bars.

## 6. What Was Fixed in the Minute Pipeline

The builder and validator were tightened substantially.

### 6.1 Partial-day filtering

`legacy_minute_data_builder.py` now enforces:

- only complete `48`-bar 5m days can generate mock 1m data
- only complete `240`-bar 1m template days can enter the stretch template pool
- stretch output is rejected if the matched template day is not complete
- runtime merge only accepts complete `240`-bar symbol-days
- partial source days no longer block synthetic fallback

### 6.2 Validation gate before backtest

`validate_minute_runtime_data.py` now checks:

- missing symbol-days
- extra symbol-days
- partial symbol-days
- volume anomalies

Current default validation thresholds:

- expected bars per day: `240`
- min total volume: `1`
- min nonzero bars: `24`
- max single-bar volume share: `0.5`

This means a backtest should not be treated as trustworthy unless runtime validation passes first.

## 7. Current Verified Build Status in This Repository

### 7.1 Syntax and unit tests

Verified:

```bash
python -m py_compile \
  scripts/validate_minute_runtime_data.py \
  scripts/cursor_layered_component_report.py \
  scripts/equal_weight_benchmark_report.py \
  scripts/export_watchlist_data.py \
  strategies/cursor_rate_layered_strategy.py \
  strategies/cursor_rate_layered/logic.py \
  strategies/cursor_rate_layered/models.py \
  strategies/cursor_rate_layered/params.py \
  rqalpha/examples/data_source/rqalpha_mod_legacy_1m_source.py

pytest -q tests/unittest/strategy/test_cursor_rate_layered_logic.py
```

Unit test status:

- `6 passed`

### 7.2 Runtime validation

Historical validated legacy window:

- `2024-03-28` to `2025-03-27`

Command:

```bash
python scripts/validate_minute_runtime_data.py \
  --sqlite-path outputs/minute_data/stock_data.db \
  --from-date 2024-03-28 \
  --to-date 2025-03-27
```

Result:

- `total_daily_symbol_days = 6746`
- `total_runtime_symbol_days = 6746`
- `missing = 0`
- `extra = 0`
- `partial = 0`
- `volume_anomaly = 0`
- `validation_status = PASS`

This is the first hard criterion that now has to pass before taking any backtest result seriously.

Current validated partitioned main-db window:

- `2024-03-28` to `2026-03-27`

Command:

```bash
python scripts/validate_minute_runtime_data.py \
  --sqlite-path outputs/minute_data/stock_data.db \
  --runtime-registry-table runtime_partition_registry \
  --symbols 000069,000423,000538,000725,000895,002033,300122,300142,600000,600016,600030,600036,600059,600085,600111,600138,600161,600276,600315,600332,600436,600456,600519,600535,600600,600887,601088,601222,601318,601888 \
  --from-date 2024-03-28 \
  --to-date 2026-03-27
```

Result:

- `total_daily_symbol_days = 14476`
- `total_runtime_symbol_days = 14476`
- `missing = 0`
- `extra = 0`
- `partial = 0`
- `volume_anomaly = 0`
- `validation_status = PASS`
- `perfect_symbols = 30/30`

This means the current main database now holds a complete 2-year partitioned runtime window for the 30-symbol working set.

## 8. Current Verified Backtest Result in This Repository

Historical verified legacy backtest window:

- `2024-03-28` to `2025-03-27`

Command:

```bash
python -m rqalpha run \
  -f strategies/cursor_rate_layered_strategy.py \
  -s 2024-03-28 -e 2025-03-27 \
  -fq 1m \
  --account stock 1000000 \
  -mc sys_analyser.enabled True \
  -mc sys_analyser.output_file outputs/backtest/cursor_layered_one_year_verify.pkl \
  -mc sys_analyser.report_save_path outputs/backtest/cursor_layered_one_year_verify_report \
  -mc legacy_1m_source.enabled True \
  -mc legacy_1m_source.lib rqalpha.examples.data_source.rqalpha_mod_legacy_1m_source \
  -mc legacy_1m_source.sqlite_path outputs/minute_data/stock_data.db \
  -mc legacy_1m_source.minute_table stock_1_min_runtime
```

Artifacts:

- `outputs/backtest/cursor_layered_one_year_verify.pkl`
- `outputs/backtest/cursor_layered_one_year_verify_report/summary.xlsx`

Result summary:

- total return: `9.9138%`
- annualized return: `10.3891%`
- max drawdown: `3.9012%`
- sharpe: `1.3191`
- trades: `1662`
- first trade: `2024-03-28 09:38:00`
- last trade: `2025-03-27 14:59:00`

This confirms that the migrated strategy and runtime source can execute a one-year minute backtest end-to-end in this repository.

Current verified partitioned backtest window:

- `2026-03-03` to `2026-03-27`

Command:

```bash
python -m rqalpha run \
  -f strategies/cursor_rate_layered_strategy.py \
  -s 2026-03-03 -e 2026-03-27 \
  -fq 1m \
  --account stock 1000000 \
  -mc sys_analyser.enabled True \
  -mc sys_analyser.output_file outputs/backtest/cursor_layered_partitioned_short_verify_main.pkl \
  -mc sys_analyser.report_save_path outputs/backtest/cursor_layered_partitioned_short_verify_main_report \
  -mc legacy_1m_source.enabled True \
  -mc legacy_1m_source.lib rqalpha.examples.data_source.rqalpha_mod_legacy_1m_source \
  -mc legacy_1m_source.sqlite_path /Users/zeta/Projects/zetazz-dev0/rqalpha/outputs/minute_data/stock_data.db \
  -mc legacy_1m_source.runtime_registry_table runtime_partition_registry
```

Artifacts:

- `outputs/backtest/cursor_layered_partitioned_short_verify_main.pkl`
- `outputs/backtest/cursor_layered_partitioned_short_verify_main_report/summary.xlsx`

Result summary:

- total return: `-0.5812%`
- annualized return: `-7.4403%`
- max drawdown: `1.3634%`
- sharpe: `-1.9120`
- trades: `301`
- end-to-end exit code: `0`

This confirms that the strategy can load partitioned runtime data and complete a real minute backtest end-to-end in this repository.

## 9. Historical Debugging Conclusions That Still Matter

These were learned while iterating on longer windows and still apply to the current strategy.

### 9.1 Reverse T was initially blocked by execution-path logic

The main issue was not originally parameter choice.

The reverse-T buyback path had previously been tied to the same low-position candidate pool used for new buys. That was structurally wrong, because reverse T applies in high-position exit contexts rather than low-position entry contexts.

After the execution path was corrected, reverse T stopped being completely dead. However, even after the bug fix, reverse T remained weak as a profit source.

Practical conclusion:

- reverse T is currently a small enhancement, not a core PnL driver

### 9.2 The strategy can enter long quiet periods

Another important finding is that the strategy can reach a state where:

- positions still exist
- exit conditions have not triggered
- many symbols still show Day0/Day1 signals
- but `buy_permission` has already been frozen

That creates long periods with no new buys and no realized turnover.

Practical conclusion:

- later-stage stagnation is a real behavior of the current ruleset
- this is not necessarily a data issue once validation already passes
- it is a strategy-state issue and should be analyzed explicitly before changing parameters

### 9.3 Most realized profit does not currently come from T itself

From earlier component analysis in the source repository, the main realized PnL driver was exit selling rather than forward/reverse T.

Practical conclusion:

- forward T contributes, but modestly
- reverse T contributes very little
- exit logic still dominates the current realized PnL structure

## 10. What Is Working Well Now

1. The minute-data build is now defendable.
2. Runtime validation exists and is actionable.
3. The strategy can be executed reproducibly in minute frequency.
4. The target clone reproduces a full one-year backtest successfully on the historical legacy runtime path.
5. The main database now also provides a complete 2-year partitioned runtime window for minute backtests.
6. Strategy-side event and day ledgers make component analysis possible.

## 11. What Is Still Fragile or Incomplete

1. The partitioned runtime path now runs from the main database, but long-window strategy verification on that partitioned path has not yet been rerun beyond the short verified window.
2. Reverse T remains quantitatively weak even after the execution-path fix.
3. Volume-limit warnings still appear frequently in minute backtests.
4. The strategy can become inactive for long periods due to the interaction between `buy_permission`, exit timing, and held inventory.
5. The historical one-year result is still the legacy single-table run; it has not yet been replayed on the rebuilt main-db partitioned runtime.

## 12. Recommended Operating Procedure

When using this strategy in this repository, use this sequence:

1. build or refresh minute runtime data
2. validate runtime coverage and volume quality
3. run a short backtest first
4. only then run a wider window
5. if needed, generate component and benchmark reports

Recommended command sequence:

```bash
# 1. Validate minute runtime
python scripts/validate_minute_runtime_data.py \
  --sqlite-path outputs/minute_data/stock_data.db \
  --runtime-registry-table runtime_partition_registry \
  --symbols 000069,000423,000538,000725,000895,002033,300122,300142,600000,600016,600030,600036,600059,600085,600111,600138,600161,600276,600315,600332,600436,600456,600519,600535,600600,600887,601088,601222,601318,601888 \
  --from-date 2026-03-03 \
  --to-date 2026-03-27

# 2. Run strategy
python -m rqalpha run \
  -f strategies/cursor_rate_layered_strategy.py \
  -s 2026-03-03 -e 2026-03-27 \
  -fq 1m \
  --account stock 1000000 \
  -mc sys_analyser.enabled True \
  -mc sys_analyser.output_file outputs/backtest/cursor_layered_partitioned_short_verify_main.pkl \
  -mc sys_analyser.report_save_path outputs/backtest/cursor_layered_partitioned_short_verify_main_report \
  -mc legacy_1m_source.enabled True \
  -mc legacy_1m_source.lib rqalpha.examples.data_source.rqalpha_mod_legacy_1m_source \
  -mc legacy_1m_source.sqlite_path /Users/zeta/Projects/zetazz-dev0/rqalpha/outputs/minute_data/stock_data.db \
  -mc legacy_1m_source.runtime_registry_table runtime_partition_registry

# 3. Optional: component split
python scripts/cursor_layered_component_report.py \
  --event-csv outputs/backtest/cursor_layered_debug/cursor_layered_events_20260303_20260327.csv \
  --day-csv outputs/backtest/cursor_layered_debug/cursor_layered_day_summary_20260303_20260327.csv \
  --result-pickle outputs/backtest/cursor_layered_partitioned_short_verify_main.pkl \
  --report-dir outputs/backtest/cursor_layered_partitioned_short_verify_main_report \
  --output-dir outputs/backtest/cursor_layered_debug/partitioned_short_component_report

# 4. Optional: equal-weight benchmark post-processing
python scripts/equal_weight_benchmark_report.py \
  --result-pickle outputs/backtest/cursor_layered_partitioned_short_verify_main.pkl \
  --sqlite-path /Users/zeta/Projects/zetazz-dev0/rqalpha/outputs/minute_data/stock_data.db \
  --output-csv outputs/backtest/cursor_layered_partitioned_short_equal_weight_benchmark.csv
```

## 13. Recommended Next Steps

If work continues from the current state, the best next steps are:

1. rerun a 3-year or 4-year validation in this clone, not only in the source workspace
2. regenerate component analysis in this clone for a wider window
3. specifically analyze long inactive periods caused by frozen `buy_permission`
4. rerun a one-year or wider strategy backtest on the rebuilt main-db partitioned runtime, not only the short verification window
5. keep the validation gate mandatory before every serious backtest conclusion

## 14. Bottom Line

The main progress is not only that the strategy exists, but that the surrounding engineering process is now much more credible.

Current state:

- strategy implementation exists
- minute runtime build exists
- runtime validation exists
- target clone migration is complete
- historical one-year legacy minute backtest in this clone has been verified successfully
- main database partitioned runtime now covers `2024-03-28 -> 2026-03-27` with `30` symbols and passes validation
- partitioned runtime minute backtest in this clone has been verified successfully on a short window

The current conclusion that is safe to state is:

- the build and backtest workflow is now operational and reproducible in both legacy and partitioned forms
- the one-year migrated legacy run is credible enough to use as a baseline
- the partitioned path is technically runnable and the main database now has a validated 2-year runtime window, but longer strategy backtests should still be rerun explicitly before drawing strategy conclusions
- future strategy evaluation should focus more on component quality and state behavior than on raw backtest execution correctness
