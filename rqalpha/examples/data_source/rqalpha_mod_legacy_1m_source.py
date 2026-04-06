# -*- coding: utf-8 -*-
import os
import re
import sqlite3
from datetime import date, datetime, time
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from rqalpha.const import INSTRUMENT_TYPE
from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.environment import Environment
from rqalpha.interface import AbstractMod
from rqalpha.model.tick import TickObject
from rqalpha.utils.datetime_func import convert_dt_to_int


__config__ = {
    # Ensure minute data source is set before system mods begin to consume data.
    "priority": 40,
    # Path of minute sqlite db. Example:
    # /path/to/rqalpha/outputs/minute_data/stock_data.db
    "sqlite_path": None,
    # Minute table generated from legacy 5m -> 1m mock logic.
    "minute_table": "stock_1_min_mock",
}


VALID_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, field_name: str) -> str:
    if not isinstance(name, str) or not VALID_SQL_IDENTIFIER.match(name):
        raise RuntimeError(
            "Invalid {}: {!r}. Only alphanumeric + underscore table names are allowed.".format(
                field_name, name
            )
        )
    return name


class LegacyMinuteDataSource(BaseDataSource):
    MINUTE_DTYPE = np.dtype(
        [
            ("datetime", "<u8"),
            ("open", "<f8"),
            ("close", "<f8"),
            ("high", "<f8"),
            ("low", "<f8"),
            ("volume", "<f8"),
            ("total_turnover", "<f8"),
        ]
    )

    SUPPORTED_1M_TYPES = {
        INSTRUMENT_TYPE.CS,
        INSTRUMENT_TYPE.ETF,
        INSTRUMENT_TYPE.LOF,
        INSTRUMENT_TYPE.INDX,
        INSTRUMENT_TYPE.REITs,
        INSTRUMENT_TYPE.PUBLIC_FUND,
    }

    def __init__(self, base_config, sqlite_path: str, minute_table: str = "stock_1_min_mock"):
        super(LegacyMinuteDataSource, self).__init__(base_config)
        self._sqlite_path = os.path.abspath(sqlite_path)
        self._minute_table = _validate_identifier(minute_table, "minute_table")
        self._minute_cache: Dict[str, np.ndarray] = {}
        self._minute_range_cache: Optional[Tuple[date, date]] = None
        self._validate_source()

    def _validate_source(self) -> None:
        if not os.path.exists(self._sqlite_path):
            raise RuntimeError("Minute source sqlite not found: {}".format(self._sqlite_path))

        with sqlite3.connect(self._sqlite_path) as conn:
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (self._minute_table,),
            ).fetchone()
            if table_exists is None:
                raise RuntimeError(
                    "Minute source table '{}' not found in sqlite: {}".format(
                        self._minute_table, self._sqlite_path
                    )
                )

            count = conn.execute(
                "SELECT COUNT(1) FROM {}".format(self._minute_table)
            ).fetchone()[0]
            if count <= 0:
                raise RuntimeError(
                    "Minute source table '{}' has no rows in sqlite: {}".format(
                        self._minute_table, self._sqlite_path
                    )
                )

    @staticmethod
    def _symbol_from_instrument(instrument) -> str:
        return instrument.order_book_id.split(".")[0]

    def _load_minute_bars(self, symbol: str) -> np.ndarray:
        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM {table}
            WHERE symbol = ?
            ORDER BY timestamp
        """.format(table=self._minute_table)
        with sqlite3.connect(self._sqlite_path) as conn:
            df = pd.read_sql_query(query, conn, params=(symbol,))

        if df.empty:
            return np.array([], dtype=self.MINUTE_DTYPE)

        timestamps = pd.to_datetime(df["timestamp"])
        dt_ints = timestamps.map(convert_dt_to_int).to_numpy(dtype=np.uint64)

        bars = np.empty(len(df), dtype=self.MINUTE_DTYPE)
        bars["datetime"] = dt_ints
        bars["open"] = df["open"].astype(float).to_numpy()
        bars["close"] = df["close"].astype(float).to_numpy()
        bars["high"] = df["high"].astype(float).to_numpy()
        bars["low"] = df["low"].astype(float).to_numpy()
        bars["volume"] = df["volume"].astype(float).to_numpy()
        bars["total_turnover"] = (df["close"] * df["volume"]).astype(float).to_numpy()
        return bars

    def _get_minute_bars(self, instrument) -> np.ndarray:
        if instrument.type not in self.SUPPORTED_1M_TYPES:
            return np.array([], dtype=self.MINUTE_DTYPE)

        symbol = self._symbol_from_instrument(instrument)
        if symbol not in self._minute_cache:
            self._minute_cache[symbol] = self._load_minute_bars(symbol)
        return self._minute_cache[symbol]

    def get_bar(self, instrument, dt, frequency):
        if frequency != "1m":
            return super(LegacyMinuteDataSource, self).get_bar(instrument, dt, frequency)

        bars = self._get_minute_bars(instrument)
        if len(bars) == 0:
            return None

        dt_int = np.uint64(convert_dt_to_int(dt))
        pos = bars["datetime"].searchsorted(dt_int)
        if pos >= len(bars) or bars["datetime"][pos] != dt_int:
            return None
        return bars[pos]

    def history_bars(
        self,
        instrument,
        bar_count,
        frequency,
        fields,
        dt,
        skip_suspended=True,
        include_now=False,
        adjust_type="pre",
        adjust_orig=None,
    ):
        if frequency != "1m":
            return super(LegacyMinuteDataSource, self).history_bars(
                instrument,
                bar_count,
                frequency,
                fields,
                dt,
                skip_suspended=skip_suspended,
                include_now=include_now,
                adjust_type=adjust_type,
                adjust_orig=adjust_orig,
            )

        bars = self._get_minute_bars(instrument)
        if not self._are_fields_valid(fields, bars.dtype.names):
            from rqalpha.utils.exception import RQInvalidArgument
            raise RQInvalidArgument("invalid fields: {}".format(fields))

        if len(bars) == 0:
            return bars if fields is None else bars[fields]

        dt_int = np.uint64(convert_dt_to_int(dt))
        side = "right" if include_now else "left"
        right = bars["datetime"].searchsorted(dt_int, side=side)
        left = 0 if bar_count is None else max(0, right - bar_count)
        window = bars[left:right]
        return window if fields is None else window[fields]

    def current_snapshot(self, instrument, frequency, dt):
        if frequency != "1m":
            raise NotImplementedError

        bars = self._get_minute_bars(instrument)
        if len(bars) == 0:
            return None

        dt_int = np.uint64(convert_dt_to_int(dt))
        right = bars["datetime"].searchsorted(dt_int, side="right")
        if right <= 0:
            return None

        day_start = np.uint64(convert_dt_to_int(datetime.combine(dt.date(), time.min)))
        left = bars["datetime"].searchsorted(day_start, side="left")
        intraday = bars[left:right]
        if len(intraday) == 0:
            return None

        env = Environment.get_instance()
        prev_close = np.nan
        try:
            prev_date = env.data_proxy.get_previous_trading_date(dt.date())
            prev_bar = super(LegacyMinuteDataSource, self).get_bar(instrument, prev_date, "1d")
            if prev_bar is not None:
                prev_close = float(prev_bar["close"])
        except Exception:
            prev_close = np.nan

        day_bar = super(LegacyMinuteDataSource, self).get_bar(instrument, dt.date(), "1d")
        limit_up = float(day_bar["limit_up"]) if day_bar is not None and "limit_up" in day_bar.dtype.names else np.nan
        limit_down = float(day_bar["limit_down"]) if day_bar is not None and "limit_down" in day_bar.dtype.names else np.nan

        d = {
            "datetime": dt,
            "open": float(intraday[0]["open"]),
            "high": float(intraday["high"].max()),
            "low": float(intraday["low"].min()),
            "last": float(intraday[-1]["close"]),
            "volume": float(intraday["volume"].sum()),
            "total_turnover": float(intraday["total_turnover"].sum()),
            "prev_close": prev_close,
            "limit_up": limit_up,
            "limit_down": limit_down,
        }
        return TickObject(instrument, d)

    def get_trading_minutes_for(self, instrument, trading_dt):
        bars = self._get_minute_bars(instrument)
        if len(bars) == 0:
            return []

        trade_date = trading_dt.date() if isinstance(trading_dt, datetime) else trading_dt
        start_int = np.uint64(convert_dt_to_int(datetime.combine(trade_date, time.min)))
        end_int = np.uint64(convert_dt_to_int(datetime.combine(trade_date, time.max)))
        left = bars["datetime"].searchsorted(start_int, side="left")
        right = bars["datetime"].searchsorted(end_int, side="right")
        return list(bars["datetime"][left:right])

    def available_data_range(self, frequency):
        if frequency != "1m":
            return super(LegacyMinuteDataSource, self).available_data_range(frequency)

        if self._minute_range_cache is not None:
            return self._minute_range_cache

        with sqlite3.connect(self._sqlite_path) as conn:
            row = conn.execute(
                "SELECT MIN(timestamp), MAX(timestamp) FROM {}".format(self._minute_table)
            ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            raise RuntimeError("No minute data found in table '{}'".format(self._minute_table))

        start = pd.Timestamp(row[0]).date()
        end = pd.Timestamp(row[1]).date()
        self._minute_range_cache = (start, end)
        return self._minute_range_cache


class LegacyMinuteDataSourceMod(AbstractMod):
    def start_up(self, env, mod_config):
        sqlite_path = getattr(mod_config, "sqlite_path", None)
        if not sqlite_path:
            raise RuntimeError(
                "mod legacy_1m_source requires sqlite_path, "
                "use -mc legacy_1m_source.sqlite_path /path/to/stock_data.db"
            )

        minute_table = getattr(mod_config, "minute_table", "stock_1_min_mock")
        env.set_data_source(
            LegacyMinuteDataSource(
                env.config.base,
                sqlite_path=sqlite_path,
                minute_table=minute_table,
            )
        )

    def tear_down(self, code, exception=None):
        pass


def load_mod():
    return LegacyMinuteDataSourceMod()
