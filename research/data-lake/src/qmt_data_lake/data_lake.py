"""Versioned, daily-bar research store backed by Parquet and DuckDB.

The QMT terminal remains the source of record.  This module deliberately keeps
the terminal's raw daily bars separate from an optional AKShare comparison so a
network source can never silently alter data used by a backtest.
"""

from __future__ import absolute_import

import datetime as dt
import hashlib
import json
import os
import re
import uuid


class DataLakeError(RuntimeError):
    pass


BAR_COLUMNS = (
    "symbol", "trade_date", "open", "high", "low", "close", "volume",
    "amount", "prev_close", "up_limit", "down_limit", "suspended",
)


def _deps():
    try:
        import duckdb
        import pandas as pd
    except ImportError as exc:
        raise DataLakeError(
            "daily data lake needs pandas, pyarrow and duckdb; install "
            "xtquant-big-convert[data]") from exc
    return pd, duckdb


def _now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _safe_part(value):
    value = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(value or "unknown"))
    return value.strip("._") or "unknown"


def _hash_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize_symbol(value):
    """Return a QMT symbol, preserving an existing exchange suffix."""
    text = str(value or "").strip().upper()
    if "." in text:
        return text
    if len(text) != 6 or not text.isdigit():
        raise DataLakeError("unsupported symbol: %r" % value)
    if text.startswith(("5", "6", "9")):
        return text + ".SH"
    if text.startswith(("8", "4")):
        return text + ".BJ"
    return text + ".SZ"


def akshare_symbol(value):
    return normalize_symbol(value).split(".", 1)[0]


def infer_asset_type(symbol):
    code = akshare_symbol(symbol)
    # ETF codes are not perfectly inferable from code alone.  The caller may
    # pass an explicit type; this conservative fallback covers common ranges.
    return "etf" if code.startswith(("5", "1")) else "stock"


def _date_series(series, pd):
    if hasattr(series, "dt"):
        numeric = pd.api.types.is_numeric_dtype(series)
    else:
        numeric = False
    if numeric:
        values = pd.to_numeric(series, errors="coerce")
        unit = "ms" if values.dropna().abs().gt(10 ** 11).any() else None
        if unit:
            return pd.to_datetime(values, unit=unit, errors="coerce").dt.normalize()
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def normalize_qmt_bars(frame, symbol=None, drop_invalid=False, return_invalid=False):
    """Normalise QMT's DataFrame/list shapes into the stable daily-bar schema.

    ``drop_invalid`` removes malformed rows only when explicitly requested.
    With ``return_invalid=True`` the caller gets ``(bars, invalid_bars)``; the
    latter retains raw normalized fields and a machine-readable reason for
    later source validation and repair.  In that mode, the first result keeps
    *all* source rows so the primary history is a full immutable ingest.
    The default remains strict for validation and callers that need an error.
    """
    pd, _ = _deps()
    if frame is None:
        empty = pd.DataFrame(columns=BAR_COLUMNS)
        return (empty, pd.DataFrame()) if return_invalid else empty
    df = frame.copy() if hasattr(frame, "copy") else pd.DataFrame(frame)
    if df.empty:
        empty = pd.DataFrame(columns=BAR_COLUMNS)
        return (empty, pd.DataFrame()) if return_invalid else empty
    if not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    aliases = {
        "symbol": ("symbol", "stock_code", "code", "stock"),
        "trade_date": ("trade_date", "date", "datetime", "time", "stime", "index"),
        "prev_close": ("prev_close", "preclose", "preClose", "lastClose"),
        "up_limit": ("up_limit", "upLimit", "upperLimit"),
        "down_limit": ("down_limit", "downLimit", "lowerLimit"),
        "suspended": ("suspended", "suspendFlag", "suspend_flag"),
    }
    lower = {str(column).lower(): column for column in df.columns}
    rename = {}
    for target, names in aliases.items():
        for name in names:
            actual = lower.get(name.lower())
            if actual is not None:
                rename[actual] = target
                break
    for field in ("open", "high", "low", "close", "volume", "amount"):
        actual = lower.get(field)
        if actual is not None:
            rename[actual] = field
    df = df.rename(columns=rename)
    if "symbol" not in df:
        if symbol is None:
            raise DataLakeError("QMT bars do not include a symbol")
        df["symbol"] = normalize_symbol(symbol)
    else:
        df["symbol"] = df["symbol"].map(normalize_symbol)
    if "trade_date" not in df:
        raise DataLakeError("QMT bars do not include a date/time column")
    df["trade_date"] = _date_series(df["trade_date"], pd)
    if df["trade_date"].isna().any():
        raise DataLakeError("QMT bars contain an unreadable date")
    for field in ("open", "high", "low", "close", "volume", "amount", "prev_close", "up_limit", "down_limit"):
        if field not in df:
            df[field] = None
        df[field] = pd.to_numeric(df[field], errors="coerce")
    if "suspended" not in df:
        df["suspended"] = False
    df["suspended"] = df["suspended"].map(
        lambda value: value is True or str(value).strip().lower() in ("1", "true", "yes", "y"))
    non_positive = df[["open", "high", "low", "close"]].isna().any(axis=1) | \
        (df[["open", "high", "low", "close"]] <= 0).any(axis=1)
    inconsistent_high = df["high"] < df[["open", "close", "low"]].max(axis=1)
    inconsistent_low = df["low"] > df[["open", "close", "high"]].min(axis=1)
    invalid = non_positive | inconsistent_high | inconsistent_low
    invalid_rows = pd.DataFrame()
    if invalid.any():
        if not drop_invalid and not return_invalid:
            if non_positive.any():
                raise DataLakeError("QMT bars contain missing/non-positive prices")
            if inconsistent_high.any():
                raise DataLakeError("QMT bars contain inconsistent high prices")
            raise DataLakeError("QMT bars contain inconsistent low prices")
        invalid_rows = df.loc[invalid, BAR_COLUMNS].copy()
        reasons = []
        for index in invalid_rows.index:
            row_reasons = []
            if bool(non_positive.loc[index]):
                row_reasons.append("missing_or_non_positive_ohlc")
            if bool(inconsistent_high.loc[index]):
                row_reasons.append("high_below_ohlc")
            if bool(inconsistent_low.loc[index]):
                row_reasons.append("low_above_ohlc")
            reasons.append("|".join(row_reasons))
        invalid_rows["quality_reason"] = reasons
        invalid_rows["quality_status"] = "pending_validation"
        if drop_invalid:
            df = df.loc[~invalid].copy()
    valid_rows = df.loc[:, BAR_COLUMNS].sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    if return_invalid:
        return valid_rows, invalid_rows.reset_index(drop=True)
    return valid_rows


def normalize_akshare_bars(frame, symbol, asset_type=None):
    pd, _ = _deps()
    if frame is None:
        return pd.DataFrame(columns=BAR_COLUMNS)
    df = frame.copy()
    mapping = {
        "日期": "trade_date", "开盘": "open", "最高": "high", "最低": "low",
        "收盘": "close", "成交量": "volume", "成交额": "amount",
    }
    df = df.rename(columns=mapping)
    df["symbol"] = normalize_symbol(symbol)
    for field in ("prev_close", "up_limit", "down_limit"):
        df[field] = None
    df["suspended"] = False
    return normalize_qmt_bars(df, symbol=symbol)


def _front_ratio(frame, pd):
    """Build point-in-time ratio-front-adjusted prices from raw preClose.

    The adjustment uses only rows at or before the requested as-of date.  On an
    ex-date the terminal's preClose is the adjusted reference price, making the
    ratio to the previous raw close sufficient for price-continuity indicators.
    """
    out = frame.copy().sort_values(["symbol", "trade_date"])
    for symbol, indexes in out.groupby("symbol", sort=False).groups.items():
        rows = out.loc[indexes].sort_values("trade_date")
        multiplier = 1.0
        factors = []
        values = list(rows.itertuples())
        for position in range(len(values) - 1, -1, -1):
            factors.append(multiplier)
            if position > 0:
                current = values[position]
                previous = values[position - 1]
                pre_close = getattr(current, "prev_close")
                prior_close = getattr(previous, "close")
                if pd.notna(pre_close) and prior_close and pre_close > 0:
                    multiplier *= float(pre_close) / float(prior_close)
        out.loc[rows.index, "adjust_factor"] = list(reversed(factors))
    for field in ("open", "high", "low", "close", "prev_close", "up_limit", "down_limit"):
        out[field] = out[field] * out["adjust_factor"]
    return out


class DataLake(object):
    """Small, versioned data lake intended for external Python research jobs."""

    def __init__(self, root):
        self.root = os.path.abspath(os.path.expanduser(root))
        self.lake_root = os.path.join(self.root, "lake")
        self.db_path = os.path.join(self.root, "catalog.duckdb")

    def initialize(self):
        _, duckdb = _deps()
        for path in (self.lake_root, os.path.join(self.root, "manifests"),
                     os.path.join(self.root, "exports"), os.path.join(self.root, "logs"),
                     os.path.join(self.root, "quarantine")):
            if not os.path.isdir(path):
                os.makedirs(path)
        con = duckdb.connect(self.db_path)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS partitions (
                    dataset VARCHAR, asset_type VARCHAR, trade_year INTEGER,
                    path VARCHAR PRIMARY KEY, row_count BIGINT, min_date DATE,
                    max_date DATE, content_hash VARCHAR, source VARCHAR,
                    manifest_version VARCHAR, status VARCHAR, is_current BOOLEAN,
                    created_at TIMESTAMP
                )""")
            con.execute("""
                CREATE TABLE IF NOT EXISTS manifests (
                    version VARCHAR PRIMARY KEY, status VARCHAR, note VARCHAR,
                    created_at TIMESTAMP, committed_at TIMESTAMP
                )""")
            con.execute("""
                CREATE TABLE IF NOT EXISTS manifest_entries (
                    version VARCHAR, path VARCHAR, PRIMARY KEY(version, path)
                )""")
            con.execute("""
                CREATE TABLE IF NOT EXISTS validation_results (
                    manifest_version VARCHAR, symbol VARCHAR, trade_date DATE,
                    asset_type VARCHAR, status VARCHAR, reason VARCHAR,
                    qmt_json VARCHAR, akshare_json VARCHAR, created_at TIMESTAMP
                )""")
            con.execute("""
                CREATE TABLE IF NOT EXISTS quarantines (
                    symbol VARCHAR, trade_date DATE, manifest_version VARCHAR,
                    reason VARCHAR, status VARCHAR, created_at TIMESTAMP,
                    resolved_at TIMESTAMP
                )""")
        finally:
            con.close()
        return self

    def _connect(self):
        _, duckdb = _deps()
        self.initialize()
        return duckdb.connect(self.db_path)

    def begin_manifest(self, note=""):
        self.initialize()
        version = "v%s-%s" % (dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S"), uuid.uuid4().hex[:8])
        con = self._connect()
        try:
            con.execute("INSERT INTO manifests VALUES (?, 'staged', ?, ?, NULL)", [version, note, _now()])
        finally:
            con.close()
        return version

    def _partition_path(self, dataset, asset_type, year, version):
        return os.path.join(
            self.lake_root, _safe_part(dataset), "asset_type=" + _safe_part(asset_type),
            "year=" + str(int(year)), "version=" + _safe_part(version), "data.parquet")

    def _current_frame(self, con, dataset, asset_type, year):
        pd, _ = _deps()
        rows = con.execute(
            "SELECT path FROM partitions WHERE dataset=? AND asset_type=? AND trade_year=? "
            "AND is_current=true AND status='approved'", [dataset, asset_type, int(year)]).fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(row[0]) for row in rows], ignore_index=True)

    def stage_bars(self, frame, asset_type, manifest_version, source="qmt", allow_invalid=False):
        pd, _ = _deps()
        bars = normalize_qmt_bars(frame, return_invalid=True)[0] if allow_invalid else normalize_qmt_bars(frame)
        if bars.empty:
            return []
        bars["asset_type"] = asset_type
        bars["source"] = source
        bars["ingested_at"] = _now()
        bars["trade_year"] = bars["trade_date"].dt.year
        paths = []
        con = self._connect()
        try:
            for year, incoming in bars.groupby("trade_year"):
                staged = con.execute(
                    "SELECT path FROM partitions WHERE dataset='bars_raw' AND asset_type=? AND trade_year=? "
                    "AND manifest_version=? AND status='staged'", [asset_type, int(year), manifest_version]).fetchall()
                existing = self._current_frame(con, "bars_raw", asset_type, year)
                if staged:
                    existing = pd.concat([existing] + [pd.read_parquet(row[0]) for row in staged], ignore_index=True)
                combined = pd.concat([existing, incoming], ignore_index=True)
                combined = combined.sort_values("ingested_at").drop_duplicates(
                    ["symbol", "trade_date"], keep="last").sort_values(["symbol", "trade_date"])
                path = self._partition_path("bars_raw", asset_type, year, manifest_version)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                temp = path + ".tmp"
                combined.drop(columns=["trade_year"], errors="ignore").to_parquet(temp, index=False)
                os.replace(temp, path)
                if staged:
                    con.execute("DELETE FROM partitions WHERE path IN (%s)" % ",".join("?" * len(staged)), [r[0] for r in staged])
                con.execute(
                    "INSERT INTO partitions (dataset, asset_type, trade_year, path, row_count, min_date, max_date, "
                    "content_hash, source, manifest_version, status, is_current, created_at) "
                    "VALUES ('bars_raw', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, ?)",
                    [asset_type, int(year), path, len(combined), combined.trade_date.min().date(),
                     combined.trade_date.max().date(), _hash_file(path), source, manifest_version, "staged", _now()])
                paths.append(path)
        finally:
            con.close()
        return paths

    def stage_records(self, dataset, frame, manifest_version, asset_type="reference", source="qmt"):
        pd, _ = _deps()
        data = frame.copy() if hasattr(frame, "copy") else pd.DataFrame(frame)
        if data.empty:
            return []
        if "trade_date" not in data:
            data["trade_date"] = pd.Timestamp.utcnow().normalize()
        data["trade_date"] = _date_series(data["trade_date"], pd)
        data["source"] = source
        data["ingested_at"] = _now()
        paths = []
        con = self._connect()
        try:
            for year, part in data.groupby(data.trade_date.dt.year):
                staged = con.execute(
                    "SELECT path FROM partitions WHERE dataset=? AND asset_type=? AND trade_year=? "
                    "AND manifest_version=? AND status='staged'",
                    [dataset, asset_type, int(year), manifest_version]).fetchall()
                existing = self._current_frame(con, dataset, asset_type, year)
                if staged:
                    existing = pd.concat([existing] + [pd.read_parquet(row[0]) for row in staged], ignore_index=True)
                combined = pd.concat([existing, part], ignore_index=True).drop_duplicates().reset_index(drop=True)
                path = self._partition_path(dataset, asset_type, year, manifest_version)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                temp = path + ".tmp"
                combined.to_parquet(temp, index=False)
                os.replace(temp, path)
                if staged:
                    con.execute("DELETE FROM partitions WHERE path IN (%s)" % ",".join("?" * len(staged)), [r[0] for r in staged])
                con.execute(
                    "INSERT INTO partitions (dataset, asset_type, trade_year, path, row_count, min_date, max_date, "
                    "content_hash, source, manifest_version, status, is_current, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, ?)",
                    [dataset, asset_type, int(year), path, len(combined), combined.trade_date.min().date(),
                     combined.trade_date.max().date(), _hash_file(path), source, manifest_version, "staged", _now()])
                paths.append(path)
        finally:
            con.close()
        return paths

    def commit(self, manifest_version):
        con = self._connect()
        try:
            staged = con.execute(
                "SELECT dataset, asset_type, trade_year FROM partitions WHERE manifest_version=? AND status='staged'",
                [manifest_version]).fetchall()
            if not staged:
                raise DataLakeError("manifest has no staged partitions: %s" % manifest_version)
            con.execute("BEGIN")
            for dataset, asset_type, year in staged:
                con.execute(
                    "UPDATE partitions SET is_current=false WHERE dataset=? AND asset_type=? AND trade_year=? "
                    "AND is_current=true", [dataset, asset_type, year])
            con.execute("UPDATE partitions SET status='approved', is_current=true WHERE manifest_version=? AND status='staged'", [manifest_version])
            con.execute("DELETE FROM manifest_entries WHERE version=?", [manifest_version])
            con.execute("INSERT INTO manifest_entries SELECT ?, path FROM partitions WHERE is_current=true AND status='approved'", [manifest_version])
            con.execute("UPDATE manifests SET status='committed', committed_at=? WHERE version=?", [_now(), manifest_version])
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            con.close()
        return manifest_version

    def _paths(self, con, dataset, manifest_version=None):
        if manifest_version:
            rows = con.execute(
                "SELECT p.path FROM partitions p JOIN manifest_entries e ON p.path=e.path "
                "WHERE e.version=? AND p.dataset=?", [manifest_version, dataset]).fetchall()
        else:
            rows = con.execute("SELECT path FROM partitions WHERE dataset=? AND is_current=true AND status='approved'", [dataset]).fetchall()
        return [row[0] for row in rows]

    def get_bars(self, symbols=None, start=None, end=None, price_basis="raw", asof=None,
                 quality="approved", manifest_version=None):
        pd, _ = _deps()
        con = self._connect()
        try:
            paths = self._paths(con, "bars_raw", manifest_version)
            quarantines = con.execute("SELECT symbol, trade_date FROM quarantines WHERE status='open'").fetchall()
        finally:
            con.close()
        if not paths:
            return pd.DataFrame(columns=BAR_COLUMNS)
        frame = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
        frame["trade_date"] = _date_series(frame["trade_date"], pd)
        if symbols:
            wanted = set(normalize_symbol(item) for item in symbols)
            frame = frame[frame.symbol.isin(wanted)]
        if start:
            frame = frame[frame.trade_date >= pd.Timestamp(start).normalize()]
        if end:
            frame = frame[frame.trade_date <= pd.Timestamp(end).normalize()]
        if asof:
            frame = frame[frame.trade_date <= pd.Timestamp(asof).normalize()]
        if quality == "approved" and quarantines:
            blocked = {(symbol, pd.Timestamp(day).normalize()) for symbol, day in quarantines}
            frame = frame[~frame.apply(lambda row: (row.symbol, row.trade_date) in blocked, axis=1)]
        frame = frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        if price_basis == "raw":
            return frame
        if price_basis == "front_ratio":
            return _front_ratio(frame, pd)
        raise DataLakeError("unknown price_basis: %s" % price_basis)

    def get_records(self, dataset, asof=None, manifest_version=None):
        """Read a slow-frequency dataset using the same immutable manifest model."""
        pd, _ = _deps()
        con = self._connect()
        try:
            paths = self._paths(con, dataset, manifest_version)
        finally:
            con.close()
        if not paths:
            return pd.DataFrame()
        frame = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
        if "trade_date" in frame:
            frame["trade_date"] = _date_series(frame["trade_date"], pd)
        if asof and "trade_date" in frame:
            frame = frame[frame.trade_date <= pd.Timestamp(asof).normalize()]
        return frame

    def get_universe(self, pool_name, asof, manifest_version=None):
        frame = self.get_records("universe", asof, manifest_version)
        if frame.empty:
            return frame
        if "pool_name" in frame:
            frame = frame[frame.pool_name == pool_name]
        return frame.sort_values("trade_date").drop_duplicates("symbol", keep="last")

    def get_trading_calendar(self, market, start=None, end=None, manifest_version=None):
        """Return the persisted exchange calendar, rather than weekday guesses."""
        pd, _ = _deps()
        frame = self.get_records("trading_calendar", manifest_version=manifest_version)
        if frame.empty:
            return frame
        frame = frame[frame["market"].astype(str).str.upper() == str(market).upper()]
        if start:
            frame = frame[frame.trade_date >= pd.Timestamp(start).normalize()]
        if end:
            frame = frame[frame.trade_date <= pd.Timestamp(end).normalize()]
        return frame.sort_values("trade_date").drop_duplicates(["market", "trade_date"], keep="last")

    def next_trading_day(self, market, after, manifest_version=None):
        """Find the next *stored* exchange session; never infer one from weekdays."""
        pd, _ = _deps()
        frame = self.get_trading_calendar(market, start=after, manifest_version=manifest_version)
        if frame.empty:
            return None
        future = frame[frame.trade_date > pd.Timestamp(after).normalize()]
        if future.empty:
            return None
        return future.iloc[0].trade_date.date()

    def get_market_sessions(self, market, asof=None, manifest_version=None):
        """Return the latest explicitly sourced session definition for a market."""
        frame = self.get_records("market_sessions", asof=asof, manifest_version=manifest_version)
        if frame.empty:
            return frame
        frame = frame[frame["market"].astype(str).str.upper() == str(market).upper()]
        keys = [field for field in ("market", "session_id") if field in frame]
        return frame.sort_values("trade_date").drop_duplicates(keys, keep="last")

    def get_trade_status(self, symbols, trade_date, manifest_version=None):
        frame = self.get_records("security_status", trade_date, manifest_version)
        if frame.empty:
            return frame
        wanted = set(normalize_symbol(item) for item in symbols)
        return frame[(frame.symbol.isin(wanted)) & (frame.trade_date == _deps()[0].Timestamp(trade_date).normalize())]

    def get_fundamentals(self, symbols, asof, manifest_version=None):
        pd, _ = _deps()
        frame = self.get_records("financials", None, manifest_version)
        if frame.empty:
            return frame
        wanted = set(normalize_symbol(item) for item in symbols)
        available = "announce_time" if "announce_time" in frame else "trade_date"
        frame[available] = _date_series(frame[available], pd)
        return frame[(frame.symbol.isin(wanted)) & (frame[available] <= pd.Timestamp(asof).normalize())]

    def record_validation(self, results, manifest_version):
        pd, _ = _deps()
        data = results.copy() if hasattr(results, "copy") else pd.DataFrame(results)
        if data.empty:
            return 0
        required = {"symbol", "trade_date", "asset_type", "status", "reason", "qmt_json", "akshare_json"}
        missing = required - set(data.columns)
        if missing:
            raise DataLakeError("validation result missing fields: %s" % sorted(missing))
        con = self._connect()
        try:
            for row in data.to_dict(orient="records"):
                values = [manifest_version, normalize_symbol(row["symbol"]), pd.Timestamp(row["trade_date"]).date(),
                          row["asset_type"], row["status"], row["reason"], row["qmt_json"], row["akshare_json"], _now()]
                con.execute("INSERT INTO validation_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
                if row["status"] == "mismatch":
                    con.execute("INSERT INTO quarantines VALUES (?, ?, ?, ?, 'open', ?, NULL)",
                                [values[1], values[2], manifest_version, row["reason"], _now()])
        finally:
            con.close()
        return len(data)

    def export_csv(self, output_path, symbols=None, start=None, end=None, manifest_version=None):
        frame = self.get_bars(symbols, start, end, manifest_version=manifest_version)
        if frame.empty:
            raise DataLakeError("no approved bars available for export")
        result = frame.rename(columns={"trade_date": "datetime"})
        result["datetime"] = result["datetime"].dt.strftime("%Y-%m-%d")
        keep = [field for field in ("datetime", "symbol", "open", "high", "low", "close", "volume", "amount", "prev_close", "up_limit", "down_limit", "suspended") if field in result]
        directory = os.path.dirname(os.path.abspath(output_path))
        if not os.path.isdir(directory):
            os.makedirs(directory)
        result.loc[:, keep].to_csv(output_path, index=False, encoding="utf-8")
        return len(result)


class AkshareValidator(object):
    """Fetch raw daily bars from AKShare and compare them without mutation."""

    def __init__(self, timeout=15):
        self.timeout = timeout

    def fetch(self, symbol, asset_type, start, end):
        try:
            import akshare as ak
        except ImportError as exc:
            raise DataLakeError("AKShare is required for cross-validation") from exc
        kwargs = dict(symbol=akshare_symbol(symbol), period="daily", start_date=str(start).replace("-", ""),
                      end_date=str(end).replace("-", ""), adjust="")
        if asset_type == "etf":
            try:
                raw = ak.fund_etf_hist_em(**kwargs)
                if raw is None or raw.empty:
                    raise DataLakeError("Eastmoney returned no ETF daily bars")
            except Exception as eastmoney_error:
                market = "sh" if normalize_symbol(symbol).endswith(".SH") else "sz"
                try:
                    raw = ak.fund_etf_hist_sina(symbol=market + akshare_symbol(symbol))
                except Exception as sina_error:
                    raise DataLakeError("AKShare Eastmoney ETF failed (%s); Sina fallback failed (%s)" %
                                        (eastmoney_error, sina_error)) from sina_error
                if raw is None or raw.empty:
                    raise DataLakeError("AKShare Eastmoney ETF failed (%s); Sina fallback returned no daily bars" %
                                        eastmoney_error)
        else:
            # Eastmoney is AKShare's normal stock-history provider.  It can
            # temporarily close connections under batch traffic, so use
            # AKShare's Tencent adapter as a same-library fallback before
            # treating the row as unavailable.
            kwargs["timeout"] = self.timeout
            try:
                raw = ak.stock_zh_a_hist(**kwargs)
                if raw is None or raw.empty:
                    raise DataLakeError("Eastmoney returned no daily bars")
            except Exception as eastmoney_error:
                market = "sh" if normalize_symbol(symbol).endswith(".SH") else "sz"
                try:
                    raw = ak.stock_zh_a_hist_tx(
                        symbol=market + akshare_symbol(symbol),
                        start_date=str(start).replace("-", ""),
                        end_date=str(end).replace("-", ""),
                        adjust="", timeout=self.timeout)
                except Exception as tencent_error:
                    raise DataLakeError("AKShare Eastmoney failed (%s); Tencent fallback failed (%s)" %
                                        (eastmoney_error, tencent_error)) from tencent_error
                if raw is None or raw.empty:
                    raise DataLakeError("AKShare Eastmoney failed (%s); Tencent fallback returned no daily bars" %
                                        eastmoney_error)
        return normalize_akshare_bars(raw, symbol, asset_type)

    @staticmethod
    def compare(qmt_frame, ak_frame, asset_type):
        pd, _ = _deps()
        qmt = normalize_qmt_bars(qmt_frame)
        ak = normalize_qmt_bars(ak_frame)
        merged = qmt.merge(ak, on=["symbol", "trade_date"], how="outer", suffixes=("_qmt", "_ak"), indicator=True)
        results = []
        for row in merged.to_dict(orient="records"):
            qmt_payload = {field: row.get(field + "_qmt") for field in ("open", "high", "low", "close", "volume", "amount")}
            ak_payload = {field: row.get(field + "_ak") for field in ("open", "high", "low", "close", "volume", "amount")}
            status, reason = "match", ""
            if row["_merge"] != "both":
                status, reason = "mismatch", "missing_bar"
            else:
                for field in ("open", "high", "low", "close"):
                    left, right = qmt_payload[field], ak_payload[field]
                    if pd.isna(left) or pd.isna(right) or (abs(left - right) > 0.001 and abs(left - right) / max(abs(left), abs(right), 1.0) > 0.0001):
                        status, reason = "mismatch", "price_%s" % field
                        break
                if status == "match":
                    for field in ("volume", "amount"):
                        left, right = qmt_payload[field], ak_payload[field]
                        if pd.isna(left) or pd.isna(right):
                            continue
                        # AKShare commonly reports volume in lots while QMT
                        # reports shares. Compare the two documented scales,
                        # while retaining the raw values for review.
                        candidates = (right, right * 100.0, right / 100.0) if field == "volume" else (right,)
                        delta = min((abs(left - item) / max(abs(left), abs(item), 1.0) for item in candidates), default=0.0)
                        if delta > 0.01:
                            status, reason = "mismatch", "%s_delta" % field
                            break
            results.append({
                "symbol": row["symbol"], "trade_date": row["trade_date"], "asset_type": asset_type,
                "status": status, "reason": reason,
                "qmt_json": json.dumps(qmt_payload, default=str, ensure_ascii=False, sort_keys=True),
                "akshare_json": json.dumps(ak_payload, default=str, ensure_ascii=False, sort_keys=True),
            })
        return pd.DataFrame(results)


def deterministic_sample(symbols, trade_date, size):
    """Stable daily cohort selection; changing process order cannot change it."""
    if size <= 0:
        return []
    seed = str(trade_date)
    ranked = sorted((hashlib.sha256((seed + normalize_symbol(code)).encode("utf-8")).hexdigest(), normalize_symbol(code)) for code in set(symbols))
    return [code for _, code in ranked[:int(size)]]
