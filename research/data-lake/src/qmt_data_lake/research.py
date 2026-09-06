"""Point-in-time research run snapshots for the local QMT data lake.

The snapshot is intentionally a small JSON artifact rather than another mutable
table: a backtest must state exactly which approved Parquet partitions, data
cutoff, quality policy and execution timing it used.
"""
from __future__ import absolute_import

import datetime as dt
import hashlib
import json
import os
import re
import uuid

from .data_lake import DataLakeError


def _date(value, label):
    try:
        return dt.datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        try:
            return dt.datetime.strptime(str(value)[:8], "%Y%m%d").date()
        except ValueError:
            raise DataLakeError("%s must be YYYY-MM-DD or YYYYMMDD" % label)


def _safe_name(value):
    text = re.sub(r"[^A-Za-z0-9_.=-]+", "_", str(value or "research"))
    return text.strip("._") or "research"


def _json_hash(value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_json(path, payload):
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def _catalog_snapshot(lake):
    con = lake._connect()
    try:
        rows = con.execute(
            "SELECT dataset, asset_type, trade_year, path, row_count, min_date, max_date, "
            "content_hash, source, manifest_version "
            "FROM partitions WHERE is_current=true AND status='approved' "
            "ORDER BY dataset, asset_type, trade_year, path"
        ).fetchall()
    finally:
        con.close()
    entries = []
    for row in rows:
        entries.append({
            "dataset": row[0], "asset_type": row[1], "trade_year": int(row[2]),
            "path": row[3], "row_count": int(row[4]), "min_date": str(row[5]),
            "max_date": str(row[6]), "content_hash": row[7], "source": row[8],
            "manifest_version": row[9],
        })
    return entries


def _quality_summary(lake, asof):
    """Report, rather than silently hide, records excluded from research."""
    import pandas as pd
    invalid = lake.get_records("invalid_bars", asof=asof)
    validation = lake.get_records("validation_results", asof=asof)
    if invalid.empty:
        return {"invalid_keys": 0, "repaired_keys": 0, "unverified_keys": 0, "unverified": []}
    invalid = invalid.copy()
    invalid["trade_date"] = pd.to_datetime(invalid["trade_date"]).dt.strftime("%Y-%m-%d")
    keys = invalid[["symbol", "trade_date"]].drop_duplicates()
    latest = validation.iloc[0:0].copy()
    if not validation.empty and {"symbol", "trade_date", "status"}.issubset(validation.columns):
        latest = validation.copy()
        latest["trade_date"] = pd.to_datetime(latest["trade_date"]).dt.strftime("%Y-%m-%d")
        order = "ingested_at" if "ingested_at" in latest.columns else "trade_date"
        latest = latest.sort_values(order).drop_duplicates(["symbol", "trade_date"], keep="last")
    merged = keys.merge(latest[[field for field in ("symbol", "trade_date", "status", "reason") if field in latest.columns]],
                        on=["symbol", "trade_date"], how="left")
    statuses = merged["status"] if "status" in merged.columns else pd.Series("", index=merged.index)
    unverified = merged[statuses.ne("repaired")]
    return {
        "invalid_keys": int(len(keys)),
        "repaired_keys": int((statuses == "repaired").sum()),
        "unverified_keys": int(len(unverified)),
        "unverified": [
            {"symbol": item["symbol"], "trade_date": item["trade_date"],
             "reason": item.get("reason") or "not_validated"}
            for item in unverified.sort_values(["symbol", "trade_date"]).to_dict(orient="records")
        ],
    }


def create_research_snapshot(lake, strategy, asof, execution_date, parameters=None, code_version="", note=""):
    """Create a reproducible, no-look-ahead research run declaration.

    ``execution_date`` is deliberately explicit.  It prevents an accidental
    assumption that a signal using a day's close can trade on that same close.
    When the exchange calendar is available it must be its next stored session.
    """
    asof_day, execution_day = _date(asof, "asof"), _date(execution_date, "execution_date")
    if execution_day <= asof_day:
        raise DataLakeError("execution_date must be after asof; close-based signals cannot trade on the same day")
    catalog = _catalog_snapshot(lake)
    bars = [row for row in catalog if row["dataset"] == "bars_raw"]
    if not bars:
        raise DataLakeError("cannot create research snapshot without approved bars_raw partitions")
    latest_bar = max(_date(row["max_date"], "catalog max_date") for row in bars)
    if asof_day > latest_bar:
        raise DataLakeError("asof %s is later than approved bars %s" % (asof_day.isoformat(), latest_bar.isoformat()))
    next_day = lake.next_trading_day("SH", asof_day.isoformat())
    if next_day and execution_day != next_day:
        raise DataLakeError("execution_date %s must equal next stored SH trading day %s" %
                            (execution_day.isoformat(), next_day.isoformat()))
    quality = _quality_summary(lake, asof_day.isoformat())
    run_id = "r%s-%s-%s" % (dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S"),
                              _safe_name(strategy), uuid.uuid4().hex[:8])
    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "created_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "strategy": str(strategy),
        "code_version": str(code_version or "unrecorded"),
        "parameters": parameters or {},
        "note": str(note or ""),
        "asof": asof_day.isoformat(),
        "execution_date": execution_day.isoformat(),
        "timing_policy": "signal_after_close_execute_next_trading_day",
        "calendar_validation": {
            "market": "SH",
            "next_stored_trading_day": next_day.isoformat() if next_day else None,
            "status": "validated" if next_day else "calendar_not_yet_available",
        },
        "price_policy": {
            "execution": "raw",
            "research_adjustment": "front_ratio_asof_only",
        },
        "quality_policy": "exclude_unverified_invalid_bars",
        "quality": quality,
        "catalog": catalog,
    }
    payload["catalog_fingerprint"] = _json_hash(catalog)
    payload["snapshot_fingerprint"] = _json_hash(payload)
    path = os.path.join(lake.root, "research", "runs", run_id + ".json")
    _atomic_json(path, payload)
    return payload, path
