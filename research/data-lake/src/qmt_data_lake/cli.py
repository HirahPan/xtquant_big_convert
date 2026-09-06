"""CLI for exporting QMT daily bars to the versioned local research store."""
from __future__ import print_function

import argparse
import datetime as dt
import json
import os
import sys

from .data_lake import AkshareValidator, DataLake, DataLakeError, deterministic_sample, infer_asset_type, normalize_qmt_bars
from .research import create_research_snapshot


DEFAULT_SECTORS = ("沪深A股", "沪深ETF")
BAR_FIELDS = ["open", "high", "low", "close", "volume", "amount", "preClose", "upLimit", "downLimit", "suspendFlag", "stime"]


def _ensure_qmt_python_on_path():
    """Discover the private QMT-side bridge config without shadowing this package."""
    candidate = os.environ.get("BIGQMT_QMT_PYTHON_DIR")
    if not candidate:
        return
    config_path = os.path.join(candidate, "bigqmt_signal_trader_local_config.py")
    if os.path.isfile(config_path) and candidate not in sys.path:
        sys.path.append(candidate)


def _qmt_source():
    _ensure_qmt_python_on_path()
    # The vendor project is a runtime bridge only; this local data-lake package
    # intentionally owns all export, checkpoint and warehouse code itself.
    from bigqmt_signal_trader.xtquant_compat import configure, xtdata
    configure()
    return xtdata


def _symbols(args, source):
    if args.symbols_file:
        with open(args.symbols_file, "r", encoding="utf-8") as handle:
            return [line.strip() for line in handle if line.strip() and not line.startswith("#")]
    symbols = []
    for sector in DEFAULT_SECTORS:
        try:
            symbols.extend(source.get_stock_list_in_sector(sector) or [])
        except Exception as exc:
            raise DataLakeError("cannot read QMT sector %s: %s" % (sector, exc))
    return sorted(set(symbols))


def _date_text(value):
    """Return a QMT date as YYYYMMDD, or an empty string when it is unusable."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    digits = "".join(char for char in text if char.isdigit())
    # Direct xtdata returns Unix milliseconds (the CLI renders the same value
    # as YYYYMMDD). Prefer that interpretation before taking the first 8
    # digits, otherwise 661536000000 becomes the nonsensical date 6615-36-00.
    if len(digits) in (12, 13) and digits.isdigit():
        try:
            shanghai = dt.timezone(dt.timedelta(hours=8))
            return dt.datetime.fromtimestamp(int(digits) / 1000.0, tz=shanghai).strftime("%Y%m%d")
        except (OverflowError, OSError, ValueError):
            return ""
    if len(digits) >= 8:
        candidate = digits[:8]
        try:
            dt.datetime.strptime(candidate, "%Y%m%d")
            return candidate
        except ValueError:
            return ""
    return ""


def _listing_start(source, code, floor):
    """Resolve a code's listing date instead of requesting a pre-listing range."""
    value = None
    try:
        value = source.get_open_date(code)
    except Exception:
        # Older QMT builds expose the same field only through contract details.
        try:
            detail = source.get_instrument_detail(code) or {}
            value = detail.get("OpenDate") or detail.get("openDate") or detail.get("listDate")
        except Exception:
            value = None
    listing_date = _date_text(value)
    if not listing_date:
        raise DataLakeError("cannot resolve listing date for %s; refusing a broad fallback query" % code)
    return max(listing_date, _date_text(floor))


def _checkpoint_path(root, incremental):
    return os.path.join(root, "logs", "daily_%s.checkpoint.json" % ("sync" if incremental else "bootstrap"))


def _load_checkpoint(path):
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise DataLakeError("cannot read export checkpoint %s: %s" % (path, exc))


def _write_checkpoint(path, state):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def _progress(root, mode, event, **fields):
    record = {"at": dt.datetime.now(dt.timezone.utc).isoformat(), "mode": mode, "event": event}
    record.update(fields)
    line = json.dumps(record, ensure_ascii=False, sort_keys=True)
    log_path = os.path.join(root, "logs", "daily_%s.progress.jsonl" % mode)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()
    print(line, flush=True)


def _fetch_qmt_bars(source, code, start, end):
    """Fetch one code from its true listing date; QMT accepts one start per call."""
    kwargs = dict(field_list=BAR_FIELDS, stock_list=[code], period="1d", start_time=start,
                  end_time=end, count=-1, dividend_type="none", fill_data=False)
    try:
        raw = source.get_market_data_ex(use_formula=False, backfill_pre_close=False, **kwargs)
    except TypeError:
        raw = source.get_market_data_ex(**kwargs)
    raw = raw or {}
    frame = raw.get(code)
    if frame is None:
        # Some QMT versions normalise the dictionary key casing.
        frame = raw.get(str(code).upper())
    if frame is None:
        frame = raw.get(str(code).lower())
    if frame is None:
        return None, 0
    # The primary lake preserves every provider row.  Invalid OHLC rows are
    # also extracted into invalid_bars so repair can later supersede them.
    normalized, invalid = normalize_qmt_bars(frame, code, return_invalid=True)
    return normalized, invalid


def _stage(args, incremental=False):
    lake = DataLake(args.root).initialize()
    source = _qmt_source()
    mode = "sync" if incremental else "bootstrap"
    if incremental and not args.start:
        dates = source.get_trading_dates(market="SH", count=1) or []
        if not dates:
            raise DataLakeError("QMT did not return a latest Shanghai trading date")
        args.start = args.end = str(dates[-1])
    symbols = _symbols(args, source)
    if not symbols:
        raise DataLakeError("QMT returned an empty universe")
    checkpoint_path = _checkpoint_path(args.root, incremental)
    if getattr(args, "reset_checkpoint", False) and os.path.isfile(checkpoint_path):
        os.remove(checkpoint_path)
        _progress(args.root, mode, "checkpoint_discarded", checkpoint=checkpoint_path)
    state = _load_checkpoint(checkpoint_path)
    floor = _date_text(args.start)
    end = _date_text(args.end)
    if not floor or not end:
        raise DataLakeError("start and end must be valid YYYYMMDD dates")
    if state:
        if (state.get("mode") != mode or state.get("floor") != floor or state.get("end") != end or
                state.get("symbols") != symbols):
            raise DataLakeError("existing %s checkpoint has a different scope: %s" % (mode, checkpoint_path))
        _progress(args.root, mode, "resume", next_offset=state.get("next_offset", 0), symbols=len(symbols))
    else:
        state = {"mode": mode, "floor": floor, "end": end, "symbols": symbols,
                 "listing_starts": {}, "failed_symbols": {}, "next_offset": 0,
                 "phase": "resolve_listing_dates"}
        _write_checkpoint(checkpoint_path, state)
        _progress(args.root, mode, "started", symbols=len(symbols), floor=floor, end=end,
                  checkpoint=checkpoint_path)

    # The first full pass must use each instrument's actual listing date.  We
    # checkpoint this discovery too, so a terminal restart does not repeat it.
    if not incremental:
        missing = [code for code in symbols if code not in state["listing_starts"]]
        for count, code in enumerate(missing, 1):
            state["listing_starts"][code] = _listing_start(source, code, floor)
            if count % 50 == 0 or count == len(missing):
                _write_checkpoint(checkpoint_path, state)
                _progress(args.root, mode, "listing_dates_resolved",
                          resolved=len(state["listing_starts"]), symbols=len(symbols))
    else:
        state["listing_starts"] = dict((code, floor) for code in symbols)
    if state.get("next_offset", 0) < len(symbols):
        state["phase"] = "export"
    _write_checkpoint(checkpoint_path, state)

    import pandas as pd
    for offset in range(int(state.get("next_offset", 0)), len(symbols), args.chunk_size):
        codes = symbols[offset:offset + args.chunk_size]
        grouped = {"stock": [], "etf": []}
        invalid_bars = []
        _progress(args.root, mode, "chunk_started", offset=offset, count=len(codes), symbols=len(symbols))
        for code in codes:
            try:
                frame, invalid = _fetch_qmt_bars(source, code, state["listing_starts"][code], end)
            except DataLakeError as exc:
                # A malformed local cache entry must not discard an otherwise
                # complete batch.  Preserve it for later repair and continue.
                state.setdefault("failed_symbols", {})[code] = str(exc)
                _write_checkpoint(checkpoint_path, state)
                _progress(args.root, mode, "symbol_skipped", symbol=code, error=str(exc))
                continue
            if invalid is not None and not invalid.empty:
                invalid["asset_type"] = infer_asset_type(code)
                invalid["source"] = "qmt"
                invalid_bars.append(invalid)
                state["invalid_bars"] = int(state.get("invalid_bars", 0)) + len(invalid)
                _write_checkpoint(checkpoint_path, state)
                _progress(args.root, mode, "invalid_bars_marked", symbol=code, rows=len(invalid))
            if frame is not None and not frame.empty:
                grouped[infer_asset_type(code)].append(frame)
        version = None
        rows = 0
        for asset_type, frames in grouped.items():
            if not frames:
                continue
            if version is None:
                version = lake.begin_manifest("daily QMT %s export codes %s-%s" %
                                              (mode, offset + 1, offset + len(codes)))
            combined = pd.concat(frames, ignore_index=True)
            rows += len(combined)
            lake.stage_bars(combined, asset_type, version, allow_invalid=True)
            status = combined[[field for field in ("symbol", "trade_date", "suspended", "up_limit", "down_limit")
                               if field in combined]].copy()
            status["asset_type"] = asset_type
            lake.stage_records("security_status", status, version, source="qmt")
        if invalid_bars:
            if version is None:
                version = lake.begin_manifest("daily QMT %s invalid-bar quarantine codes %s-%s" %
                                              (mode, offset + 1, offset + len(codes)))
            lake.stage_records("invalid_bars", pd.concat(invalid_bars, ignore_index=True), version, source="qmt")
        if version:
            lake.commit(version)
        state["next_offset"] = offset + len(codes)
        _write_checkpoint(checkpoint_path, state)
        _progress(args.root, mode, "chunk_committed", offset=offset, next_offset=state["next_offset"],
                  rows=rows, manifest_version=version)
    # This is a dated, auditable current-universe snapshot.  Historical sector
    # membership can be supplied later through stage_records without changing
    # the raw bar source of truth.
    if state.get("phase") != "universe_done":
        version = lake.begin_manifest("daily QMT %s universe snapshot" % mode)
        snapshot = pd.DataFrame([
            {"symbol": code, "pool_name": "沪深ETF" if infer_asset_type(code) == "etf" else "沪深A股",
             "asset_type": infer_asset_type(code), "trade_date": end}
            for code in symbols
        ])
        lake.stage_records("universe", snapshot, version, source="qmt")
        lake.commit(version)
        state["phase"] = "universe_done"
        _write_checkpoint(checkpoint_path, state)
        _progress(args.root, mode, "universe_committed", manifest_version=version, symbols=len(symbols))
    os.remove(checkpoint_path)
    _progress(args.root, mode, "completed", symbols=len(symbols), end=end)
    print(json.dumps({"ok": True, "symbols": len(symbols), "skipped": len(state.get("failed_symbols", {})),
                      "invalid_bars": int(state.get("invalid_bars", 0)),
                      "progress_log": os.path.join(args.root, "logs")}, ensure_ascii=False))


def cmd_bootstrap(args):
    _stage(args, incremental=False)


def cmd_sync(args):
    _stage(args, incremental=True)


def _session_rows(market, snapshot_date, source):
    """Normalize QMT sessions, with an auditable daily-research fallback.

    Some iQuant builds expose the calendar but return an empty get_trade_times
    result. The fallback is deliberately tagged as configured, not QMT.
    """
    rows = []
    for index, item in enumerate(source.get_trade_times(market) or []):
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            rows.append({"market": market, "session_id": "qmt_%02d" % index,
                         "begin_time": str(item[0]), "end_time": str(item[1]),
                         "session_type": str(item[2]) if len(item) > 2 else "qmt",
                         "session_source": "qmt", "trade_date": snapshot_date})
    if rows:
        return rows
    return [
        {"market": market, "session_id": "continuous_morning", "begin_time": "09:30:00",
         "end_time": "11:30:00", "session_type": "continuous", "session_source": "configured_cn_equity_v1",
         "trade_date": snapshot_date},
        {"market": market, "session_id": "continuous_afternoon", "begin_time": "13:00:00",
         "end_time": "15:00:00", "session_type": "continuous", "session_source": "configured_cn_equity_v1",
         "trade_date": snapshot_date},
    ]


def cmd_sync_calendar(args):
    """Persist QMT trading dates and explicit session provenance for backtests."""
    lake = DataLake(args.root).initialize()
    source = _qmt_source()
    start, end = _date_text(args.start), _date_text(args.end)
    if not start and not end:
        latest = source.get_trading_dates(market="SH", count=1) or []
        start = end = _date_text(latest[-1]) if latest else ""
    if not start or not end or start > end:
        raise DataLakeError("sync-calendar requires a valid start/end date range")
    markets = [value.strip().upper() for value in args.markets.split(",") if value.strip()]
    if not markets:
        raise DataLakeError("sync-calendar requires at least one market")
    version = lake.begin_manifest("QMT trading calendar and market session snapshot")
    total_dates, session_sources = 0, {}
    import pandas as pd
    for market in markets:
        dates = source.get_trading_dates(market=market, start_time=start, end_time=end, count=-1) or []
        dates = sorted(set(filter(None, (_date_text(value) for value in dates))))
        if not dates:
            raise DataLakeError("QMT returned no trading dates for %s in %s-%s" % (market, start, end))
        rows = [{"market": market, "trade_date": value, "is_trading_day": True,
                 "calendar_source": "qmt"} for value in dates]
        lake.stage_records("trading_calendar", pd.DataFrame(rows), version, source="qmt")
        sessions = _session_rows(market, end, source)
        for session in sessions:
            session_sources[session["session_source"]] = session_sources.get(session["session_source"], 0) + 1
        lake.stage_records("market_sessions", pd.DataFrame(sessions), version,
                           source="qmt" if sessions[0]["session_source"] == "qmt" else "configured")
        total_dates += len(dates)
    lake.commit(version)
    # A prior parser interpreted QMT epoch milliseconds as calendar years.
    # Retire only those impossible, already-audited reference partitions; do
    # not delete files or alter any raw market data.
    con = lake._connect()
    try:
        retired = con.execute(
            "UPDATE partitions SET is_current=false WHERE dataset='trading_calendar' AND is_current=true "
            "AND (min_date < DATE '1990-01-01' OR max_date > DATE '2100-12-31')"
        ).fetchone()[0]
    finally:
        con.close()
    print(json.dumps({"ok": True, "markets": markets, "trading_dates": total_dates,
                      "session_sources": session_sources, "retired_invalid_calendar_partitions": retired,
                      "manifest_version": version}, ensure_ascii=False))


def cmd_validate_akshare(args):
    lake = DataLake(args.root).initialize()
    start = args.start or args.date
    end = args.end or args.date
    if not start or not end:
        con = lake._connect()
        try:
            row = con.execute(
                "SELECT max(max_date) FROM partitions "
                "WHERE dataset='bars_raw' AND is_current=true AND status='approved'"
            ).fetchone()
        finally:
            con.close()
        if not row or not row[0]:
            raise DataLakeError("validate-akshare requires --date or both --start and --end when the warehouse is empty")
        start = end = str(row[0])
    bars = lake.get_bars(start=start, end=end, quality="all")
    if bars.empty:
        raise DataLakeError("no local bars found for %s to %s" % (start, end))
    version = args.manifest_version or lake.begin_manifest("AKShare validation only")
    validator = AkshareValidator(timeout=args.timeout)
    results = []
    for asset_type, size in (("stock", args.stock_sample), ("etf", args.etf_sample)):
        symbols = bars[bars.asset_type == asset_type].symbol.unique().tolist() if "asset_type" in bars else \
            bars[bars.symbol.map(infer_asset_type) == asset_type].symbol.unique().tolist()
        for symbol in deterministic_sample(symbols, end, size):
            qmt = bars[bars.symbol == symbol]
            try:
                ak = validator.fetch(symbol, asset_type, start, end)
                results.append(validator.compare(qmt, ak, asset_type))
            except Exception as exc:
                import pandas as pd
                results.append(pd.DataFrame([{
                    "symbol": symbol, "trade_date": end, "asset_type": asset_type,
                    "status": "unverified", "reason": "akshare_error:%s" % exc,
                    "qmt_json": "{}", "akshare_json": "{}",
                }]))
    import pandas as pd
    result = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    count = lake.record_validation(result, version) if not result.empty else 0
    if not args.manifest_version:
        # Validation-only runs have no partitions and should not become a data manifest.
        con = lake._connect()
        try:
            con.execute("UPDATE manifests SET status='validated', committed_at=? WHERE version=?", [dt.datetime.now(dt.timezone.utc), version])
        finally:
            con.close()
    print(json.dumps({"ok": True, "checked": count, "mismatches": int((result.status == 'mismatch').sum()) if not result.empty else 0,
                      "unverified": int((result.status == 'unverified').sum()) if not result.empty else 0}, ensure_ascii=False))


def cmd_export_csv(args):
    lake = DataLake(args.root).initialize()
    symbols = None
    if args.symbols_file:
        with open(args.symbols_file, "r", encoding="utf-8") as handle:
            symbols = [line.strip() for line in handle if line.strip() and not line.startswith("#")]
    count = lake.export_csv(args.output, symbols=symbols, start=args.start, end=args.end,
                            manifest_version=args.manifest_version)
    print(json.dumps({"ok": True, "rows": count, "output": os.path.abspath(args.output)}, ensure_ascii=False))


def cmd_research_snapshot(args):
    """Freeze a point-in-time data and timing contract before research runs."""
    try:
        parameters = json.loads(args.parameters_json) if args.parameters_json else {}
    except ValueError as exc:
        raise DataLakeError("parameters-json must be a JSON object: %s" % exc)
    if not isinstance(parameters, dict):
        raise DataLakeError("parameters-json must decode to a JSON object")
    lake = DataLake(args.root).initialize()
    snapshot, path = create_research_snapshot(
        lake, strategy=args.strategy, asof=args.asof, execution_date=args.execution_date,
        parameters=parameters, code_version=args.code_version, note=args.note,
    )
    print(json.dumps({
        "ok": True, "run_id": snapshot["run_id"], "path": path,
        "catalog_fingerprint": snapshot["catalog_fingerprint"],
        "unverified_keys": snapshot["quality"]["unverified_keys"],
    }, ensure_ascii=False))


def _repair_anchors(primary_bars, invalid_keys, row, pd):
    """Return the valid QMT bars immediately surrounding an invalid target."""
    symbol = row["symbol"]
    target = pd.Timestamp(row["trade_date"]).normalize()
    bars = primary_bars[primary_bars["symbol"].eq(symbol)].copy()
    bars["trade_date"] = pd.to_datetime(bars["trade_date"]).dt.normalize()
    bars = bars.drop_duplicates("trade_date", keep="last").sort_values("trade_date").reset_index(drop=True)
    locations = bars.index[bars["trade_date"].eq(target)].tolist()
    if len(locations) != 1 or locations[0] == 0 or locations[0] == len(bars) - 1:
        raise DataLakeError("adjacent QMT trading-day anchors are unavailable")
    anchors = bars.iloc[[locations[0] - 1, locations[0] + 1]].copy()
    for anchor in anchors.to_dict(orient="records"):
        if (symbol, pd.Timestamp(anchor["trade_date"]).normalize()) in invalid_keys:
            raise DataLakeError("adjacent QMT anchor is itself marked invalid")
    return anchors


def _anchor_validation(validator, anchors, ak_bars, asset_type, pd):
    """Require both adjacent bars to match before trusting an AKShare repair."""
    anchor_dates = set(pd.to_datetime(anchors["trade_date"]).dt.normalize())
    ak_anchors = ak_bars[ak_bars["trade_date"].isin(anchor_dates)]
    compared = validator.compare(anchors, ak_anchors, asset_type)
    matches = compared[(compared["trade_date"].isin(anchor_dates)) & (compared["status"] == "match")]
    if matches["trade_date"].nunique() == 2:
        return True, "adjacent_bars_match", compared
    failures = compared[compared["status"] != "match"]
    detail = ",".join(sorted(set(failures["reason"].fillna("missing_anchor")))) or "missing_anchor"
    return False, "adjacent_bars_not_identical:%s" % detail, compared


def cmd_repair_akshare(args):
    import pandas as pd
    lake = DataLake(args.root).initialize()
    pending = lake.get_records("invalid_bars")
    if pending.empty:
        print(json.dumps({"ok": True, "repaired": 0, "pending": 0}, ensure_ascii=False)); return
    # Earlier versions could complete a bar replacement without writing a
    # partitioned validation record.  The primary source remains authoritative
    # for those rows and prevents an unnecessary second remote fetch.
    current_bars = lake.get_records("bars_raw")
    repaired_keys = current_bars[current_bars.get("source", "").eq("akshare_repair")][["symbol", "trade_date"]].copy()
    repaired_keys["trade_date"] = pd.to_datetime(repaired_keys["trade_date"]).dt.normalize()
    invalid_keys = set((item["symbol"], pd.Timestamp(item["trade_date"]).normalize())
                       for item in pending[["symbol", "trade_date"]].drop_duplicates().to_dict(orient="records"))
    prior = lake.get_records("validation_results")
    if not prior.empty and {"symbol", "trade_date", "status"}.issubset(prior.columns):
        # The current primary source decides whether a historical repair remains
        # applied.  For unrepaired rows, only the latest unavailable outcome is
        # skipped by default; --retry-unverified deliberately retries it.
        prior = prior.copy()
        if "ingested_at" in prior:
            prior = prior.sort_values("ingested_at").drop_duplicates(["symbol", "trade_date"], keep="last")
        attempted = prior.iloc[0:0][["symbol", "trade_date"]].copy() if args.retry_unverified else prior[
            prior["status"] == "unverified"][["symbol", "trade_date"]].copy()
        attempted["trade_date"] = pd.to_datetime(attempted["trade_date"]).dt.normalize()
        pending = pending.copy()
        pending["trade_date"] = pd.to_datetime(pending["trade_date"]).dt.normalize()
        attempted = pd.concat([attempted, repaired_keys], ignore_index=True).drop_duplicates()
        pending = pending.merge(attempted, on=["symbol", "trade_date"], how="left", indicator=True)
        pending = pending[pending["_merge"] == "left_only"].drop(columns="_merge")
    elif not repaired_keys.empty:
        pending = pending.copy()
        pending["trade_date"] = pd.to_datetime(pending["trade_date"]).dt.normalize()
        pending = pending.merge(repaired_keys.drop_duplicates(), on=["symbol", "trade_date"], how="left", indicator=True)
        pending = pending[pending["_merge"] == "left_only"].drop(columns="_merge")
    if pending.empty:
        print(json.dumps({"ok": True, "repaired": 0, "pending": 0}, ensure_ascii=False)); return
    # Keep a repair batch concentrated in a small number of immutable yearly
    # partitions.  This is purely an execution-order optimization: every
    # unrepaired invalid key remains eligible and still has to pass both
    # adjacent QMT-to-AKShare anchor comparisons before a target is replaced.
    pending = pending.drop_duplicates(["symbol", "trade_date"], keep="last").copy()
    pending["_trade_year"] = pd.to_datetime(pending["trade_date"]).dt.year
    pending = (pending.sort_values(["asset_type", "_trade_year", "trade_date", "symbol"], kind="stable")
                      .head(args.limit)
                      .drop(columns="_trade_year"))
    validator, repairs, results = AkshareValidator(timeout=args.timeout), [], []
    for row in pending.to_dict(orient="records"):
        asset_type, day = row.get("asset_type", infer_asset_type(row["symbol"])), str(row["trade_date"])[:10]
        try:
            anchors = _repair_anchors(current_bars, invalid_keys, row, pd)
            start, end = str(anchors["trade_date"].min())[:10], str(anchors["trade_date"].max())[:10]
            ak = validator.fetch(row["symbol"], asset_type, start, end)
            target = ak[ak.trade_date == pd.Timestamp(day)]
            if target.empty:
                raise DataLakeError("AKShare missing target trade date")
            aligned, reason, _ = _anchor_validation(validator, anchors, ak, asset_type, pd)
            if not aligned:
                results.append({"symbol":row["symbol"],"trade_date":day,"asset_type":asset_type,"status":"unverified","reason":reason,"qmt_json":json.dumps(row, default=str, ensure_ascii=False),"akshare_json":target.iloc[0].to_json(force_ascii=False, date_format="iso")})
                continue
            repairs.append(target.iloc[[0]])
            results.append({"symbol":row["symbol"],"trade_date":day,"asset_type":asset_type,"status":"repaired","reason":"akshare_replacement_after_adjacent_match","qmt_json":json.dumps(row, default=str, ensure_ascii=False),"akshare_json":target.iloc[0].to_json(force_ascii=False, date_format="iso")})
        except Exception as exc:
            results.append({"symbol":row["symbol"],"trade_date":day,"asset_type":asset_type,"status":"unverified","reason":"akshare_error:%s"%exc,"qmt_json":json.dumps(row, default=str, ensure_ascii=False),"akshare_json":"{}"})
    version = lake.begin_manifest("AKShare invalid-bar repair")
    for asset_type in ("stock", "etf"):
        frames=[f for f in repairs if infer_asset_type(f.iloc[0].symbol)==asset_type]
        if frames: lake.stage_bars(pd.concat(frames, ignore_index=True), asset_type, version, source="akshare_repair")
    # Validation outcomes are a partitioned data set as well as an audit table.
    # This guarantees a manifest can commit even when AKShare returns no usable bar.
    result_frame = pd.DataFrame(results)
    lake.stage_records("validation_results", result_frame, version, source="akshare")
    lake.record_validation(result_frame, version)
    lake.commit(version)
    print(json.dumps({"ok":True,"attempted":len(pending),"repaired":len(repairs),
                      "unverified":len(pending)-len(repairs),"manifest_version":version},ensure_ascii=False))


def cmd_audit_akshare_repairs(args):
    """Apply the adjacent-bar rule retroactively to earlier replacements."""
    import pandas as pd
    lake = DataLake(args.root).initialize()
    current = lake.get_records("bars_raw")
    invalid = lake.get_records("invalid_bars")
    repaired = current[current.get("source", "").eq("akshare_repair")].copy()
    if repaired.empty:
        print(json.dumps({"ok": True, "audited": 0, "retained": 0, "reverted": 0}, ensure_ascii=False)); return
    originals = invalid.drop_duplicates(["symbol", "trade_date"], keep="last").copy()
    originals["trade_date"] = pd.to_datetime(originals["trade_date"]).dt.normalize()
    original_by_key = {(item["symbol"], item["trade_date"]): item for item in originals.to_dict(orient="records")}
    invalid_keys = set(original_by_key)
    repaired["trade_date"] = pd.to_datetime(repaired["trade_date"]).dt.normalize()
    repaired = repaired.drop_duplicates(["symbol", "trade_date"], keep="last").head(args.limit)
    validator, restores, results = AkshareValidator(timeout=args.timeout), [], []
    for item in repaired.to_dict(orient="records"):
        key, asset_type = (item["symbol"], item["trade_date"]), item.get("asset_type", infer_asset_type(item["symbol"]))
        original = original_by_key.get(key)
        if original is None:
            continue
        try:
            anchors = _repair_anchors(current, invalid_keys, original, pd)
            ak = validator.fetch(item["symbol"], asset_type, str(anchors["trade_date"].min())[:10], str(anchors["trade_date"].max())[:10])
            aligned, reason, _ = _anchor_validation(validator, anchors, ak, asset_type, pd)
            if aligned:
                results.append({"symbol":key[0],"trade_date":key[1],"asset_type":asset_type,"status":"repaired","reason":"adjacent_bars_audit_passed","qmt_json":json.dumps(original, default=str, ensure_ascii=False),"akshare_json":json.dumps(item, default=str, ensure_ascii=False)})
            else:
                restores.append(original)
                results.append({"symbol":key[0],"trade_date":key[1],"asset_type":asset_type,"status":"unverified","reason":reason,"qmt_json":json.dumps(original, default=str, ensure_ascii=False),"akshare_json":json.dumps(item, default=str, ensure_ascii=False)})
        except Exception as exc:
            restores.append(original)
            results.append({"symbol":key[0],"trade_date":key[1],"asset_type":asset_type,"status":"unverified","reason":"adjacent_bars_audit_error:%s" % exc,"qmt_json":json.dumps(original, default=str, ensure_ascii=False),"akshare_json":json.dumps(item, default=str, ensure_ascii=False)})
    version = lake.begin_manifest("Audit AKShare repairs with adjacent-bar validation")
    for asset_type in ("stock", "etf"):
        frames = [pd.DataFrame([item]) for item in restores if item.get("asset_type", infer_asset_type(item["symbol"])) == asset_type]
        if frames:
            lake.stage_bars(pd.concat(frames, ignore_index=True), asset_type, version, source="qmt_restored", allow_invalid=True)
    result_frame = pd.DataFrame(results)
    lake.stage_records("validation_results", result_frame, version, source="akshare")
    lake.record_validation(result_frame, version)
    lake.commit(version)
    print(json.dumps({"ok": True, "audited": len(results), "retained": len(results) - len(restores),
                      "reverted": len(restores), "manifest_version": version}, ensure_ascii=False))


def build_parser():
    parser = argparse.ArgumentParser(description="QMT daily data lake and AKShare validation")
    parser.add_argument("--root", default=r"D:\QMT-data", help="independent local data directory")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func in (("bootstrap", cmd_bootstrap), ("sync", cmd_sync)):
        item = sub.add_parser(name)
        item.add_argument("--start", required=name == "bootstrap", help="YYYYMMDD or YYYY-MM-DD")
        item.add_argument("--end", required=name == "bootstrap", help="YYYYMMDD or YYYY-MM-DD")
        item.add_argument("--symbols-file", help="optional QMT symbols, one per line")
        item.add_argument("--chunk-size", type=int, default=100)
        item.add_argument("--reset-checkpoint", action="store_true",
                          help="discard only this command's saved progress before starting")
        item.set_defaults(func=func)
    item = sub.add_parser("sync-calendar", help="persist QMT exchange calendar and session definitions")
    item.add_argument("--start", help="YYYYMMDD or YYYY-MM-DD; defaults to QMT's latest date")
    item.add_argument("--end", help="YYYYMMDD or YYYY-MM-DD; defaults to QMT's latest date")
    item.add_argument("--markets", default="SH,SZ", help="comma-separated QMT market codes")
    item.set_defaults(func=cmd_sync_calendar)
    item = sub.add_parser("validate-akshare")
    item.add_argument("--date", help="one trade date; mutually exclusive with --start/--end (defaults to warehouse latest)")
    item.add_argument("--start", help="initial full-history validation start")
    item.add_argument("--end", help="initial full-history validation end")
    item.add_argument("--manifest-version")
    item.add_argument("--stock-sample", type=int, default=120)
    item.add_argument("--etf-sample", type=int, default=30)
    item.add_argument("--timeout", type=float, default=15)
    item.set_defaults(func=cmd_validate_akshare)
    item = sub.add_parser("repair-akshare")
    item.add_argument("--limit", type=int, default=100)
    item.add_argument("--timeout", type=float, default=15)
    item.add_argument("--retry-unverified", action="store_true", help="retry rows with a prior AKShare connection or response failure")
    item.set_defaults(func=cmd_repair_akshare)
    item = sub.add_parser("audit-akshare-repairs")
    item.add_argument("--limit", type=int, default=100)
    item.add_argument("--timeout", type=float, default=15)
    item.set_defaults(func=cmd_audit_akshare_repairs)
    item = sub.add_parser("export-csv")
    item.add_argument("--output", required=True)
    item.add_argument("--start", required=True)
    item.add_argument("--end", required=True)
    item.add_argument("--symbols-file")
    item.add_argument("--manifest-version")
    item.set_defaults(func=cmd_export_csv)
    item = sub.add_parser("research-snapshot")
    item.add_argument("--strategy", required=True, help="strategy or experiment name")
    item.add_argument("--asof", required=True, help="last close date visible to the signal")
    item.add_argument("--execution-date", required=True, help="next eligible execution date; must be after --asof")
    item.add_argument("--parameters-json", default="{}", help="JSON object with strategy parameters")
    item.add_argument("--code-version", default="", help="source-control revision or immutable strategy build id")
    item.add_argument("--note", default="")
    item.set_defaults(func=cmd_research_snapshot)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
        return 0
    except DataLakeError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
