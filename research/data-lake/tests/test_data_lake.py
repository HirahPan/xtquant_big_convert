# coding: utf-8
"""Offline checks for the daily Parquet/DuckDB research store."""

from __future__ import absolute_import

import os
import json
import shutil
import sys
import tempfile
import unittest

import pandas as pd


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from qmt_data_lake.data_lake import (  # noqa: E402
    AkshareValidator,
    DataLake,
    deterministic_sample,
    normalize_qmt_bars,
)
from qmt_data_lake.cli import _date_text, _listing_start, _session_rows  # noqa: E402
from qmt_data_lake.research import create_research_snapshot  # noqa: E402


def _bars(symbol="600000.SH"):
    return pd.DataFrame([
        {"symbol": symbol, "stime": "2026-01-05", "open": 10.0, "high": 10.5, "low": 9.8,
         "close": 10.4, "volume": 1000, "amount": 10400, "preClose": 10.0,
         "suspendFlag": 0},
        {"symbol": symbol, "stime": "2026-01-06", "open": 10.0, "high": 10.2, "low": 9.9,
         "close": 10.1, "volume": 1100, "amount": 11110, "preClose": 10.0,
         "suspendFlag": 0},
    ])


class DataLakeTest(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix="bigqmt_data_lake_")
        self.lake = DataLake(self.path).initialize()

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)

    def test_normalizer_keeps_false_suspend_flag(self):
        result = normalize_qmt_bars(_bars(), "600000.SH")
        self.assertEqual(result.symbol.tolist(), ["600000.SH", "600000.SH"])
        self.assertFalse(result.suspended.any())

    def test_export_mode_drops_only_an_invalid_row(self):
        bad = _bars()
        bad.loc[0, "low"] = 10.2
        with self.assertRaises(Exception):
            normalize_qmt_bars(bad, "600000.SH")
        result = normalize_qmt_bars(bad, "600000.SH", drop_invalid=True)
        self.assertEqual(result.trade_date.astype(str).tolist(), ["2026-01-06"])

    def test_full_ingest_retains_and_marks_invalid_row(self):
        bad = _bars()
        bad.loc[0, "low"] = 10.2
        bars, invalid = normalize_qmt_bars(bad, "600000.SH", return_invalid=True)
        self.assertEqual(len(bars), 2)
        self.assertEqual(len(invalid), 1)
        self.assertEqual(invalid.quality_reason.tolist(), ["low_above_ohlc"])
        self.assertEqual(invalid.quality_status.tolist(), ["pending_validation"])

    def test_commit_version_and_export_csv(self):
        version = self.lake.begin_manifest("test")
        self.lake.stage_bars(_bars(), "stock", version)
        self.lake.commit(version)

        raw = self.lake.get_bars(symbols=["600000.SH"], manifest_version=version)
        self.assertEqual(len(raw), 2)
        self.assertEqual(raw.close.tolist(), [10.4, 10.1])

        adjusted = self.lake.get_bars(symbols=["600000.SH"], price_basis="front_ratio")
        # 2026-01-06 preClose=10.0 versus prior raw close=10.4.
        self.assertAlmostEqual(adjusted.iloc[0].close, 10.0, places=6)
        self.assertAlmostEqual(adjusted.iloc[1].close, 10.1, places=6)

        output = os.path.join(self.path, "export.csv")
        self.assertEqual(self.lake.export_csv(output, symbols=["600000.SH"]), 2)
        self.assertEqual(len(pd.read_csv(output)), 2)

    def test_new_version_retains_prior_rows_and_quarantine_excludes(self):
        first = self.lake.begin_manifest("first")
        self.lake.stage_bars(_bars(), "stock", first)
        self.lake.commit(first)
        second = self.lake.begin_manifest("second")
        update = pd.DataFrame([{"symbol": "600000.SH", "stime": "2026-01-07", "open": 10.2, "high": 10.5,
                                "low": 10.0, "close": 10.4, "volume": 1200,
                                "amount": 12480, "preClose": 10.1, "suspendFlag": 0}])
        self.lake.stage_bars(update, "stock", second)
        self.lake.commit(second)
        self.assertEqual(len(self.lake.get_bars(symbols=["600000.SH"])), 3)

        results = pd.DataFrame([{
            "symbol": "600000.SH", "trade_date": "2026-01-07", "asset_type": "stock",
            "status": "mismatch", "reason": "price_close", "qmt_json": "{}", "akshare_json": "{}",
        }])
        self.lake.record_validation(results, second)
        self.assertEqual(len(self.lake.get_bars(symbols=["600000.SH"])), 2)
        self.assertEqual(len(self.lake.get_bars(symbols=["600000.SH"], quality="all")), 3)

    def test_reference_snapshot_has_asof_lookup(self):
        version = self.lake.begin_manifest("universe")
        self.lake.stage_records("universe", pd.DataFrame([{
            "symbol": "600000.SH", "pool_name": "沪深A股", "trade_date": "2026-01-07",
        }]), version)
        self.lake.commit(version)
        result = self.lake.get_universe("沪深A股", "2026-01-07")
        self.assertEqual(result.symbol.tolist(), ["600000.SH"])

    def test_research_snapshot_freezes_catalog_and_rejects_same_day_execution(self):
        version = self.lake.begin_manifest("research bars")
        self.lake.stage_bars(_bars(), "stock", version)
        self.lake.commit(version)
        snapshot, path = create_research_snapshot(
            self.lake, strategy="daily-value", asof="2026-01-06", execution_date="2026-01-07",
            parameters={"lookback": 20}, code_version="test-revision",
        )
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(snapshot["timing_policy"], "signal_after_close_execute_next_trading_day")
        self.assertEqual(snapshot["parameters"]["lookback"], 20)
        self.assertTrue(snapshot["catalog_fingerprint"])
        with open(path, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle)["run_id"], snapshot["run_id"])
        with self.assertRaises(Exception):
            create_research_snapshot(self.lake, "daily-value", "2026-01-06", "2026-01-06")

    def test_calendar_requires_the_next_stored_trade_day_and_sessions_keep_provenance(self):
        version = self.lake.begin_manifest("calendar")
        self.lake.stage_records("trading_calendar", pd.DataFrame([
            {"market": "SH", "trade_date": "2026-01-06", "is_trading_day": True},
            {"market": "SH", "trade_date": "2026-01-08", "is_trading_day": True},
        ]), version)
        self.lake.stage_records("market_sessions", pd.DataFrame([
            {"market": "SH", "session_id": "continuous_morning", "begin_time": "09:30:00",
             "end_time": "11:30:00", "session_source": "configured_cn_equity_v1", "trade_date": "2026-01-06"},
        ]), version)
        self.lake.stage_bars(_bars(), "stock", version)
        self.lake.commit(version)
        self.assertEqual(str(self.lake.next_trading_day("SH", "2026-01-06")), "2026-01-08")
        self.assertEqual(self.lake.get_market_sessions("SH", "2026-01-06").iloc[0].session_source,
                         "configured_cn_equity_v1")
        with self.assertRaises(Exception):
            create_research_snapshot(self.lake, "daily-value", "2026-01-06", "2026-01-07")

    def test_empty_qmt_session_api_uses_explicitly_configured_fallback(self):
        class Source(object):
            def get_trade_times(self, market):
                return []
        rows = _session_rows("SH", "20260904", Source())
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["session_source"] == "configured_cn_equity_v1" for row in rows))


class AkshareComparisonTest(unittest.TestCase):
    def test_match_and_mismatch_are_explicit(self):
        qmt = normalize_qmt_bars(_bars(), "600000.SH")
        same = AkshareValidator.compare(qmt, qmt, "stock")
        self.assertEqual(set(same.status), {"match"})
        changed = qmt.copy()
        changed.loc[0, "close"] = 10.49
        result = AkshareValidator.compare(qmt, changed, "stock")
        self.assertIn("mismatch", result.status.tolist())

    def test_sampling_is_repeatable(self):
        symbols = ["600000.SH", "000001.SZ", "510300.SH"]
        self.assertEqual(deterministic_sample(symbols, "2026-01-07", 2),
                         deterministic_sample(list(reversed(symbols)), "2026-01-07", 2))

    def test_volume_lot_scale_is_not_a_false_mismatch(self):
        qmt = normalize_qmt_bars(_bars(), "600000.SH")
        ak = qmt.copy()
        ak["volume"] = ak["volume"] / 100.0
        self.assertEqual(set(AkshareValidator.compare(qmt, ak, "stock").status), {"match"})


class ListingDateTest(unittest.TestCase):
    def test_listing_date_is_normalized_and_bounded_by_requested_floor(self):
        class Source(object):
            def get_open_date(self, code):
                self.code = code
                return 20170418

        source = Source()
        self.assertEqual(_date_text("2017-04-18"), "20170418")
        self.assertEqual(_date_text(661536000000), "19901219")
        self.assertEqual(_listing_start(source, "600000.SH", "20190101"), "20190101")
        self.assertEqual(source.code, "600000.SH")

    def test_missing_listing_date_refuses_broad_history_query(self):
        class Source(object):
            def get_open_date(self, code):
                return None

            def get_instrument_detail(self, code):
                return {}

        with self.assertRaises(Exception):
            _listing_start(Source(), "600000.SH", "20150101")


if __name__ == "__main__":
    unittest.main()
