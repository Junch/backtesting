from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import order_utils


class _FakeBaoResult:
    def __init__(self, rows, error_code: str = "0", error_msg: str = "") -> None:
        self._rows = rows
        self._idx = -1
        self.error_code = error_code
        self.error_msg = error_msg

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self):
        return self._rows[self._idx]


class _FakeBaoStockApi:
    def __init__(self, rows_by_code: dict[str, list[list[str]]]) -> None:
        self._rows_by_code = rows_by_code

    def login(self):
        return type("LoginRet", (), {"error_code": "0", "error_msg": ""})()

    def logout(self):
        return None

    def query_history_k_data_plus(self, code, fields, start_date, end_date, frequency, adjustflag):
        rows = self._rows_by_code.get(code, [])
        return _FakeBaoResult(rows=rows)


class _FakeXtDataApi:
    def __init__(self, close_by_code: dict[str, list[float]]) -> None:
        self._close_by_code = close_by_code

    def get_market_data_ex(self, stock_list, period, start_time, end_time, dividend_type, field_list):
        payload: dict[str, pd.DataFrame] = {}
        for code in stock_list:
            close_values = self._close_by_code.get(code)
            if close_values is None:
                continue
            payload[code] = pd.DataFrame({"close": close_values})
        return payload


def test_fetch_close_prices_qmt_success(monkeypatch):
    monkeypatch.setattr(
        order_utils,
        "xtdata",
        _FakeXtDataApi(close_by_code={"000001.SZ": [10.5]}),
    )
    monkeypatch.setattr(order_utils, "bs", None)

    prices, sources, errors = order_utils._fetch_close_prices_for_signal_date(
        ["000001.SZ"],
        pd.Timestamp("2026-03-13"),
    )

    assert prices == {"000001.SZ": 10.5}
    assert sources == {"000001.SZ": "close_qmt"}
    assert errors == []


def test_fetch_close_prices_fallback_to_baostock(monkeypatch):
    monkeypatch.setattr(
        order_utils,
        "xtdata",
        _FakeXtDataApi(close_by_code={}),
    )
    monkeypatch.setattr(
        order_utils,
        "bs",
        _FakeBaoStockApi(
            rows_by_code={
                "sz.000001": [["2026-03-12", "10.20"], ["2026-03-13", "10.35"]]
            }
        ),
    )

    prices, sources, errors = order_utils._fetch_close_prices_for_signal_date(
        ["000001.SZ"],
        pd.Timestamp("2026-03-13"),
    )

    assert prices == {"000001.SZ": 10.35}
    assert sources == {"000001.SZ": "close_baostock"}
    assert errors == []


def test_double_failure_keeps_symbol_and_quantity_zero(monkeypatch):
    monkeypatch.setattr(order_utils, "xtdata", None)
    monkeypatch.setattr(order_utils, "bs", None)

    prices, sources, errors = order_utils._fetch_close_prices_for_signal_date(
        ["000001.SZ"],
        pd.Timestamp("2026-03-13"),
    )

    assert prices == {}
    assert sources == {}
    assert any("000001.SZ" in message for message in errors)

    ranked_df = pd.DataFrame(
        {
            "stock_code": ["000001.SZ"],
            "stock_name": ["平安银行"],
        }
    )

    allocated_df = order_utils._calculate_allocated_quantities(
        ranked_df=ranked_df,
        total_capital=100000.0,
        buy_count=1,
        close_prices=prices,
        price_sources=sources,
    )

    assert int(allocated_df.loc[0, "下单数量(股)"]) == 0
    assert allocated_df.loc[0, "下单备注"] == "无可用信号日收盘价"


def test_pick_date_maps_to_signal_date():
    trading_days = [
        pd.Timestamp("2026-03-12"),
        pd.Timestamp("2026-03-13"),
    ]
    signal_date = order_utils._resolve_signal_date(
        trading_days=trading_days,
        target_date=pd.Timestamp("2026-03-14"),
    )

    assert signal_date == pd.Timestamp("2026-03-13")
