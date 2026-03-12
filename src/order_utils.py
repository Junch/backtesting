from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from xtquant import xtdata
except Exception:
    xtdata = None


def _to_positive_float(value: object) -> Optional[float]:
    """将任意对象转换为正数浮点，失败返回 None。"""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(number) or number <= 0:
        return None
    return number


def _extract_quote_price_from_tick(tick_item: dict) -> Tuple[Optional[float], str]:
    """优先 askPrice1，缺失时回退 lastPrice。"""

    ask_price = tick_item.get("askPrice1")
    if isinstance(ask_price, (list, tuple, np.ndarray)):
        ask_price = ask_price[0] if len(ask_price) > 0 else None
    ask_price = _to_positive_float(ask_price)
    if ask_price is not None:
        return ask_price, "askPrice1"

    last_price = _to_positive_float(tick_item.get("lastPrice"))
    if last_price is not None:
        return last_price, "lastPrice"

    return None, ""


def _resolve_signal_date(
    trading_days: List[pd.Timestamp], target_date: datetime
) -> Optional[pd.Timestamp]:
    """将用户输入日期映射到不晚于该日期的最近交易日。"""
    if not trading_days:
        return None

    target_ts = pd.Timestamp(target_date).normalize()
    eligible = [d for d in trading_days if pd.Timestamp(d).normalize() <= target_ts]
    if not eligible:
        return None
    return pd.Timestamp(max(eligible)).normalize()


def _get_next_trade_date(
    trading_days: List[pd.Timestamp], signal_date: pd.Timestamp
) -> pd.Timestamp:
    """返回信号日后的下一交易日；若缺失则回退到自然日+1。"""
    signal_ts = pd.Timestamp(signal_date).normalize()
    future_days = [d for d in trading_days if pd.Timestamp(d).normalize() > signal_ts]
    if future_days:
        return pd.Timestamp(min(future_days)).normalize()
    return (signal_ts + timedelta(days=1)).normalize()


def _build_order_lines(
    ranked_df: pd.DataFrame,
    order_date: pd.Timestamp,
    quantity_column: str = "下单数量(股)",
) -> List[List[str]]:
    """将候选股转换为 order.csv 格式行（无表头，含可选限价列）。"""
    rows: List[List[str]] = []
    date_text = order_date.strftime("%Y-%m-%d")

    for _, row in ranked_df.iterrows():
        stock_code = str(row.get("stock_code", "")).upper().strip()
        if not re.match(r"^\d{6}\.(SH|SZ|BJ)$", stock_code):
            continue

        try:
            quantity = int(float(row.get(quantity_column, 0)))
        except (TypeError, ValueError):
            quantity = 0
        if quantity <= 0:
            continue

        stock_name = str(row.get("stock_name", stock_code)).strip() or stock_code
        rows.append([date_text, "买入", stock_code, stock_name, str(quantity), ""])

    return rows


def _fetch_realtime_prices(
    stock_codes: List[str],
) -> Tuple[Dict[str, float], Dict[str, str], List[str]]:
    """批量获取实时报价，返回价格、价格来源与错误列表。"""
    prices: Dict[str, float] = {}
    price_sources: Dict[str, str] = {}
    errors: List[str] = []

    if xtdata is None:
        errors.append("xtquant.xtdata 不可用，请在可连接 miniQMT 的环境运行")
        return prices, price_sources, errors

    valid_codes = []
    for code in stock_codes:
        code_upper = str(code).upper().strip()
        if re.match(r"^\d{6}\.(SH|SZ|BJ)$", code_upper):
            valid_codes.append(code_upper)

    if not valid_codes:
        errors.append("没有可用于获取实时价格的合法股票代码")
        return prices, price_sources, errors

    try:
        tick_data = xtdata.get_full_tick(valid_codes) or {}
    except Exception as e:
        errors.append(f"获取 xtdata 实时行情失败: {e}")
        return prices, price_sources, errors

    for code in valid_codes:
        tick_item = tick_data.get(code)
        if not isinstance(tick_item, dict):
            errors.append(f"{code} 未返回 tick 数据")
            continue

        price, source = _extract_quote_price_from_tick(tick_item)
        if price is None:
            errors.append(f"{code} 缺少 askPrice1 和 lastPrice")
            continue

        prices[code] = price
        price_sources[code] = source

    return prices, price_sources, errors


def _calculate_allocated_quantities(
    ranked_df: pd.DataFrame,
    total_capital: float,
    buy_count: int,
    realtime_prices: Dict[str, float],
    price_sources: Dict[str, str],
    lot_size: int = 100,
) -> pd.DataFrame:
    """基于总资金和实时价格计算每只股票下单数量。"""
    result_df = ranked_df.copy()
    per_stock_budget = total_capital / buy_count if buy_count > 0 else 0.0

    quantities: List[int] = []
    allocated_amounts: List[float] = []
    used_prices: List[float] = []
    used_price_sources: List[str] = []
    notes: List[str] = []

    for _, row in result_df.iterrows():
        stock_code = str(row.get("stock_code", "")).upper().strip()
        price = realtime_prices.get(stock_code)
        source = price_sources.get(stock_code, "")

        if price is None or price <= 0:
            quantities.append(0)
            allocated_amounts.append(0.0)
            used_prices.append(np.nan)
            used_price_sources.append("")
            notes.append("无可用实时价格")
            continue

        raw_qty = int(per_stock_budget / price)
        qty = (raw_qty // lot_size) * lot_size

        if qty < lot_size:
            quantities.append(0)
            allocated_amounts.append(0.0)
            used_prices.append(price)
            used_price_sources.append(source)
            notes.append("单票预算不足100股")
            continue

        quantities.append(qty)
        allocated_amounts.append(qty * price)
        used_prices.append(price)
        used_price_sources.append(source)
        notes.append("")

    result_df["单票预算(元)"] = per_stock_budget
    result_df["实时价格"] = used_prices
    result_df["价格来源"] = used_price_sources
    result_df["下单数量(股)"] = quantities
    result_df["预计下单金额(元)"] = allocated_amounts
    result_df["下单备注"] = notes
    return result_df
