from datetime import datetime, timedelta
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from xtquant import xtdata
except Exception:
    xtdata = None

try:
    import baostock as bs  # type: ignore[reportMissingImports]
except Exception:
    bs = None


def _to_positive_float(value: Any) -> Optional[float]:
    """将任意对象转换为正数浮点，失败返回 None。"""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(number) or number <= 0:
        return None
    return number


def _to_baostock_code(stock_code: str) -> Optional[str]:
    """将 000001.SZ 转为 baostock 代码格式。"""
    normalized = str(stock_code).upper().strip()
    match = re.match(r"^(\d{6})\.(SH|SZ|BJ)$", normalized)
    if not match:
        return None
    numeric_code, market = match.groups()
    return f"{market.lower()}.{numeric_code}"


def _extract_close_from_qmt_item(item: object) -> Optional[float]:
    """从 get_market_data_ex 返回项中提取最后一个有效 close。"""
    if not isinstance(item, pd.DataFrame) or "close" not in item.columns:
        return None
    if item.empty:
        return None

    close_values = pd.to_numeric(item["close"], errors="coerce").tolist()
    for close_value in reversed(close_values):
        close_price = _to_positive_float(close_value)
        if close_price is not None:
            return close_price
    return None


def _fetch_close_prices_from_qmt(
    stock_codes: List[str], signal_date: pd.Timestamp
) -> Tuple[Dict[str, float], Dict[str, str], List[str], List[str]]:
    """通过 QMT 读取信号日收盘价。"""
    prices: Dict[str, float] = {}
    sources: Dict[str, str] = {}
    errors: List[str] = []

    if xtdata is None:
        errors.append("xtquant.xtdata 不可用，跳过 QMT 收盘价读取")
        return prices, sources, stock_codes, errors

    signal_text = pd.Timestamp(signal_date).strftime("%Y%m%d")
    try:
        market_data = xtdata.get_market_data_ex(
            stock_list=stock_codes,
            period="1d",
            start_time=signal_text,
            end_time=signal_text,
            dividend_type="front",
            field_list=["close"],
        )
    except Exception as e:
        errors.append(f"QMT 获取信号日收盘价失败: {e}")
        return prices, sources, stock_codes, errors

    market_data = market_data if isinstance(market_data, dict) else {}
    unresolved_codes: List[str] = []
    for code in stock_codes:
        close_price = _extract_close_from_qmt_item(market_data.get(code))
        if close_price is None:
            unresolved_codes.append(code)
            continue
        prices[code] = close_price
        sources[code] = "close_qmt"

    return prices, sources, unresolved_codes, errors


def _fetch_close_prices_from_baostock(
    stock_codes: List[str], signal_date: pd.Timestamp
) -> Tuple[Dict[str, float], Dict[str, str], List[str], List[str]]:
    """通过 baostock 回退读取信号日收盘价。"""
    prices: Dict[str, float] = {}
    sources: Dict[str, str] = {}
    errors: List[str] = []

    if not stock_codes:
        return prices, sources, [], errors

    if bs is None:
        errors.append("baostock 不可用，无法执行收盘价回退")
        return prices, sources, stock_codes, errors

    login_result = None
    try:
        login_result = bs.login()
        if getattr(login_result, "error_code", "") != "0":
            errors.append(
                f"baostock 登录失败: {getattr(login_result, 'error_msg', 'unknown')}"
            )
            return prices, sources, stock_codes, errors

        signal_ts = pd.Timestamp(signal_date).normalize()
        start_date = (signal_ts - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        end_date = signal_ts.strftime("%Y-%m-%d")
        unresolved_codes: List[str] = []

        for code in stock_codes:
            bs_code = _to_baostock_code(code)
            if bs_code is None:
                unresolved_codes.append(code)
                errors.append(f"{code} 无法转换为 baostock 代码格式")
                continue

            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,close",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2",
                )
            except Exception as e:
                unresolved_codes.append(code)
                errors.append(f"{code} baostock 查询失败: {e}")
                continue

            if getattr(rs, "error_code", "") != "0":
                unresolved_codes.append(code)
                errors.append(
                    f"{code} baostock 返回错误: {getattr(rs, 'error_msg', 'unknown')}"
                )
                continue

            close_price: Optional[float] = None
            while rs.next():
                row_data = rs.get_row_data()
                if len(row_data) < 2:
                    continue
                parsed = _to_positive_float(row_data[1])
                if parsed is not None:
                    close_price = parsed

            if close_price is None:
                unresolved_codes.append(code)
                errors.append(f"{code} baostock 未返回可用收盘价")
                continue

            prices[code] = close_price
            sources[code] = "close_baostock"

        return prices, sources, unresolved_codes, errors
    finally:
        if login_result is not None:
            try:
                bs.logout()
            except Exception:
                pass


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


def _fetch_close_prices_for_signal_date(
    stock_codes: List[str], signal_date: pd.Timestamp
) -> Tuple[Dict[str, float], Dict[str, str], List[str]]:
    """获取信号日收盘价，优先 QMT，失败后回退 baostock。"""
    prices: Dict[str, float] = {}
    price_sources: Dict[str, str] = {}
    errors: List[str] = []

    valid_codes = []
    seen_codes = set()
    for code in stock_codes:
        code_upper = str(code).upper().strip()
        if re.match(r"^\d{6}\.(SH|SZ|BJ)$", code_upper) and code_upper not in seen_codes:
            valid_codes.append(code_upper)
            seen_codes.add(code_upper)

    if not valid_codes:
        errors.append("没有可用于获取信号日收盘价的合法股票代码")
        return prices, price_sources, errors

    qmt_prices, qmt_sources, unresolved_codes, qmt_errors = _fetch_close_prices_from_qmt(
        valid_codes, pd.Timestamp(signal_date)
    )
    prices.update(qmt_prices)
    price_sources.update(qmt_sources)
    errors.extend(qmt_errors)

    bs_prices, bs_sources, still_unresolved, bs_errors = _fetch_close_prices_from_baostock(
        unresolved_codes, pd.Timestamp(signal_date)
    )
    prices.update(bs_prices)
    price_sources.update(bs_sources)
    errors.extend(bs_errors)

    for code in still_unresolved:
        errors.append(f"{code} 在信号日无可用收盘价（QMT 与 baostock 均失败）")

    return prices, price_sources, errors


def _calculate_allocated_quantities(
    ranked_df: pd.DataFrame,
    total_capital: float,
    buy_count: int,
    close_prices: Dict[str, float],
    price_sources: Dict[str, str],
    lot_size: int = 100,
) -> pd.DataFrame:
    """基于总资金和信号日收盘价计算每只股票下单数量。"""
    result_df = ranked_df.copy()
    per_stock_budget = total_capital / buy_count if buy_count > 0 else 0.0

    quantities: List[int] = []
    allocated_amounts: List[float] = []
    used_prices: List[float] = []
    used_price_sources: List[str] = []
    notes: List[str] = []

    for _, row in result_df.iterrows():
        stock_code = str(row.get("stock_code", "")).upper().strip()
        price = close_prices.get(stock_code)
        source = price_sources.get(stock_code, "")

        if price is None or price <= 0:
            quantities.append(0)
            allocated_amounts.append(0.0)
            used_prices.append(np.nan)
            used_price_sources.append("")
            notes.append("无可用信号日收盘价")
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
    result_df["收盘价"] = used_prices
    result_df["收盘价来源"] = used_price_sources
    result_df["下单数量(股)"] = quantities
    result_df["预计下单金额(元)"] = allocated_amounts
    result_df["下单备注"] = notes
    return result_df
