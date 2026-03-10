"""
基于回测日志重建收益率曲线的 Streamlit 工具。

功能:
1. 读取回测系统输出的 .log 文件（买卖成交记录）。
2. 结合 cjdata 行情（LocalData）重建每日资产净值。
3. 支持输入日期查询当日持仓明细。
"""

from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from cjdata import LocalData


INITIAL_CAPITAL = 1_000_000.0
DEFAULT_DB_PATH = "C:/github/cjdata/data/stock_data_hfq.db"
LOG_PATTERN = re.compile(
    r"^(?P<trade_date>\d{4}-\d{2}-\d{2})\s+"
    r"(?P<side>买入|卖出)"
    r"(?P<stock_code>\d{6}\.(?:SH|SZ))"
    r"\((?P<stock_name>[^)]*)\),\s*"
    r"成交价(?P<price>\d+(?:\.\d+)?),\s*"
    r"成交量(?P<volume>\d+),\s*"
    r"佣金(?P<commission>\d+(?:\.\d+)?)"
)


@dataclass
class ReplayResult:
    equity_curve: pd.DataFrame
    holdings_by_date: dict[pd.Timestamp, pd.DataFrame]
    trades: pd.DataFrame


@st.cache_data(show_spinner=False)
def parse_trade_log(content: str) -> pd.DataFrame:
    """解析日志文本，仅提取成交买卖记录。"""
    rows: list[dict] = []

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = LOG_PATTERN.match(line)
        if not match:
            continue

        gd = match.groupdict()
        rows.append(
            {
                "trade_date": pd.to_datetime(gd["trade_date"]),
                "side": gd["side"],
                "stock_code": gd["stock_code"],
                "stock_name": gd["stock_name"],
                "price": float(gd["price"]),
                "volume": int(gd["volume"]),
                "commission": float(gd["commission"]),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "side",
                "stock_code",
                "stock_name",
                "price",
                "volume",
                "commission",
            ]
        )

    df = pd.DataFrame(rows).sort_values(
        ["trade_date", "stock_code", "side"]
    )  # 稳定排序便于重现结果
    return df.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_market_data(start_date: str, end_date: str) -> pd.DataFrame:
    """加载沪深A股区间行情数据。"""
    db_path = os.environ.get("STOCK_DATA_DB", DEFAULT_DB_PATH)
    data_source = LocalData(db_path)
    df = data_source.get_stock_data_frame_in_sector(
        "沪深A股",
        start_date,
        end_date,
        adj="hfq",
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=["trade_date", "stock_code", "close"])

    use_cols = ["trade_date", "stock_code", "close"]
    df = df[use_cols].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["trade_date", "stock_code"]).reset_index(drop=True)
    return df


def replay_portfolio(
    trades: pd.DataFrame,
    market_df: pd.DataFrame,
    initial_capital: float = INITIAL_CAPITAL,
) -> ReplayResult:
    """按日回放交易，重建组合净值曲线和每日持仓。"""
    if trades.empty:
        return ReplayResult(
            equity_curve=pd.DataFrame(),
            holdings_by_date={},
            trades=trades,
        )

    trade_codes = sorted(trades["stock_code"].unique().tolist())
    market_df = market_df[market_df["stock_code"].isin(trade_codes)].copy()

    if market_df.empty:
        return ReplayResult(
            equity_curve=pd.DataFrame(),
            holdings_by_date={},
            trades=trades,
        )

    close_pivot = market_df.pivot(
        index="trade_date", columns="stock_code", values="close"
    )
    all_dates = close_pivot.index.sort_values()

    trades_by_day = {d: g.copy() for d, g in trades.groupby("trade_date", sort=True)}

    cash = float(initial_capital)
    positions: dict[str, int] = {}
    total_commission = 0.0

    curve_rows: list[dict] = []
    holdings_snapshots: dict[pd.Timestamp, pd.DataFrame] = {}

    for dt in all_dates:
        day_trades = trades_by_day.get(dt)

        if day_trades is not None:
            # 先卖后买，和常见调仓执行顺序一致，减少当日资金不足误差。
            side_order = {"卖出": 0, "买入": 1}
            day_trades = day_trades.assign(
                _order=day_trades["side"].map(side_order)
            ).sort_values(["_order", "stock_code"])

            for _, tr in day_trades.iterrows():
                code = tr["stock_code"]
                qty = int(tr["volume"])
                price = float(tr["price"])
                comm = float(tr["commission"])

                if tr["side"] == "买入":
                    cash -= price * qty + comm
                    positions[code] = positions.get(code, 0) + qty
                else:
                    # 对日志中可能出现的异常卖出做保护，避免仓位为负。
                    held = positions.get(code, 0)
                    sell_qty = min(held, qty)
                    cash += price * sell_qty - comm
                    new_qty = held - sell_qty
                    if new_qty > 0:
                        positions[code] = new_qty
                    elif code in positions:
                        del positions[code]

                total_commission += comm

        close_today = close_pivot.loc[dt]
        market_value = 0.0
        holding_rows: list[dict] = []

        for code, qty in sorted(positions.items()):
            close_price = close_today.get(code)
            if pd.isna(close_price):
                continue
            mv = float(close_price) * qty
            market_value += mv
            holding_rows.append(
                {
                    "stock_code": code,
                    "quantity": qty,
                    "close": float(close_price),
                    "market_value": mv,
                }
            )

        total_asset = cash + market_value
        nav = total_asset / initial_capital

        curve_rows.append(
            {
                "trade_date": dt,
                "cash": cash,
                "market_value": market_value,
                "total_asset": total_asset,
                "nav": nav,
                "return_pct": (nav - 1.0) * 100,
                "commission_cum": total_commission,
            }
        )

        holding_df = pd.DataFrame(holding_rows)
        if not holding_df.empty:
            holding_df["weight"] = holding_df["market_value"] / total_asset
        holdings_snapshots[dt] = holding_df

    equity_curve = pd.DataFrame(curve_rows)
    return ReplayResult(
        equity_curve=equity_curve,
        holdings_by_date=holdings_snapshots,
        trades=trades,
    )


def _read_uploaded_or_local_log(
    uploaded_file, selected_local_file: str | None
) -> tuple[str | None, str | None]:
    """读取用户选择的日志内容，返回(内容, 显示名称)。"""
    if uploaded_file is not None:
        content = uploaded_file.getvalue().decode("utf-8", errors="ignore")
        return content, uploaded_file.name

    if selected_local_file:
        p = Path(selected_local_file)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="ignore"), p.name

    return None, None


def _render_holdings_for_date(
    query_date: date,
    holdings_by_date: dict[pd.Timestamp, pd.DataFrame],
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
) -> None:
    qd = pd.Timestamp(query_date)
    available_dates = sorted(holdings_by_date.keys())
    if not available_dates:
        st.warning("没有可用交易日数据。")
        return

    prev_dates = [d for d in available_dates if d <= qd]
    if not prev_dates:
        st.warning("输入日期早于首个交易日，暂无持仓。")
        return

    effective_date = prev_dates[-1]
    holding_df = holdings_by_date.get(effective_date, pd.DataFrame()).copy()

    st.write(
        f"查询日期: `{qd.date()}`，按最近交易日 `{effective_date.date()}` 显示持仓"
    )

    if not equity_curve.empty:
        day_equity = equity_curve[equity_curve["trade_date"] == effective_date]
        if not day_equity.empty:
            cash = day_equity.iloc[0]["cash"]
            market_val = day_equity.iloc[0]["market_value"]
            total_asset = day_equity.iloc[0]["total_asset"]

            prev_date = prev_dates[-2] if len(prev_dates) > 1 else None
            day_pct_change = None
            if prev_date:
                prev_equity = equity_curve[equity_curve["trade_date"] == prev_date]
                if not prev_equity.empty:
                    prev_total = prev_equity.iloc[0]["total_asset"]
                    if prev_total and prev_total != 0:
                        day_pct_change = (total_asset - prev_total) / prev_total * 100

            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("现金", f"{cash:,.2f}")
            with c2:
                st.metric("持仓市值", f"{market_val:,.2f}")
            with c3:
                if day_pct_change is not None:
                    st.metric(
                        "总资产", f"{total_asset:,.2f}", delta=f"{day_pct_change:.2f}%"
                    )
                else:
                    st.metric("总资产", f"{total_asset:,.2f}")

    if holding_df.empty:
        st.info("该日期无持仓。")
        return

    # 估算持仓成本: 按日志成交重建股票持仓的加权平均成本。
    trades_until_date = trades[trades["trade_date"] <= effective_date].copy()
    cost_map: dict[str, float] = {}
    qty_map: dict[str, int] = {}

    for _, tr in trades_until_date.sort_values(
        ["trade_date", "stock_code", "side"]
    ).iterrows():
        code = tr["stock_code"]
        qty = int(tr["volume"])
        price = float(tr["price"])
        side = tr["side"]

        prev_qty = qty_map.get(code, 0)
        prev_cost = cost_map.get(code, 0.0)

        if side == "买入":
            new_qty = prev_qty + qty
            if new_qty > 0:
                new_cost = (prev_cost * prev_qty + price * qty) / new_qty
            else:
                new_cost = 0.0
            qty_map[code] = new_qty
            cost_map[code] = new_cost
        else:
            new_qty = max(prev_qty - qty, 0)
            qty_map[code] = new_qty
            if new_qty == 0:
                cost_map[code] = 0.0

    holding_df["avg_cost"] = holding_df["stock_code"].map(cost_map).fillna(0.0)
    holding_df["unrealized_pnl"] = (
        holding_df["close"] - holding_df["avg_cost"]
    ) * holding_df["quantity"]

    stock_name_map = (
        trades.drop_duplicates("stock_code")
        .set_index("stock_code")["stock_name"]
        .to_dict()
    )
    holding_df["stock_name"] = holding_df["stock_code"].map(stock_name_map)

    prev_date = prev_dates[-2] if len(prev_dates) > 1 else None
    prev_close_map = {}
    if prev_date and prev_date in holdings_by_date:
        prev_holding = holdings_by_date[prev_date]
        if not prev_holding.empty:
            prev_close_map = prev_holding.set_index("stock_code")["close"].to_dict()

    def calc_pct_change(row):
        code = row["stock_code"]
        prev_close = prev_close_map.get(code)
        if prev_close and prev_close != 0:
            return (row["close"] - prev_close) / prev_close * 100
        return None

    holding_df["pct_change"] = holding_df.apply(calc_pct_change, axis=1)

    show_df = holding_df[
        [
            "stock_code",
            "stock_name",
            "quantity",
            "avg_cost",
            "close",
            "pct_change",
            "market_value",
            "weight",
            "unrealized_pnl",
        ]
    ].sort_values("market_value", ascending=False)

    st.dataframe(
        show_df.style.format(
            {
                "avg_cost": "{:.2f}",
                "close": "{:.2f}",
                "pct_change": "{:.2f}%",
                "market_value": "{:,.2f}",
                "weight": "{:.2%}",
                "unrealized_pnl": "{:,.2f}",
            }
        ),
        width="stretch",
    )


def main() -> None:
    st.set_page_config(
        page_title="回测日志收益率重建",
        page_icon="📈",
        layout="wide",
    )

    st.title("回测日志收益率重建")
    st.caption("读取回测日志买卖记录 + cjdata 行情，重建净值曲线与持仓")

    with st.sidebar:
        st.subheader("日志文件")
        uploaded_file = st.file_uploader("上传 .log 文件", type=["log"])

        log_dir = Path("log")
        local_files: list[str] = []
        if log_dir.exists() and log_dir.is_dir():
            local_files = sorted([str(p) for p in log_dir.glob("*.log")], reverse=True)

        selected_local_file = st.selectbox(
            "或选择本地 log/ 文件",
            options=[""] + local_files,
            format_func=lambda x: "(不选择)" if x == "" else Path(x).name,
        )

        st.divider()
        st.write(f"初始本金: {INITIAL_CAPITAL:,.2f} 元")

    log_content, log_name = _read_uploaded_or_local_log(
        uploaded_file, selected_local_file
    )

    if not log_content:
        st.info("请先上传 .log 文件，或在侧边栏选择 `log/` 目录中的日志文件。")
        return

    trades = parse_trade_log(log_content)

    if trades.empty:
        st.error("日志中未解析到成交记录，请检查日志格式。")
        return

    min_date = trades["trade_date"].min()
    max_date = trades["trade_date"].max()
    st.success(f"已加载日志: {log_name}，成交记录 {len(trades)} 条")

    # 为估值多留一个月，便于查询交易结束后日期的持仓市值。
    market_start = min_date.strftime("%Y%m%d")
    market_end = (max_date + pd.Timedelta(days=31)).strftime("%Y%m%d")

    with st.spinner("正在加载行情并重建净值..."):
        market_df = load_market_data(market_start, market_end)
        replay = replay_portfolio(trades, market_df, initial_capital=INITIAL_CAPITAL)

    if replay.equity_curve.empty:
        st.error("未能构建净值曲线：行情数据为空或缺失。")
        return

    curve = replay.equity_curve.copy()

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        total_return = curve.iloc[-1]["return_pct"]
        st.metric("总收益率", f"{total_return:.2f}%")
    with c2:
        final_asset = curve.iloc[-1]["total_asset"]
        st.metric("期末总资产", f"{final_asset:,.2f}")
    with c3:
        max_drawdown = (
            (curve["nav"].cummax() - curve["nav"]) / curve["nav"].cummax()
        ).max()
        st.metric("最大回撤", f"{max_drawdown * 100:.2f}%")
    with c4:
        nav = curve["nav"].values
        cummax = np.maximum.accumulate(nav)
        drawdown_arr = (cummax - nav) / cummax
        max_dd_len = 0
        dd_start = None
        for i in range(len(nav)):
            if nav[i] >= cummax[i]:
                if dd_start is not None:
                    max_dd_len = max(max_dd_len, i - dd_start)
                    dd_start = None
            elif drawdown_arr[i] > 0 and dd_start is None:
                dd_start = i
        if dd_start is not None:
            max_dd_len = max(max_dd_len, len(nav) - dd_start)
        st.metric("最长回撤时间", f"{max_dd_len} 天")
    with c5:
        st.metric("累计佣金", f"{curve.iloc[-1]['commission_cum']:,.2f}")

    fig = px.line(
        curve,
        x="trade_date",
        y="return_pct",
        title="收益率曲线(%)",
        labels={"trade_date": "日期", "return_pct": "收益率(%)"},
    )
    fig.update_layout(height=460)
    st.plotly_chart(fig, width="stretch")

    with st.expander("查看净值明细"):
        st.dataframe(
            curve.style.format(
                {
                    "cash": "{:,.2f}",
                    "market_value": "{:,.2f}",
                    "total_asset": "{:,.2f}",
                    "nav": "{:.4f}",
                    "return_pct": "{:.2f}",
                    "commission_cum": "{:,.2f}",
                }
            ),
            width="stretch",
        )

    st.subheader("日期持仓查询")
    query_default = curve.iloc[-1]["trade_date"].date()
    query_date = st.date_input(
        "输入日期",
        value=query_default,
        min_value=curve.iloc[0]["trade_date"].date(),
        max_value=curve.iloc[-1]["trade_date"].date(),
    )

    _render_holdings_for_date(
        query_date, replay.holdings_by_date, replay.trades, replay.equity_curve
    )

    with st.expander("查看成交明细"):
        st.dataframe(
            replay.trades.style.format(
                {
                    "price": "{:.2f}",
                    "commission": "{:.2f}",
                }
            ),
            width="stretch",
        )


if __name__ == "__main__":
    main()
