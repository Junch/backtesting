import backtrader as bt
from datetime import datetime, timedelta
import warnings
import re

warnings.filterwarnings("ignore")
import sys
import os
import argparse
import streamlit as st
import streamlit.components.v1 as components

from cjdata import LocalData
try:
    from xtquant import xtdata
except Exception:
    xtdata = None
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns

from stock_filters import (
    ListingAgeFilter,
    MarketCapRangeFilter,
    StockFilterContext,
    StockFilterPipeline,
)

# 导入单因子计算器
from single_factor_analysis import (
    FactorCalculator,
    MarketValueFactor,
    MomentumFactor,
    ValueFactor,
    ROEFactor,
    VolatilityFactor,
    TurnoverFactor,
    AVAILABLE_FACTORS,
)

# 导入回测工具类
from backtest_utils import (
    DateStrategy,
    StockTradeAnalyzer,
    plot_strategy_performance,
    get_trading_days,
)


class MultiFactorCalculator:
    """
    多因子计算器类
    支持多个因子的组合计算，用户可以设置每个因子的权重
    """

    def __init__(
        self, name="多因子组合", description="基于多个因子加权组合的股票选择策略"
    ):
        """
        初始化多因子计算器

        Args:
            name: 多因子组合名称
            description: 多因子组合描述
        """
        self.name = name
        self.description = description
        self.factor_calculators: Dict[str, FactorCalculator] = {}
        self.factor_weights: Dict[str, float] = {}
        self.standardize_factors = True  # 是否标准化因子值

    def add_factor(
        self, factor_name: str, factor_calculator: FactorCalculator, weight: float = 1.0
    ):
        """
        添加因子到多因子组合中

        Args:
            factor_name: 因子名称
            factor_calculator: 因子计算器实例
            weight: 因子权重
        """
        self.factor_calculators[factor_name] = factor_calculator
        self.factor_weights[factor_name] = weight

    def remove_factor(self, factor_name: str):
        """
        从多因子组合中移除因子

        Args:
            factor_name: 要移除的因子名称
        """
        if factor_name in self.factor_calculators:
            del self.factor_calculators[factor_name]
            del self.factor_weights[factor_name]

    def update_weight(self, factor_name: str, weight: float):
        """
        更新因子权重

        Args:
            factor_name: 因子名称
            weight: 新的权重值
        """
        if factor_name in self.factor_weights:
            self.factor_weights[factor_name] = weight

    def get_factor_weights(self) -> Dict[str, float]:
        """获取所有因子权重"""
        return self.factor_weights.copy()

    def set_standardize_factors(self, standardize: bool):
        """设置是否标准化因子值"""
        self.standardize_factors = standardize

    def calculate(
        self, df: pd.DataFrame, factor_params: Optional[Dict[str, Dict]] = None
    ) -> pd.DataFrame:
        """
        计算多因子组合值

        Args:
            df: 包含股票数据的DataFrame
            factor_params: 各因子的参数字典，格式为 {factor_name: {param_key: param_value}}

        Returns:
            DataFrame: 添加了各个因子和复合因子列的数据框
        """
        if not self.factor_calculators:
            raise ValueError("没有添加任何因子，请先添加因子")

        df_result = df.copy()
        factor_columns = []

        # 计算每个因子的值
        for factor_name, factor_calculator in self.factor_calculators.items():
            print(f"正在计算因子: {factor_name}")

            # 获取该因子的参数
            params = factor_params.get(factor_name, {}) if factor_params else {}

            # 计算因子值
            df_result = factor_calculator.calculate(df_result, **params)
            factor_col = factor_calculator.get_factor_column()
            factor_columns.append(factor_col)

        # 标准化因子值（可选）
        if self.standardize_factors:
            print("正在标准化因子值...")
            df_result = self._standardize_factors(df_result, factor_columns)

        # 计算复合因子值
        print("正在计算复合因子...")
        df_result = self._calculate_composite_factor(df_result, factor_columns)

        return df_result

    def _standardize_factors(
        self, df: pd.DataFrame, factor_columns: List[str]
    ) -> pd.DataFrame:
        """
        标准化因子值（按交易日标准化）

        Args:
            df: 数据框
            factor_columns: 需要标准化的因子列名列表

        Returns:
            DataFrame: 标准化后的数据框
        """
        df_result = df.copy()

        # 按交易日分组标准化
        for trade_date in df["trade_date"].unique():
            date_mask = df_result["trade_date"] == trade_date
            date_data = df_result.loc[date_mask, factor_columns]

            # 只对有效值进行标准化
            valid_mask = date_data.notna().all(axis=1)
            if valid_mask.sum() > 1:  # 至少需要2个有效样本
                scaler = StandardScaler()
                date_data_valid = date_data[valid_mask]
                standardized_data = pd.DataFrame(
                    scaler.fit_transform(date_data_valid),
                    index=date_data_valid.index,
                    columns=factor_columns,
                )
                df_result.loc[date_mask & valid_mask, factor_columns] = (
                    standardized_data
                )

        return df_result

    def _calculate_composite_factor(
        self, df: pd.DataFrame, factor_columns: List[str]
    ) -> pd.DataFrame:
        """
        计算复合因子值

        Args:
            df: 数据框
            factor_columns: 因子列名列表

        Returns:
            DataFrame: 添加了复合因子的数据框
        """
        df_result = df.copy()

        # 获取权重向量
        weights = []
        weight_factor_names = []
        for factor_name, factor_calculator in self.factor_calculators.items():
            factor_col = factor_calculator.get_factor_column()
            if factor_col in factor_columns:
                weight = self.factor_weights[factor_name]
                if factor_calculator.ascending:
                    weight = -weight
                weights.append(weight)
                weight_factor_names.append(factor_col)

        weights = np.array(weights)

        # 计算加权复合因子
        composite_factor = np.zeros(len(df_result))
        valid_composite_mask = np.ones(len(df_result), dtype=bool)

        for i, factor_col in enumerate(weight_factor_names):
            factor_values = df_result[factor_col].values
            weight = weights[i]

            # 处理缺失值
            valid_factor_mask = ~pd.isna(factor_values)
            valid_composite_mask = valid_composite_mask & valid_factor_mask

            composite_factor += weight * np.nan_to_num(factor_values, nan=0.0)

        # 设置无效值为NaN
        composite_factor[~valid_composite_mask] = np.nan

        df_result["composite_factor"] = composite_factor

        return df_result

    def get_factor_column(self) -> str:
        """返回复合因子列名"""
        return "composite_factor"

    def analyze_factor_correlation(
        self, df: pd.DataFrame, trade_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        分析因子间相关性

        Args:
            df: 包含因子数据的DataFrame
            trade_date: 指定交易日期，如果为None则使用所有数据

        Returns:
            DataFrame: 因子相关性矩阵
        """
        # 获取因子列
        factor_columns = []
        factor_names = []

        for factor_name, factor_calculator in self.factor_calculators.items():
            factor_col = factor_calculator.get_factor_column()
            if factor_col in df.columns:
                factor_columns.append(factor_col)
                factor_names.append(factor_name)

        if not factor_columns:
            return pd.DataFrame()

        # 筛选数据
        if trade_date:
            analysis_df = df[df["trade_date"] == pd.to_datetime(trade_date)]
        else:
            analysis_df = df

        # 计算相关性矩阵
        correlation_matrix = analysis_df[factor_columns].corr()
        correlation_matrix.index = factor_names
        correlation_matrix.columns = factor_names

        return correlation_matrix


def run_multi_factor_backtesting(
    df: pd.DataFrame,
    all_trade_dates: List[datetime],
    multi_factor_calculator: MultiFactorCalculator,
    rebalance_period: int,
    hold_top: int,
    factor_params: Optional[Dict[str, Dict]] = None,
    filter_pipeline: Optional[StockFilterPipeline] = None,
    listed_dates: Optional[pd.Series] = None,
) -> Tuple[set, Dict, Dict]:
    """
    多因子回测数据处理函数

    Args:
        df: 包含多只股票日行情数据的DataFrame
        all_trade_dates: 所有交易日列表
        multi_factor_calculator: 多因子计算器实例
        rebalance_period: 调仓周期
        hold_top: 持有股票数量
        factor_params: 各因子的参数字典

    Returns:
        tuple: (stock_list, buy_dates, sell_dates)
    """
    # 计算多因子值
    df = multi_factor_calculator.calculate(df, factor_params)
    composite_factor_col = multi_factor_calculator.get_factor_column()

    first_trade_dates = (
        df.loc[df["trade_date"].notna(), ["stock_code", "trade_date"]]
        .groupby("stock_code")["trade_date"]
        .min()
    )

    buy_dates = {}
    sell_dates = {}
    position = set()
    stock_list = set()

    len_all_trade_dates = len(all_trade_dates)
    for i in range(0, len_all_trade_dates, rebalance_period):
        str_date = str(all_trade_dates[i].date())

        print(f"处理调仓日: {str_date}")
        if i + 1 >= len_all_trade_dates:
            break

        # 仅使用当日可见信息生成信号，避免读取下一交易日数据导致前视偏差
        valid_stocks = df[
            (df["trade_date"] == all_trade_dates[i])
            & (df[composite_factor_col].notna())
        ]  # 过滤掉复合因子值为空的股票

        if filter_pipeline:
            filter_context = StockFilterContext(
                trade_date=all_trade_dates[i],
                universe_df=df,
                listed_dates=listed_dates,
                first_trade_dates=first_trade_dates,
            )
            valid_stocks = filter_pipeline.apply(valid_stocks, filter_context)

        if not valid_stocks.empty:
            # 复合因子按降序排列（高值优先）
            selected_stocks = (
                valid_stocks.sort_values(by=composite_factor_col, ascending=False)
                .head(hold_top)["stock_code"]
                .tolist()
            )
        else:
            selected_stocks = []

        buy_dates[str_date] = selected_stocks

        sell_list = list(position - set(buy_dates[str_date]))
        sell_dates[str_date] = sell_list
        position = set(buy_dates[str_date])
        stock_list = stock_list.union(position)

    return (stock_list, buy_dates, sell_dates)


def plot_factor_analysis(
    df: pd.DataFrame, multi_factor_calculator: MultiFactorCalculator, trade_date: str
) -> Tuple[go.Figure, go.Figure]:
    """
    绘制因子分析图表

    Args:
        df: 包含因子数据的DataFrame
        multi_factor_calculator: 多因子计算器
        trade_date: 分析日期

    Returns:
        Tuple[go.Figure, go.Figure]: (相关性热力图, 因子权重分布图)
    """
    # 因子相关性分析
    correlation_matrix = multi_factor_calculator.analyze_factor_correlation(
        df, trade_date
    )

    # 创建相关性热力图
    fig_corr = go.Figure(
        data=go.Heatmap(
            z=correlation_matrix.values,
            x=correlation_matrix.columns,
            y=correlation_matrix.index,
            colorscale="RdBu",
            zmid=0,
            text=correlation_matrix.values,
            texttemplate="%{text:.2f}",
            textfont={"size": 10},
            colorbar=dict(title="相关系数"),
        )
    )

    fig_corr.update_layout(
        title=f"因子相关性分析 ({trade_date})",
        xaxis_title="因子",
        yaxis_title="因子",
        height=500,
    )

    # 创建因子权重分布图
    weights = multi_factor_calculator.get_factor_weights()
    factor_names = list(weights.keys())
    weight_values = list(weights.values())

    fig_weights = go.Figure(
        data=[
            go.Bar(
                x=factor_names,
                y=weight_values,
                marker_color="lightblue",
                text=weight_values,
                texttemplate="%{text:.2f}",
                textposition="outside",
            )
        ]
    )

    fig_weights.update_layout(
        title="因子权重分布", xaxis_title="因子", yaxis_title="权重", height=400
    )

    return fig_corr, fig_weights


def _resolve_signal_date(trading_days: List[pd.Timestamp], target_date: datetime) -> Optional[pd.Timestamp]:
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
) -> List[str]:
    """将候选股转换为 order.txt 格式行（无表头）。"""
    lines: List[str] = []
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
        lines.append(f"{date_text} 买入 {stock_code} {stock_name} {quantity}")

    return lines


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


# --- Streamlit 应用主程序 ---
def main():
    st.set_page_config(
        page_title="多因子量化策略回测系统", page_icon="🔬", layout="wide"
    )

    st.title("🔬 多因子量化策略系统")
    st.markdown("同一套多因子参数支持回测分析与盘后选股")

    # 侧边栏参数配置
    st.sidebar.header("多因子策略配置")

    # 因子选择区域
    st.sidebar.subheader("📊 因子选择与权重设置")

    # 创建多因子计算器
    multi_factor_calculator = MultiFactorCalculator()

    # 可用因子列表
    factor_names = list(AVAILABLE_FACTORS.keys())

    # 因子选择和权重设置
    selected_factors = {}
    factor_params = {}

    st.sidebar.write("**选择要使用的因子：**")

    for factor_name in factor_names:
        # 因子选择复选框
        factor_selected = st.sidebar.checkbox(
            f"{factor_name}",
            key=f"select_{factor_name}",
            help=AVAILABLE_FACTORS[factor_name].description,
        )

        if factor_selected:
            # 权重设置
            weight = st.sidebar.slider(
                f"{factor_name} 权重",
                min_value=0.1,
                max_value=3.0,
                value=1.0,
                step=0.1,
                key=f"weight_{factor_name}",
                help=f"设置 {factor_name} 在复合因子中的权重",
            )

            selected_factors[factor_name] = weight

            # 添加因子到多因子计算器
            multi_factor_calculator.add_factor(
                factor_name, AVAILABLE_FACTORS[factor_name], weight
            )

            # 因子特定参数设置
            st.sidebar.write(f"**{factor_name} 参数：**")

            if factor_name == "市值因子":
                market_option = st.sidebar.selectbox(
                    "市值计算方式",
                    ["总市值", "流通市值"],
                    index=0,
                    key=f"market_option_{factor_name}",
                )
                smooth_window = st.sidebar.slider(
                    "市值平滑窗口（交易日）",
                    min_value=1,
                    max_value=20,
                    value=7,
                    key=f"smooth_window_{factor_name}",
                )
                factor_params[factor_name] = {
                    "market_option": market_option,
                    "smooth_window": smooth_window,
                }

            elif factor_name == "动量因子":
                momentum_period = st.sidebar.slider(
                    "动量回看期（交易日）",
                    min_value=5,
                    max_value=60,
                    value=21,
                    key=f"momentum_period_{factor_name}",
                )
                factor_params[factor_name] = {"momentum_period": momentum_period}

            elif factor_name == "波动率因子":
                volatility_period = st.sidebar.slider(
                    "波动率计算周期（交易日）",
                    min_value=5,
                    max_value=60,
                    value=20,
                    key=f"volatility_period_{factor_name}",
                )
                factor_params[factor_name] = {"volatility_period": volatility_period}

            elif factor_name == "换手率因子":
                turnover_period = st.sidebar.slider(
                    "换手率平滑周期（交易日）",
                    min_value=5,
                    max_value=60,
                    value=20,
                    key=f"turnover_period_{factor_name}",
                )
                factor_params[factor_name] = {"turnover_period": turnover_period}

    # 检查是否至少选择了一个因子
    if not selected_factors:
        st.sidebar.error("⚠️ 请至少选择一个因子！")
        st.warning("请在左侧边栏中选择至少一个因子来构建多因子模型。")
        return

    # 显示选中的因子权重
    st.sidebar.write("**当前因子配置：**")
    total_weight = sum(selected_factors.values())

    for factor_name, weight in selected_factors.items():
        weight_pct = (weight / total_weight) * 100
        st.sidebar.write(f"- {factor_name}: {weight:.1f} ({weight_pct:.1f}%)")

    st.sidebar.write(f"**总权重：** {total_weight:.1f}")

    # 其他参数设置
    st.sidebar.subheader("📈 参数设置")

    # 板块选择
    sector_options = {
        "上证50": "000016.SH",
        "科创50": "000688.SH",
        "沪深300": "000300.SH",
        "中证500": "000905.SH",
        "中证1000": "000852.SH",
        "创业板": "399006.SZ",
    }
    sector_names = list(sector_options.keys())
    sector_name = st.sidebar.selectbox("选择板块", sector_names, index=2)

    # 日期选择
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("开始日期", value=datetime(2021, 1, 1))
    with col2:
        end_date = st.date_input("结束日期", value=datetime(2021, 12, 31))

    # 基础策略参数
    rebalance_period = st.sidebar.slider(
        "调仓周期（交易日）", min_value=5, max_value=60, value=21
    )
    hold_top = st.sidebar.slider("持有股票数量", min_value=5, max_value=30, value=10)

    # 前置过滤参数（可扩展）
    st.sidebar.subheader("前置过滤")
    filter_pipeline = StockFilterPipeline()

    enable_market_cap_filter = st.sidebar.checkbox("启用市值范围过滤", value=False)
    if enable_market_cap_filter:
        col_mc1, col_mc2 = st.sidebar.columns(2)
        with col_mc1:
            min_market_cap = st.number_input(
                "最小市值(亿元)", min_value=0.0, value=0.0, step=10.0
            )
        with col_mc2:
            max_market_cap = st.number_input(
                "最大市值(亿元)", min_value=0.0, value=2000.0, step=10.0
            )
        if max_market_cap < min_market_cap:
            st.sidebar.warning("最大市值小于最小市值，将自动交换两者")
            min_market_cap, max_market_cap = max_market_cap, min_market_cap
        filter_pipeline.add_filter(
            MarketCapRangeFilter(min_cap=min_market_cap, max_cap=max_market_cap)
        )

    listing_min_days = None
    enable_listing_age_filter = st.sidebar.checkbox("启用上市时长过滤", value=False)
    if enable_listing_age_filter:
        listing_days_option = st.sidebar.selectbox(
            "最短上市时长（自然日）",
            options=[365, 730, 1095, 1460],
            index=0,
            help="365约等于1年，730约等于2年",
        )
        listing_min_days = int(listing_days_option)
        filter_pipeline.add_filter(ListingAgeFilter(min_days=listing_min_days))

    # 因子标准化选项
    standardize_factors = st.sidebar.checkbox(
        "标准化因子值", value=True, help="对因子值进行标准化处理，消除量纲影响"
    )
    multi_factor_calculator.set_standardize_factors(standardize_factors)

    # 风险控制参数（回测页使用）
    st.sidebar.subheader("🛡️ 风险控制")
    enable_stop_loss = st.sidebar.checkbox("启用止损", value=False)
    stop_loss_pct = None
    trailing_stop = False

    if enable_stop_loss:
        stop_loss_pct = (
            st.sidebar.slider(
                "止损比例 (%)", min_value=1.0, max_value=20.0, value=10.0, step=0.5
            )
            / 100
        )
        trailing_stop = st.sidebar.checkbox("启用移动止损", value=False)

        if trailing_stop:
            st.sidebar.info("移动止损：从最高价回撤超过止损比例时卖出")
        else:
            st.sidebar.info("固定止损：从买入价下跌超过止损比例时卖出")

    st.sidebar.caption(
        "执行规则: t日生成调仓信号, 下一根K线执行。执行日若停牌/不可交易则跳过, 且不补单。"
    )

    tab_backtest, tab_picker = st.tabs(["📈 回测分析", "🌓 盘后选股"])

    with tab_backtest:
        # 主界面展示区域
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("🎯 多因子策略概览")

            strategy_info = f"""
            **策略名称：** {multi_factor_calculator.name}

            **选中因子：** {len(selected_factors)} 个

            **因子权重配置：**
            """

            for factor_name, weight in selected_factors.items():
                weight_pct = (weight / total_weight) * 100
                strategy_info += f"\n- **{factor_name}**: {weight:.1f} ({weight_pct:.1f}%)"

            strategy_info += f"""

            **标准化处理：** {"是" if standardize_factors else "否"}

            **板块范围：** {sector_name}

            **回测期间：** {start_date} 至 {end_date}
            """

            st.markdown(strategy_info)

        with col2:
            st.subheader("📋 策略参数")
            st.write(f"**调仓周期：** {rebalance_period} 个交易日")
            st.write(f"**持仓数量：** {hold_top} 只股票")
            st.write(f"**手续费率：** 0.1%")
            if enable_stop_loss and stop_loss_pct is not None:
                st.write(
                    f"**止损设置：** {stop_loss_pct * 100:.1f}% ({'移动' if trailing_stop else '固定'})"
                )
            else:
                st.write("**止损设置：** 未启用")

        if st.button("🚀 开始多因子回测", type="primary", width="stretch"):
            start_str = start_date.strftime("%Y%m%d")
            end_str = end_date.strftime("%Y%m%d")

            with st.spinner("正在运行多因子回测..."):
                try:
                    cerebro = bt.Cerebro()
                    cerebro.broker.setcash(1000000.0)
                    cerebro.broker.setcommission(commission=0.001)
                    cerebro.addobserver(bt.observers.Value)

                    st.info(f"正在加载{sector_name}板块股票数据...")
                    db_path = os.environ.get(
                        "STOCK_DATA_DB", "C:/github/cjdata/data/stock_data_hfq.db"
                    )
                    findata = LocalData(db_path)

                    pre_lookback_days = 60
                    if listing_min_days is not None:
                        pre_lookback_days = max(pre_lookback_days, listing_min_days + 30)

                    start_ts = pd.Timestamp(start_date)
                    pre_start_date = (
                        start_ts - pd.Timedelta(days=pre_lookback_days)
                    ).strftime("%Y%m%d")
                    df = findata.get_stock_data_frame_in_sector(
                        sector_name, pre_start_date, end_str, adj="hfq"
                    )

                    listed_dates = None
                    stock_name_map: Dict[str, str] = {}
                    basic_df = findata.get_stock_basic_by_sector(sector_name)
                    if isinstance(basic_df, pd.DataFrame) and not basic_df.empty:
                        if {
                            "stock_code",
                            "listed_date",
                        }.issubset(basic_df.columns) and enable_listing_age_filter:
                            listed_dates = (
                                basic_df.dropna(subset=["stock_code"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["listed_date"]
                            )
                            listed_dates = pd.Series(listed_dates)

                        name_col = None
                        for candidate_col in ["stock_name", "name", "sec_name"]:
                            if candidate_col in basic_df.columns:
                                name_col = candidate_col
                                break
                        if name_col is not None and "stock_code" in basic_df.columns:
                            stock_name_map = (
                                basic_df.dropna(subset=["stock_code", name_col])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")[name_col]
                                .astype(str)
                                .to_dict()
                            )

                    all_trade_dates = get_trading_days(df, start_date)

                    stock_list, buy_dates, sell_dates = run_multi_factor_backtesting(
                        df,
                        all_trade_dates,
                        multi_factor_calculator,
                        rebalance_period,
                        hold_top,
                        factor_params,
                        filter_pipeline=filter_pipeline if filter_pipeline else None,
                        listed_dates=listed_dates,
                    )

                    if not stock_list:
                        st.error("未能加载任何股票数据，请检查数据源或调整日期范围")
                        return

                    for stock in stock_list:
                        df_stock = df[
                            (df["stock_code"] == stock)
                            & (df["trade_date"].isin(all_trade_dates))
                        ].set_index("trade_date")
                        data = bt.feeds.PandasData(
                            dataname=df_stock,
                            datetime=None,
                            open="open",
                            high="high",
                            low="low",
                            close="close",
                            volume="volume",
                            openinterest=None,
                        )
                        data._name = stock
                        cerebro.adddata(data)

                    st.success(f"成功加载 {len(stock_list)} 只股票")

                    cerebro.addstrategy(
                        DateStrategy,
                        data_source=findata,
                        buy_dates=buy_dates,
                        sell_dates=sell_dates,
                        stop_loss_pct=stop_loss_pct,
                        trailing_stop=trailing_stop,
                        log_file=None,
                    )
                    cerebro.addanalyzer(
                        StockTradeAnalyzer,
                        _name="stock_trade_analyzer",
                        data_source=findata,
                    )
                    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe_ratio")
                    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
                    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

                    initial_value = cerebro.broker.getvalue()
                    st.info(f"初始资金: {initial_value:,.2f} 元")

                    results = cerebro.run()
                    final_value = cerebro.broker.getvalue()

                    st.success(f"回测完成！最终资金: {final_value:,.2f} 元")

                    strat = results[0]
                    trade_analyzer = strat.analyzers.stock_trade_analyzer

                    st.subheader("📊 多因子策略性能指标")

                    col1, col2, col3, col4, col5 = st.columns(5)

                    with col1:
                        total_return = (final_value / initial_value - 1) * 100
                        st.metric("总收益率", f"{total_return:.2f}%")

                    with col2:
                        try:
                            sharpe = strat.analyzers.sharpe_ratio.get_analysis()[
                                "sharperatio"
                            ]
                            if sharpe is None:
                                sharpe = 0.0
                            st.metric("夏普比率", f"{sharpe:.3f}")
                        except Exception:
                            st.metric("夏普比率", "N/A")

                    with col3:
                        try:
                            max_dd = strat.analyzers.drawdown.get_analysis()["max"][
                                "drawdown"
                            ]
                            st.metric("最大回撤", f"{max_dd:.2f}%")
                        except Exception:
                            st.metric("最大回撤", "N/A")

                    with col4:
                        try:
                            max_dd_len = strat.analyzers.drawdown.get_analysis()["max"][
                                "len"
                            ]
                            st.metric("最长回撤时间", f"{max_dd_len} 天")
                        except Exception:
                            st.metric("最长回撤时间", "N/A")

                    with col5:
                        try:
                            annual_return = strat.analyzers.returns.get_analysis()[
                                "rnorm100"
                            ]
                            st.metric("年化收益率", f"{annual_return:.2f}%")
                        except Exception:
                            st.metric("年化收益率", "N/A")

                    st.subheader("📈 策略收益曲线")
                    sector_code = sector_options[sector_name]
                    pre_start_str = all_trade_dates[0].strftime("%Y%m%d")
                    chart = plot_strategy_performance(
                        findata, trade_analyzer, pre_start_str, end_str, sector_code
                    )

                    if chart:
                        chart_html = chart.render_embed()
                        components.html(chart_html, height=650)

                    st.subheader("🔬 因子分析")

                    mid_date_idx = len(all_trade_dates) // 2
                    analysis_date = all_trade_dates[mid_date_idx].strftime("%Y-%m-%d")

                    try:
                        df_analysis = multi_factor_calculator.calculate(df, factor_params)

                        fig_corr, fig_weights = plot_factor_analysis(
                            df_analysis, multi_factor_calculator, analysis_date
                        )

                        col1, col2 = st.columns(2)

                        with col1:
                            st.plotly_chart(fig_corr, width="stretch")

                        with col2:
                            st.plotly_chart(fig_weights, width="stretch")

                    except Exception as e:
                        st.warning(f"因子分析图表生成失败: {str(e)}")

                    with st.expander("📋 详细分析结果"):
                        st.write("**多因子策略参数:**")
                        st.write(f"- 策略名称: {multi_factor_calculator.name}")
                        st.write(f"- 选中因子: {', '.join(selected_factors.keys())}")
                        st.write(f"- 板块: {sector_name}")
                        st.write(f"- 回测期间: {start_str} - {end_str}")
                        st.write(f"- 调仓周期: {rebalance_period} 个交易日")
                        st.write(f"- 持有股票数量: {hold_top} 只")
                        st.write(f"- 手续费率: 0.1%")
                        st.write(f"- 因子标准化: {'是' if standardize_factors else '否'}")
                        st.write(
                            "- 执行规则: t日生成信号, 下一根K线执行; 执行日不可交易则跳过且不补单"
                        )

                        active_filters = filter_pipeline.get_filter_descriptions()
                        if active_filters:
                            st.write("**前置过滤条件:**")
                            for filter_desc in active_filters:
                                st.write(f"- {filter_desc}")
                            if enable_listing_age_filter:
                                st.write(
                                    "- 上市日期来源: stock_basic.listed_date(缺失时回退首个交易日)"
                                )
                        else:
                            st.write("- 前置过滤: 未启用")

                        st.write("**因子权重配置:**")
                        for factor_name, weight in selected_factors.items():
                            weight_pct = (weight / total_weight) * 100
                            st.write(f"- {factor_name}: {weight:.1f} ({weight_pct:.1f}%)")

                        if factor_params:
                            st.write("**因子特定参数:**")
                            for factor_name, params in factor_params.items():
                                st.write(f"- {factor_name}:")
                                for key, value in params.items():
                                    st.write(f"  - {key}: {value}")

                        if enable_stop_loss and stop_loss_pct is not None:
                            st.write(f"- 止损比例: {stop_loss_pct * 100:.1f}%")
                            st.write(
                                f"- 止损类型: {'移动止损' if trailing_stop else '固定止损'}"
                            )
                        else:
                            st.write("- 止损: 未启用")

                        st.write("**回测统计:**")
                        st.write(f"- 加载股票数量: {len(stock_list)} 只")
                        st.write(f"- 初始资金: {initial_value:,.2f} 元")
                        st.write(f"- 最终资金: {final_value:,.2f} 元")
                        st.write(f"- 绝对收益: {final_value - initial_value:,.2f} 元")

                        analysis_result = trade_analyzer.get_analysis()
                        if (
                            analysis_result["stock_analysis_df"] is not None
                            and not analysis_result["stock_analysis_df"].empty
                        ):
                            st.write("**📈 股票交易分析数据:**")
                            st.dataframe(
                                analysis_result["stock_analysis_df"].style.format(
                                    {
                                        "买入总额": "{:,.2f}",
                                        "卖出总额": "{:,.2f}",
                                        "当前持仓市值": "{:,.2f}",
                                        "交易佣金": "{:.2f}",
                                        "净盈亏": "{:,.2f}",
                                        "收益率(%)": "{:.2f}%",
                                    }
                                ),
                                width="stretch",
                            )

                            if analysis_result["summary_data"] is not None:
                                st.write("**📊 回测汇总数据:**")
                                summary_df = pd.DataFrame([analysis_result["summary_data"]])
                                st.dataframe(
                                    summary_df.style.format(
                                        {
                                            "初始资金": "{:,.2f}",
                                            "最终资金": "{:,.2f}",
                                            "总盈亏": "{:,.2f}",
                                            "总收益率(%)": "{:.2f}%",
                                            "总佣金": "{:.2f}",
                                            "股票交易盈亏合计": "{:,.2f}",
                                        }
                                    ),
                                    width="stretch",
                                )

                except Exception as e:
                    st.error(f"多因子回测过程中发生错误: {str(e)}")
                    st.exception(e)

    with tab_picker:
        st.subheader("🌓 盘后选股")
        st.caption("按指定日期计算多因子得分并排序，使用 xtdata 实时价格计算次日买入清单")

        picker_col1, picker_col2, picker_col3, picker_col4 = st.columns(4)
        with picker_col1:
            pick_date = st.date_input(
                "选股日期",
                value=end_date,
                min_value=start_date,
                max_value=end_date,
                key="pick_trade_date",
            )
        with picker_col2:
            top_n = st.number_input(
                "展示候选数量",
                min_value=1,
                max_value=100,
                value=20,
                step=1,
                key="picker_top_n",
            )
        with picker_col3:
            total_capital = st.number_input(
                "总资金(元)",
                min_value=1000.0,
                max_value=100000000.0,
                value=240000.0,
                step=10000.0,
                key="picker_total_capital",
                help="总资金将按购买股票数量平均分配",
                format="%.2f",
            )
        with picker_col4:
            buy_count = st.number_input(
                "购买股票数量",
                min_value=1,
                max_value=100,
                value=12,
                step=1,
                key="picker_buy_count",
            )

        st.caption("数量计算规则：单票预算 = 总资金 / 购买股票数量；下单数量按100股向下取整，剩余资金保留")

        run_picker = st.button("🔎 生成盘后候选与次日订单", type="primary", width="stretch")

        if run_picker:
            with st.spinner("正在计算盘后选股结果..."):
                try:
                    db_path = os.environ.get(
                        "STOCK_DATA_DB", "C:/github/cjdata/data/stock_data_hfq.db"
                    )
                    findata = LocalData(db_path)

                    pre_lookback_days = 60
                    if listing_min_days is not None:
                        pre_lookback_days = max(pre_lookback_days, listing_min_days + 30)

                    pick_ts = pd.Timestamp(pick_date)
                    pre_start_date = (
                        pick_ts - pd.Timedelta(days=pre_lookback_days)
                    ).strftime("%Y%m%d")
                    post_end_date = (pick_ts + pd.Timedelta(days=10)).strftime("%Y%m%d")

                    df = findata.get_stock_data_frame_in_sector(
                        sector_name,
                        pre_start_date,
                        post_end_date,
                        adj="hfq",
                    )

                    if not isinstance(df, pd.DataFrame) or df.empty:
                        st.error("未能加载股票数据，请检查日期或数据源")
                        return

                    listed_dates = None
                    stock_name_map: Dict[str, str] = {}
                    basic_df = findata.get_stock_basic_by_sector(sector_name)
                    if isinstance(basic_df, pd.DataFrame) and not basic_df.empty:
                        if enable_listing_age_filter and {
                            "stock_code",
                            "listed_date",
                        }.issubset(basic_df.columns):
                            listed_dates = (
                                basic_df.dropna(subset=["stock_code"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["listed_date"]
                            )
                            listed_dates = pd.Series(listed_dates)

                        name_col = None
                        for candidate_col in ["stock_name", "name", "sec_name"]:
                            if candidate_col in basic_df.columns:
                                name_col = candidate_col
                                break
                        if name_col is not None and "stock_code" in basic_df.columns:
                            stock_name_map = (
                                basic_df.dropna(subset=["stock_code", name_col])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")[name_col]
                                .astype(str)
                                .to_dict()
                            )

                    df = multi_factor_calculator.calculate(df, factor_params)
                    composite_col = multi_factor_calculator.get_factor_column()

                    trading_days = sorted(pd.to_datetime(df["trade_date"].dropna().unique()))
                    signal_date = _resolve_signal_date(trading_days, pick_date)

                    if signal_date is None:
                        st.error("选股日期之前没有可用交易日数据")
                        return

                    day_df = df[
                        (df["trade_date"] == signal_date) & (df[composite_col].notna())
                    ].copy()

                    first_trade_dates = (
                        df.loc[df["trade_date"].notna(), ["stock_code", "trade_date"]]
                        .groupby("stock_code")["trade_date"]
                        .min()
                    )

                    if filter_pipeline:
                        filter_context = StockFilterContext(
                            trade_date=signal_date,
                            universe_df=df,
                            listed_dates=listed_dates,
                            first_trade_dates=first_trade_dates,
                        )
                        day_df = filter_pipeline.apply(day_df, filter_context)

                    if day_df.empty:
                        st.warning("过滤后无可选股票，请调整参数")
                        return

                    ranked_df = day_df.sort_values(by=composite_col, ascending=False).head(
                        int(top_n)
                    )
                    ranked_df = ranked_df.copy()
                    if "stock_name" not in ranked_df.columns:
                        ranked_df["stock_name"] = ranked_df["stock_code"].map(stock_name_map)
                    else:
                        ranked_df["stock_name"] = ranked_df["stock_name"].fillna(
                            ranked_df["stock_code"].map(stock_name_map)
                        )
                    ranked_df["stock_name"] = ranked_df["stock_name"].fillna(
                        ranked_df["stock_code"]
                    )
                    ranked_df.insert(0, "rank", np.arange(1, len(ranked_df) + 1))

                    # 当日市值(亿元): market_cap > market > amount/turn/1e6
                    if "market_cap" in ranked_df.columns:
                        ranked_df["当日市值(亿元)"] = pd.to_numeric(
                            ranked_df["market_cap"], errors="coerce"
                        )
                    elif "market" in ranked_df.columns:
                        ranked_df["当日市值(亿元)"] = pd.to_numeric(
                            ranked_df["market"], errors="coerce"
                        )
                    elif "amount" in ranked_df.columns and "turn" in ranked_df.columns:
                        turn = pd.to_numeric(ranked_df["turn"], errors="coerce").replace(
                            0, np.nan
                        )
                        amount = pd.to_numeric(ranked_df["amount"], errors="coerce")
                        ranked_df["当日市值(亿元)"] = amount / turn / 1e6
                    else:
                        ranked_df["当日市值(亿元)"] = np.nan

                    # PE: peTTM > pe
                    if "peTTM" in ranked_df.columns:
                        ranked_df["PE(TTM)"] = pd.to_numeric(
                            ranked_df["peTTM"], errors="coerce"
                        )
                    elif "pe" in ranked_df.columns:
                        ranked_df["PE(TTM)"] = pd.to_numeric(
                            ranked_df["pe"], errors="coerce"
                        )
                    else:
                        ranked_df["PE(TTM)"] = np.nan

                    display_columns = [
                        "rank",
                        "stock_code",
                        "stock_name",
                        "当日市值(亿元)",
                        "PE(TTM)",
                    ]

                    factor_columns = [
                        calc.get_factor_column()
                        for calc in multi_factor_calculator.factor_calculators.values()
                        if calc.get_factor_column() in ranked_df.columns
                    ]

                    display_columns.append(composite_col)
                    display_columns.extend(factor_columns)
                    display_columns = list(dict.fromkeys(display_columns))

                    st.success(
                        f"信号日 {signal_date.strftime('%Y-%m-%d')} 选出 {len(ranked_df)} 只候选股票"
                    )

                    if xtdata is None:
                        st.error("xtdata 不可用，无法按实时价格计算下单数量。请在 miniQMT 环境运行。")
                        return

                    target_buy_count = int(buy_count)
                    if target_buy_count > len(ranked_df):
                        st.warning(
                            f"候选仅 {len(ranked_df)} 只，实际按 {len(ranked_df)} 只尝试下单"
                        )
                    if target_buy_count > int(top_n):
                        st.warning(
                            f"购买股票数量({target_buy_count}) 大于展示候选数量({int(top_n)})，仅能在候选范围内下单"
                        )

                    order_pool_count = min(target_buy_count, len(ranked_df))
                    order_pool_df = ranked_df.head(order_pool_count).copy()

                    realtime_prices, price_sources, quote_errors = _fetch_realtime_prices(
                        order_pool_df["stock_code"].tolist()
                    )
                    if quote_errors:
                        with st.expander("实时价格获取详情"):
                            for err in quote_errors:
                                st.write(f"- {err}")

                    allocated_df = _calculate_allocated_quantities(
                        order_pool_df,
                        total_capital=float(total_capital),
                        buy_count=target_buy_count,
                        realtime_prices=realtime_prices,
                        price_sources=price_sources,
                    )

                    st.dataframe(ranked_df[display_columns], width="stretch")

                    st.subheader("🧾 次日买入清单预览")
                    order_display_columns = [
                        "rank",
                        "stock_code",
                        "stock_name",
                        "实时价格",
                        "价格来源",
                        "单票预算(元)",
                        "下单数量(股)",
                        "预计下单金额(元)",
                        "下单备注",
                    ]
                    st.dataframe(allocated_df[order_display_columns], width="stretch")

                    next_trade_date = _get_next_trade_date(trading_days, signal_date)

                    executable_df = allocated_df[
                        (allocated_df["下单数量(股)"] > 0)
                        & allocated_df["stock_code"].astype(str).str.match(
                            r"^\d{6}\.(SH|SZ|BJ)$"
                        )
                    ].copy()

                    order_lines = _build_order_lines(
                        executable_df,
                        next_trade_date,
                    )

                    if not order_lines:
                        st.error("未生成可下单记录，请检查实时价格或资金参数")
                        return

                    total_allocated_amount = float(executable_df["预计下单金额(元)"].sum())
                    remaining_amount = float(total_capital) - total_allocated_amount

                    st.info(
                        "总资金: "
                        f"{float(total_capital):,.2f} 元 | "
                        f"计划买入: {target_buy_count} 只 | "
                        f"可下单: {len(executable_df)} 只 | "
                        f"预计占用: {total_allocated_amount:,.2f} 元 | "
                        f"剩余资金: {remaining_amount:,.2f} 元"
                    )

                    order_content = "\n".join(order_lines)
                    order_file_path = os.path.join(os.path.dirname(__file__), "order.txt")
                    with open(order_file_path, "w", encoding="utf-8") as f:
                        f.write(order_content + "\n")

                    st.info(
                        f"已生成次日买入清单，日期: {next_trade_date.strftime('%Y-%m-%d')}，共 {len(order_lines)} 条"
                    )
                    st.code(order_content, language="text")
                    st.download_button(
                        "⬇️ 下载 order.txt",
                        data=order_content.encode("utf-8"),
                        file_name="order.txt",
                        mime="text/plain",
                        width="stretch",
                    )

                except Exception as e:
                    st.error(f"盘后选股过程中发生错误: {str(e)}")
                    st.exception(e)


if __name__ == "__main__":
    main()
