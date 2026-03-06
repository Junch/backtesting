import backtrader as bt
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")
import sys
import os
import argparse
import streamlit as st
import streamlit.components.v1 as components

from cjdata import LocalData
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns

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
                # 考虑因子的排序方向，如果是升序排列（小值更好），权重为正；
                # 如果是降序排列（大值更好），权重也为正，因为我们要让好的股票得到更高的复合因子值
                # 但为了统一排序方向，我们需要调整权重符号
                weight = self.factor_weights[factor_name]
                if factor_calculator.ascending:
                    # 升序因子（小值更好），需要取负权重，这样小值对应更大的复合因子值
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

        # 获取下一交易日停牌的股票代码
        date_next = all_trade_dates[i + 1].date()
        next_day_df = df[df["trade_date"] == date_next][["stock_code", "volume"]]
        halted_stocks = next_day_df[next_day_df["volume"].isna()]["stock_code"].tolist()

        # 选择下一交易日可以交易的股票
        valid_stocks = df[
            (df["trade_date"] == all_trade_dates[i])
            & (df["stock_code"].isin(halted_stocks) == False)
            & (df[composite_factor_col].notna())
        ]  # 过滤掉复合因子值为空的股票

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


# --- Streamlit 应用主程序 ---
def main():
    st.set_page_config(
        page_title="多因子量化策略回测系统", page_icon="🔬", layout="wide"
    )

    st.title("🔬 多因子量化策略回测系统")
    st.markdown("基于多个因子加权组合的股票选择策略回测分析")

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
    st.sidebar.subheader("📈 回测参数")

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

    # 因子标准化选项
    standardize_factors = st.sidebar.checkbox(
        "标准化因子值", value=True, help="对因子值进行标准化处理，消除量纲影响"
    )
    multi_factor_calculator.set_standardize_factors(standardize_factors)

    # 风险控制参数
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

    # 主界面展示区域
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🎯 多因子策略概览")

        # 显示策略信息
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

    # 运行回测按钮
    if st.button("🚀 开始多因子回测", type="primary", width="stretch"):
        # 转换日期格式
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        with st.spinner("正在运行多因子回测..."):
            try:
                # 创建回测引擎
                cerebro = bt.Cerebro()
                cerebro.broker.setcash(1000000.0)
                cerebro.broker.setcommission(commission=0.001)
                cerebro.addobserver(bt.observers.Value)

                # 加载股票数据
                st.info(f"正在加载{sector_name}板块股票数据...")
                findata = LocalData("C:/github/cjdata/data/stock_data_hfq.db")
                # 取start_date前60个交易日的数据用于计算因子
                pre_start_date = (start_date - pd.Timedelta(days=60)).strftime("%Y%m%d")
                df = findata.get_stock_data_frame_in_sector(
                    sector_name, pre_start_date, end_str, adj="hfq"
                )
                all_trade_dates = get_trading_days(df, start_date)

                # 运行多因子回测
                stock_list, buy_dates, sell_dates = run_multi_factor_backtesting(
                    df,
                    all_trade_dates,
                    multi_factor_calculator,
                    rebalance_period,
                    hold_top,
                    factor_params,
                )

                if not stock_list:
                    st.error("未能加载任何股票数据，请检查数据源或调整日期范围")
                    return

                # 添加股票数据到回测引擎
                for stock in stock_list:
                    df_stock = (
                        df[
                            (df["stock_code"] == stock)
                            & (df["trade_date"].isin(all_trade_dates))
                        ]
                        .bfill()
                        .set_index("trade_date")
                    )
                    # 确保数据列名符合backtrader的要求
                    data = bt.feeds.PandasData(
                        dataname=df_stock,
                        datetime=None,  # 使用索引作为日期
                        open="open",
                        high="high",
                        low="low",
                        close="close",
                        volume="volume",
                        openinterest=None,
                    )
                    data._name = stock  # 设置数据源名称
                    cerebro.adddata(data)

                st.success(f"成功加载 {len(stock_list)} 只股票")

                # 添加策略和分析器
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

                # 运行回测
                initial_value = cerebro.broker.getvalue()
                st.info(f"初始资金: {initial_value:,.2f} 元")

                results = cerebro.run()
                final_value = cerebro.broker.getvalue()

                # 显示回测结果
                st.success(f"回测完成！最终资金: {final_value:,.2f} 元")

                # 获取策略实例和分析结果
                strat = results[0]

                # 获取交易分析器
                trade_analyzer = strat.analyzers.stock_trade_analyzer

                # 显示性能指标
                st.subheader("📊 多因子策略性能指标")

                col1, col2, col3, col4 = st.columns(4)

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
                    except:
                        st.metric("夏普比率", "N/A")

                with col3:
                    try:
                        max_dd = strat.analyzers.drawdown.get_analysis()["max"][
                            "drawdown"
                        ]
                        st.metric("最大回撤", f"{max_dd:.2f}%")
                    except:
                        st.metric("最大回撤", "N/A")

                with col4:
                    try:
                        annual_return = strat.analyzers.returns.get_analysis()[
                            "rnorm100"
                        ]
                        st.metric("年化收益率", f"{annual_return:.2f}%")
                    except:
                        st.metric("年化收益率", "N/A")

                # 绘制收益曲线
                st.subheader("📈 策略收益曲线")
                sector_code = sector_options[sector_name]
                pre_start_str = all_trade_dates[0].strftime("%Y%m%d")
                chart = plot_strategy_performance(
                    findata, trade_analyzer, pre_start_str, end_str, sector_code
                )

                if chart:
                    # 渲染 pyecharts 图表
                    chart_html = chart.render_embed()
                    components.html(chart_html, height=650)

                # 因子分析图表
                st.subheader("🔬 因子分析")

                # 选择分析日期（使用回测期间中间的日期）
                mid_date_idx = len(all_trade_dates) // 2
                analysis_date = all_trade_dates[mid_date_idx].strftime("%Y-%m-%d")

                try:
                    # 重新计算因子数据用于分析
                    df_analysis = multi_factor_calculator.calculate(df, factor_params)

                    # 绘制因子分析图表
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

                # 显示详细分析
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

                    # 显示因子权重配置
                    st.write("**因子权重配置:**")
                    for factor_name, weight in selected_factors.items():
                        weight_pct = (weight / total_weight) * 100
                        st.write(f"- {factor_name}: {weight:.1f} ({weight_pct:.1f}%)")

                    # 显示因子特定参数
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

                    # 显示股票交易分析数据
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

                        # 显示汇总统计
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


if __name__ == "__main__":
    main()
