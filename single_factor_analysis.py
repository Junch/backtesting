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

# 导入回测工具类
from backtest_utils import (
    DateStrategy,
    StockTradeAnalyzer,
    plot_strategy_performance,
    get_trading_days,
)


class FactorCalculator:
    """
    单因子计算器基类
    为不同的因子计算提供统一的接口
    """

    def __init__(self, name, description, ascending=True):
        """
        初始化因子计算器

        Args:
            name: 因子名称
            description: 因子描述
            ascending: 排序方式，True为升序（小的值排在前面），False为降序
        """
        self.name = name
        self.description = description
        self.ascending = ascending

    def calculate(self, df, **kwargs):
        """
        计算因子值

        Args:
            df: 包含股票数据的DataFrame
            **kwargs: 其他参数

        Returns:
            DataFrame: 添加了因子列的数据框
        """
        raise NotImplementedError("子类必须实现calculate方法")

    def get_factor_column(self):
        """返回因子列名"""
        return f"factor_{self.name.lower().replace(' ', '_')}"


class MarketValueFactor(FactorCalculator):
    """市值因子计算器"""

    def __init__(self):
        super().__init__("市值", "基于总市值或流通市值进行股票排序", ascending=True)

    def calculate(self, df, market_option="总市值", smooth_window=7, **kwargs):
        """
        计算市值因子

        Args:
            df: 股票数据DataFrame
            market_option: 市值计算方式（"总市值" 或 "流通市值"）
            smooth_window: 市值平滑窗口大小

        Returns:
            DataFrame: 添加了市值因子的数据框
        """
        df = df.copy()

        if "tradestatus" in df.columns:
            valid_turn = df["turn"] != 0
            df.loc[(df["tradestatus"] == 1) & valid_turn, "market"] = (
                df["amount"] / df["turn"] / 1e8
            )
        else:
            raise ValueError("输入数据缺少 'turn' 列，无法计算市值")

        # 按股票代码分组，计算移动平均市值，平滑数据
        df = df.sort_values(["stock_code", "trade_date"])
        factor_col = self.get_factor_column()
        df[factor_col] = df.groupby("stock_code")["market"].transform(
            lambda x: x.rolling(window=smooth_window, min_periods=1).mean()
        )

        return df


class MomentumFactor(FactorCalculator):
    """动量因子计算器"""

    def __init__(self):
        super().__init__("动量", "基于价格动量进行股票排序", ascending=False)

    def calculate(self, df, momentum_period=21, **kwargs):
        """
        计算动量因子

        Args:
            df: 股票数据DataFrame
            momentum_period: 动量计算周期

        Returns:
            DataFrame: 添加了动量因子的数据框
        """
        df = df.copy()

        # 按股票分组后分别计算每只股票的动量
        factor_col = self.get_factor_column()
        df[factor_col] = df.groupby("stock_code")["close"].pct_change(
            periods=momentum_period
        )

        return df


class ValueFactor(FactorCalculator):
    """价值因子计算器（基于市盈率）"""

    def __init__(self):
        super().__init__("价值", "基于市盈率进行股票排序，低市盈率优先", ascending=True)

    def calculate(self, df, **kwargs):
        """
        计算价值因子（市盈率）

        Args:
            df: 股票数据DataFrame

        Returns:
            DataFrame: 添加了价值因子的数据框
        """
        df = df.copy()

        factor_col = self.get_factor_column()

        # 如果数据中有pe列，直接使用
        if "peTTM" in df.columns:
            df[factor_col] = df["peTTM"]
        elif "pe" in df.columns:
            df[factor_col] = df["pe"]
        else:
            # 如果没有pe列，尝试从市值和净利润计算
            # 这里需要根据实际数据结构调整
            st.warning("数据中缺少PE（市盈率）列，价值因子计算可能不准确")
            # 使用价格作为替代（仅为示例，实际应用中需要真实的PE数据）
            df[factor_col] = df["close"]

        # 过滤掉负市盈率和过高市盈率的股票
        df.loc[df[factor_col] <= 0, factor_col] = np.nan
        df.loc[df[factor_col] > 100, factor_col] = np.nan

        return df


class ROEFactor(FactorCalculator):
    """ROE因子计算器"""

    def __init__(self):
        super().__init__("ROE", "基于净资产收益率进行股票排序", ascending=False)

    def calculate(self, df, **kwargs):
        """
        计算ROE因子

        Args:
            df: 股票数据DataFrame

        Returns:
            DataFrame: 添加了ROE因子的数据框
        """
        df = df.copy()

        factor_col = self.get_factor_column()

        # 如果数据中有roe列，直接使用
        if "roe" in df.columns:
            df[factor_col] = df["roe"]
        else:
            st.warning("数据中缺少ROE列，ROE因子计算将使用默认值")
            # 使用随机值作为示例（实际应用中需要真实的ROE数据）
            df[factor_col] = np.random.uniform(0, 30, len(df))

        return df


class VolatilityFactor(FactorCalculator):
    """波动率因子计算器"""

    def __init__(self):
        super().__init__(
            "波动率", "基于价格波动率进行股票排序，低波动率优先", ascending=True
        )

    def calculate(self, df, volatility_period=20, **kwargs):
        """
        计算波动率因子

        Args:
            df: 股票数据DataFrame
            volatility_period: 波动率计算周期

        Returns:
            DataFrame: 添加了波动率因子的数据框
        """
        df = df.copy()

        factor_col = self.get_factor_column()

        # 计算每日收益率
        df = df.sort_values(["stock_code", "trade_date"])
        df["daily_return"] = df.groupby("stock_code")["close"].pct_change()

        # 计算滚动标准差作为波动率
        df[factor_col] = df.groupby("stock_code")["daily_return"].transform(
            lambda x: x.rolling(window=volatility_period, min_periods=5).std()
        )

        return df


class TurnoverFactor(FactorCalculator):
    """换手率因子计算器"""

    def __init__(self):
        super().__init__(
            "换手率", "基于换手率进行股票排序，高换手率优先", ascending=False
        )

    def calculate(self, df, turnover_period=20, **kwargs):
        """
        计算换手率因子

        Args:
            df: 股票数据DataFrame
            turnover_period: 换手率平滑周期

        Returns:
            DataFrame: 添加了换手率因子的数据框
        """
        df = df.copy()

        factor_col = self.get_factor_column()

        if "turn" in df.columns:
            # 计算平均换手率
            df = df.sort_values(["stock_code", "trade_date"])
            df[factor_col] = df.groupby("stock_code")["turn"].transform(
                lambda x: x.rolling(window=turnover_period, min_periods=1).mean()
            )
        else:
            st.warning("数据中缺少换手率('turn')列，无法计算换手率因子")
            df[factor_col] = np.nan

        return df


# 注册所有可用的因子
AVAILABLE_FACTORS = {
    "市值因子": MarketValueFactor(),
    "动量因子": MomentumFactor(),
    "价值因子(PE)": ValueFactor(),
    "ROE因子": ROEFactor(),
    "波动率因子": VolatilityFactor(),
    "换手率因子": TurnoverFactor(),
}


def run_single_factor_backtesting(
    df,
    all_trade_dates,
    factor_calculator,
    rebalance_period,
    hold_top,
    factor_params=None,
):
    """
    通用的单因子回测数据处理函数

    Args:
        df: 包含多只股票日行情数据的DataFrame
        all_trade_dates: 所有交易日列表
        factor_calculator: 因子计算器实例
        rebalance_period: 调仓周期
        hold_top: 持有股票数量
        factor_params: 因子计算参数字典

    Returns:
        tuple: (stock_list, buy_dates, sell_dates)
    """
    if factor_params is None:
        factor_params = {}

    # 计算因子值
    df = factor_calculator.calculate(df, **factor_params)
    factor_col = factor_calculator.get_factor_column()

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
            & (df[factor_col].notna())
        ]  # 过滤掉因子值为空的股票

        if not valid_stocks.empty:
            # 根据因子计算器的排序方式进行排序
            selected_stocks = (
                valid_stocks.sort_values(
                    by=factor_col, ascending=factor_calculator.ascending
                )
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


# --- Streamlit 应用主程序 ---
def main():
    st.set_page_config(
        page_title="单因子量化策略回测系统", page_icon="📈", layout="wide"
    )

    st.title("📈 单因子量化策略回测系统")
    st.markdown("基于不同因子的股票选择策略回测分析")

    # 侧边栏参数配置
    st.sidebar.header("策略参数配置")

    # 因子选择
    factor_names = list(AVAILABLE_FACTORS.keys())
    selected_factor_name = st.sidebar.selectbox("选择因子", factor_names, index=0)
    selected_factor = AVAILABLE_FACTORS[selected_factor_name]

    st.sidebar.write(f"**因子描述**: {selected_factor.description}")

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

    # 因子特定参数
    st.sidebar.subheader("因子参数")
    factor_params = {}

    if selected_factor_name == "市值因子":
        market_option = st.sidebar.selectbox(
            "市值计算方式", ["总市值", "流通市值"], index=0
        )
        smooth_window = st.sidebar.slider(
            "市值平滑窗口（交易日）", min_value=1, max_value=20, value=7
        )
        factor_params = {"market_option": market_option, "smooth_window": smooth_window}

    elif selected_factor_name == "动量因子":
        momentum_period = st.sidebar.slider(
            "动量回看期（交易日）", min_value=5, max_value=60, value=21
        )
        factor_params = {"momentum_period": momentum_period}

    elif selected_factor_name == "价值因子(PE)":
        st.sidebar.info("价值因子基于市盈率，低PE优先选择")

    elif selected_factor_name == "ROE因子":
        st.sidebar.info("ROE因子基于净资产收益率，高ROE优先选择")

    elif selected_factor_name == "波动率因子":
        volatility_period = st.sidebar.slider(
            "波动率计算周期（交易日）", min_value=5, max_value=60, value=20
        )
        factor_params = {"volatility_period": volatility_period}

    elif selected_factor_name == "换手率因子":
        turnover_period = st.sidebar.slider(
            "换手率平滑周期（交易日）", min_value=5, max_value=60, value=20
        )
        factor_params = {"turnover_period": turnover_period}

    # 风险控制参数
    st.sidebar.subheader("风险控制")
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

    # 运行回测按钮
    if st.sidebar.button("🚀 开始回测", type="primary"):
        # 转换日期格式
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        with st.spinner("正在运行回测..."):
            try:
                # 创建回测引擎
                cerebro = bt.Cerebro()
                cerebro.broker.setcash(1000000.0)
                cerebro.broker.setcommission(commission=0.001)
                cerebro.addobserver(bt.observers.Value)

                # 加载股票数据
                st.info(f"正在加载{sector_name}板块股票数据...")
                db_path = os.environ.get(
                    "STOCK_DATA_DB", "C:/github/cjdata/data/stock_data_hfq.db"
                )
                findata = LocalData(db_path)
                # 取start_date前60个交易日的数据用于计算因子
                pre_start_date = (start_date - pd.Timedelta(days=60)).strftime("%Y%m%d")
                df = findata.get_stock_data_frame_in_sector(
                    sector_name, pre_start_date, end_str, adj="hfq"
                )
                all_trade_dates = get_trading_days(df, start_date)

                # 运行单因子回测
                stock_list, buy_dates, sell_dates = run_single_factor_backtesting(
                    df,
                    all_trade_dates,
                    selected_factor,
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
                st.subheader("📊 策略性能指标")

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

                # 显示详细分析
                with st.expander("📋 详细分析结果"):
                    st.write("**策略参数:**")
                    st.write(f"- 选择因子: {selected_factor_name}")
                    st.write(f"- 因子描述: {selected_factor.description}")
                    st.write(f"- 板块: {sector_name}")
                    st.write(f"- 回测期间: {start_str} - {end_str}")
                    st.write(f"- 调仓周期: {rebalance_period} 个交易日")
                    st.write(f"- 持有股票数量: {hold_top} 只")
                    st.write(f"- 手续费率: 0.1%")

                    # 显示因子特定参数
                    if factor_params:
                        st.write("**因子参数:**")
                        for key, value in factor_params.items():
                            st.write(f"- {key}: {value}")

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
                st.error(f"回测过程中发生错误: {str(e)}")
                st.exception(e)


if __name__ == "__main__":
    main()
