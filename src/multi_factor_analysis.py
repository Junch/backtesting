import backtrader as bt
from datetime import date, datetime
import warnings
import csv
import io

warnings.filterwarnings("ignore")
import sys
import os
import argparse
import streamlit as st
import streamlit.components.v1 as components

from cjdata import LocalData
import pandas as pd
import numpy as np
from typing import Dict

from stock_filters import (
    ListingAgeFilter,
    MarketCapRangeFilter,
    StockFilterContext,
    StockFilterPipeline,
)

# 导入单因子计算器
from single_factor_analysis import (
    AVAILABLE_FACTORS,
)

# 导入回测工具类
from backtest_utils import (
    DateStrategy,
    StockTradeAnalyzer,
    plot_strategy_performance,
    get_trading_days,
)

from multi_factor_calculator import (
    MultiFactorCalculator,
    run_multi_factor_backtesting,
    plot_factor_analysis,
)
from order_utils import (
    xtdata,
    _resolve_signal_date,
    _get_next_trade_date,
    _build_order_lines,
    _fetch_realtime_prices,
    _calculate_allocated_quantities,
)
from strategy_config_io import (
    build_backtest_results,
    build_strategy_config,
    list_saved_strategies,
    load_strategy_yaml,
    save_strategy_yaml,
)


def _parse_date_or_default(value, fallback: date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return fallback
    return fallback


# --- Streamlit 应用主程序 ---
def main():
    st.set_page_config(
        page_title="多因子量化策略回测系统", page_icon="🔬", layout="wide"
    )

    st.title("🔬 多因子量化策略系统")
    st.markdown("同一套多因子参数支持回测分析与盘后选股")

    # 侧边栏参数配置
    st.sidebar.header("多因子策略配置")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    saved_backtests_dir = os.path.join(project_root, "saved_backtests")
    loaded_strategy_config = st.session_state.get("loaded_multi_factor_config", {})
    save_success_path = st.session_state.pop("multi_factor_saved_path", None)

    if save_success_path:
        st.sidebar.success(f"保存成功: {save_success_path}")

    with st.sidebar.expander("📂 加载已保存配置", expanded=False):
        saved_items = list_saved_strategies(saved_backtests_dir)
        if not saved_items:
            st.info("暂无已保存配置")
        else:
            path_to_label = {item["path"]: item["label"] for item in saved_items}
            selected_path = st.selectbox(
                "选择配置文件",
                options=list(path_to_label.keys()),
                format_func=lambda p: path_to_label.get(p, p),
                key="load_multi_factor_select",
            )

            if st.button("加载配置", key="load_multi_factor_btn", width="stretch"):
                try:
                    payload = load_strategy_yaml(selected_path)
                    st.session_state["loaded_multi_factor_config"] = payload
                    st.success("配置加载成功，已应用到当前页面")
                    st.rerun()
                except Exception as e:
                    st.error(f"加载配置失败: {str(e)}")

    factors_cfg = loaded_strategy_config.get("factors", {})
    neutralization_cfg = loaded_strategy_config.get("neutralization", {})
    backtest_cfg = loaded_strategy_config.get("backtest_params", {})
    filters_cfg = loaded_strategy_config.get("filters", {})
    risk_cfg = loaded_strategy_config.get("risk", {}).get("stop_loss", {})

    # 因子选择区域
    st.sidebar.subheader("📊 因子选择与权重设置")

    # 创建多因子计算器
    multi_factor_calculator = MultiFactorCalculator()

    # 中性化全局设置
    st.sidebar.subheader("⚖️ 因子中性化设置")
    neutralization_factor_cfg = neutralization_cfg.get("per_factor", {})
    enable_factor_neutralization = st.sidebar.checkbox(
        "启用因子中性化",
        value=bool(neutralization_cfg.get("enabled", False)),
        help="按交易日横截面对因子做行业/市值中性化，使用残差参与打分",
    )
    default_industry_col = neutralization_cfg.get("industry_column", "industry_sw1")
    neutralization_industry_col = default_industry_col
    if enable_factor_neutralization:
        industry_options = ["industry_sw1", "industry_sw2"]
        industry_index = (
            industry_options.index(default_industry_col)
            if default_industry_col in industry_options
            else 0
        )
        neutralization_industry_col = st.sidebar.selectbox(
            "行业字段",
            options=industry_options,
            index=industry_index,
            help="industry_sw1=申万一级，industry_sw2=申万二级",
        )
    multi_factor_calculator.set_industry_column(neutralization_industry_col)

    # 可用因子列表
    factor_names = list(AVAILABLE_FACTORS.keys())

    # 因子选择和权重设置
    selected_factors = {}
    factor_params = {}

    st.sidebar.write("**选择要使用的因子：**")

    for factor_name in factor_names:
        loaded_factor_cfg = factors_cfg.get(factor_name, {})
        # 因子选择复选框
        factor_selected = st.sidebar.checkbox(
            f"{factor_name}",
            value=bool(loaded_factor_cfg),
            key=f"select_{factor_name}",
            help=AVAILABLE_FACTORS[factor_name].description,
        )

        if factor_selected:
            # 权重设置
            weight = st.sidebar.slider(
                f"{factor_name} 权重",
                min_value=0.1,
                max_value=3.0,
                value=float(loaded_factor_cfg.get("weight", 1.0)),
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
            loaded_factor_params = loaded_factor_cfg.get("params", {})

            if factor_name == "市值因子":
                market_options = ["总市值", "流通市值"]
                default_market_option = loaded_factor_params.get("market_option", "总市值")
                market_option_idx = (
                    market_options.index(default_market_option)
                    if default_market_option in market_options
                    else 0
                )
                market_option = st.sidebar.selectbox(
                    "市值计算方式",
                    market_options,
                    index=market_option_idx,
                    key=f"market_option_{factor_name}",
                )
                smooth_window = st.sidebar.slider(
                    "市值平滑窗口（交易日）",
                    min_value=1,
                    max_value=20,
                    value=int(loaded_factor_params.get("smooth_window", 7)),
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
                    value=int(loaded_factor_params.get("momentum_period", 21)),
                    key=f"momentum_period_{factor_name}",
                )
                factor_params[factor_name] = {"momentum_period": momentum_period}

            elif factor_name == "波动率因子":
                volatility_period = st.sidebar.slider(
                    "波动率计算周期（交易日）",
                    min_value=5,
                    max_value=60,
                    value=int(loaded_factor_params.get("volatility_period", 20)),
                    key=f"volatility_period_{factor_name}",
                )
                factor_params[factor_name] = {"volatility_period": volatility_period}

            elif factor_name == "换手率因子":
                turnover_period = st.sidebar.slider(
                    "换手率平滑周期（交易日）",
                    min_value=5,
                    max_value=60,
                    value=int(loaded_factor_params.get("turnover_period", 20)),
                    key=f"turnover_period_{factor_name}",
                )
                factor_params[factor_name] = {"turnover_period": turnover_period}

            if enable_factor_neutralization:
                loaded_neutralize_cfg = neutralization_factor_cfg.get(factor_name, {})
                default_industry_neutralize = bool(
                    loaded_neutralize_cfg.get(
                        "industry", factor_name in ("动量因子", "市值因子")
                    )
                )
                default_market_cap_neutralize = bool(
                    loaded_neutralize_cfg.get("market_cap", factor_name == "动量因子")
                )

                ncol1, ncol2 = st.sidebar.columns(2)
                with ncol1:
                    industry_neutralize = st.checkbox(
                        "行业中性化",
                        value=default_industry_neutralize,
                        key=f"neutralize_industry_{factor_name}",
                    )

                if factor_name == "市值因子":
                    market_cap_neutralize = False
                    with ncol2:
                        st.checkbox(
                            "市值中性化",
                            value=False,
                            key=f"neutralize_market_cap_{factor_name}",
                            disabled=True,
                            help="市值因子不对市值本身中性化，避免自回归",
                        )
                else:
                    with ncol2:
                        market_cap_neutralize = st.checkbox(
                            "市值中性化",
                            value=default_market_cap_neutralize,
                            key=f"neutralize_market_cap_{factor_name}",
                        )

                multi_factor_calculator.set_neutralization_config(
                    factor_name,
                    industry=industry_neutralize,
                    market_cap=market_cap_neutralize,
                )

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
    default_sector = backtest_cfg.get("sector", "沪深300")
    sector_index = sector_names.index(default_sector) if default_sector in sector_names else 2
    sector_name = st.sidebar.selectbox("选择板块", sector_names, index=sector_index)

    # 日期选择
    default_start_date = _parse_date_or_default(
        backtest_cfg.get("start_date"), datetime(2021, 1, 1).date()
    )
    default_end_date = _parse_date_or_default(
        backtest_cfg.get("end_date"), datetime(2021, 12, 31).date()
    )
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("开始日期", value=default_start_date)
    with col2:
        end_date = st.date_input("结束日期", value=default_end_date)

    # 基础策略参数
    rebalance_period = st.sidebar.slider(
        "调仓周期（交易日）",
        min_value=5,
        max_value=60,
        value=int(backtest_cfg.get("rebalance_period", 21)),
    )
    hold_top = st.sidebar.slider(
        "持有股票数量",
        min_value=5,
        max_value=30,
        value=int(backtest_cfg.get("hold_top", 10)),
    )

    # 前置过滤参数（可扩展）
    st.sidebar.subheader("前置过滤")
    filter_pipeline = StockFilterPipeline()

    market_cap_cfg = filters_cfg.get("market_cap", {})
    default_min_market_cap = float(market_cap_cfg.get("min_billion", 0.0))
    default_max_market_cap = float(market_cap_cfg.get("max_billion", 2000.0))
    min_market_cap = default_min_market_cap
    max_market_cap = default_max_market_cap

    enable_market_cap_filter = st.sidebar.checkbox(
        "启用市值范围过滤", value=bool(market_cap_cfg.get("enabled", False))
    )
    if enable_market_cap_filter:
        col_mc1, col_mc2 = st.sidebar.columns(2)
        with col_mc1:
            min_market_cap = st.number_input(
                "最小市值(亿元)",
                min_value=0.0,
                value=default_min_market_cap,
                step=10.0,
            )
        with col_mc2:
            max_market_cap = st.number_input(
                "最大市值(亿元)",
                min_value=0.0,
                value=default_max_market_cap,
                step=10.0,
            )
        if max_market_cap < min_market_cap:
            st.sidebar.warning("最大市值小于最小市值，将自动交换两者")
            min_market_cap, max_market_cap = max_market_cap, min_market_cap
        filter_pipeline.add_filter(
            MarketCapRangeFilter(min_cap=min_market_cap, max_cap=max_market_cap)
        )

    listing_cfg = filters_cfg.get("listing_age", {})
    listing_min_days = None
    enable_listing_age_filter = st.sidebar.checkbox(
        "启用上市时长过滤", value=bool(listing_cfg.get("enabled", False))
    )
    if enable_listing_age_filter:
        listing_options = [60, 180, 365, 730, 1095, 1460]
        default_listing_min_days = int(listing_cfg.get("min_days", 60))
        listing_index = (
            listing_options.index(default_listing_min_days)
            if default_listing_min_days in listing_options
            else 0
        )
        listing_days_option = st.sidebar.selectbox(
            "最短上市时长（自然日）",
            options=listing_options,
            index=listing_index,
            help="365约等于1年，730约等于2年",
        )
        listing_min_days = int(listing_days_option)
        filter_pipeline.add_filter(ListingAgeFilter(min_days=listing_min_days))

    # 去极值选项
    winsorize_factors = st.sidebar.checkbox(
        "去极值 (Winsorization)",
        value=bool(backtest_cfg.get("winsorize", True)),
        help="使用MAD方法识别和处理异常值，比传统标准差方法更稳健，适合非正态分布数据",
    )
    multi_factor_calculator.set_winsorize_factors(winsorize_factors)

    # 因子标准化选项
    standardize_factors = st.sidebar.checkbox(
        "标准化因子值",
        value=bool(backtest_cfg.get("standardize", True)),
        help="对因子值进行标准化处理，消除量纲影响",
    )
    multi_factor_calculator.set_standardize_factors(standardize_factors)

    # 风险控制参数（回测页使用）
    st.sidebar.subheader("🛡️ 风险控制")
    enable_stop_loss = st.sidebar.checkbox(
        "启用止损", value=bool(risk_cfg.get("enabled", False))
    )
    stop_loss_pct = None
    trailing_stop = bool(risk_cfg.get("trailing", False))

    if enable_stop_loss:
        default_stop_loss_percent = float(risk_cfg.get("percentage", 10.0))
        stop_loss_pct = (
            st.sidebar.slider(
                "止损比例 (%)",
                min_value=1.0,
                max_value=20.0,
                value=default_stop_loss_percent,
                step=0.5,
            )
            / 100
        )
        trailing_stop = st.sidebar.checkbox("启用移动止损", value=trailing_stop)

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
        backtest_snapshot = st.session_state.get("last_multi_factor_backtest")

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

            **因子中性化：** {"是" if enable_factor_neutralization else "否"}

            **行业字段：** {neutralization_industry_col}

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
                    industry_sw1_map: Dict[str, str] = {}
                    industry_sw2_map: Dict[str, str] = {}
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

                        if {"stock_code", "industry_sw1"}.issubset(basic_df.columns):
                            industry_sw1_map = (
                                basic_df.dropna(subset=["stock_code", "industry_sw1"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["industry_sw1"]
                                .astype(str)
                                .to_dict()
                            )

                        if {"stock_code", "industry_sw2"}.issubset(basic_df.columns):
                            industry_sw2_map = (
                                basic_df.dropna(subset=["stock_code", "industry_sw2"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["industry_sw2"]
                                .astype(str)
                                .to_dict()
                            )

                    selected_industry_map = (
                        industry_sw2_map
                        if neutralization_industry_col == "industry_sw2"
                        else industry_sw1_map
                    )
                    multi_factor_calculator.set_industry_map(selected_industry_map)

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
                        industry_map=selected_industry_map,
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
                    cerebro.addanalyzer(bt.analyzers.SharpeRatio, 
                                        _name="sharpe_ratio",
                                        timeframe=bt.TimeFrame.Days,
                                        annualize=True,
                                        riskfreerate=0.015,
                                        factor=242) # 中国股市一年大约 242 个交易日
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
                    sharpe_value = None
                    max_dd_value = None
                    max_dd_len_value = None
                    annual_return_value = None

                    with col1:
                        total_return = (final_value / initial_value - 1) * 100
                        st.metric("总收益率", f"{total_return:.2f}%")

                    with col2:
                        try:
                            sharpe_value = strat.analyzers.sharpe_ratio.get_analysis()[
                                "sharperatio"
                            ]
                            if sharpe_value is None:
                                sharpe_value = 0.0
                            st.metric("夏普比率", f"{sharpe_value:.3f}")
                        except Exception:
                            st.metric("夏普比率", "N/A")

                    with col3:
                        try:
                            max_dd_value = strat.analyzers.drawdown.get_analysis()["max"][
                                "drawdown"
                            ]
                            st.metric("最大回撤", f"{max_dd_value:.2f}%")
                        except Exception:
                            st.metric("最大回撤", "N/A")

                    with col4:
                        try:
                            max_dd_len_value = strat.analyzers.drawdown.get_analysis()[
                                "max"
                            ]["len"]
                            st.metric("最长回撤时间", f"{max_dd_len_value} 天")
                        except Exception:
                            st.metric("最长回撤时间", "N/A")

                    with col5:
                        try:
                            annual_return_value = strat.analyzers.returns.get_analysis()[
                                "rnorm100"
                            ]
                            st.metric("年化收益率", f"{annual_return_value:.2f}%")
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

                    analysis_result = trade_analyzer.get_analysis()

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
                            f"- 因子中性化: {'是' if enable_factor_neutralization else '否'}"
                        )
                        if enable_factor_neutralization:
                            st.write(f"- 中性化行业字段: {neutralization_industry_col}")
                            neutralization_cfg = (
                                multi_factor_calculator.neutralization_config
                            )
                            for factor_name in selected_factors.keys():
                                cfg = neutralization_cfg.get(factor_name, {})
                                st.write(
                                    "- "
                                    f"{factor_name} 中性化: "
                                    f"行业={'是' if cfg.get('industry', False) else '否'}, "
                                    f"市值={'是' if cfg.get('market_cap', False) else '否'}"
                                )
                        st.write(
                            "- 执行规则: t日生成信号, 下一根K线执行; 执行日不可交易则跳过且不补单"
                        )

                        active_filters = filter_pipeline.get_filter_descriptions()
                        if active_filters:
                            st.write("**前置过滤条件:**")
                            for filter_desc in active_filters:
                                st.write(f"- {filter_desc}")
                            if enable_listing_age_filter:
                                st.write("- 上市日期来源: stock_basic.listed_date")
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

                    strategy_config = build_strategy_config(
                        selected_factors=selected_factors,
                        factor_params=factor_params,
                        enable_factor_neutralization=enable_factor_neutralization,
                        neutralization_industry_col=neutralization_industry_col,
                        neutralization_config=multi_factor_calculator.neutralization_config,
                        sector_name=sector_name,
                        start_date=start_date,
                        end_date=end_date,
                        rebalance_period=rebalance_period,
                        hold_top=hold_top,
                        standardize_factors=standardize_factors,
                        enable_market_cap_filter=enable_market_cap_filter,
                        min_market_cap=min_market_cap,
                        max_market_cap=max_market_cap,
                        enable_listing_age_filter=enable_listing_age_filter,
                        listing_min_days=listing_min_days,
                        enable_stop_loss=enable_stop_loss,
                        stop_loss_pct=stop_loss_pct,
                        trailing_stop=trailing_stop,
                    )

                    backtest_results_payload = build_backtest_results(
                        initial_value=initial_value,
                        final_value=final_value,
                        total_return_pct=total_return,
                        annual_return_pct=annual_return_value,
                        sharpe_ratio=sharpe_value,
                        max_drawdown_pct=max_dd_value,
                        max_drawdown_days=max_dd_len_value,
                        loaded_stock_count=len(stock_list),
                        summary_data=analysis_result.get("summary_data"),
                        total_commission=analysis_result.get("total_commission"),
                    )

                    st.session_state["last_multi_factor_backtest"] = {
                        "strategy_config": strategy_config,
                        "results": backtest_results_payload,
                    }
                    backtest_snapshot = st.session_state.get("last_multi_factor_backtest")
                    st.info("回测结果已缓存，可点击下方按钮保存为 YAML")

                except Exception as e:
                    st.error(f"多因子回测过程中发生错误: {str(e)}")
                    st.exception(e)

        if backtest_snapshot:
            st.subheader("💾 保存参数与回测结果")
            save_note = st.text_input(
                "保存备注（可选）",
                key="save_multi_factor_note",
                placeholder="例如：2021年沪深300，含动量+市值中性化",
            )
            if st.button(
                "💾 保存当前配置与回测结果",
                key="save_multi_factor_snapshot_btn",
                width="stretch",
            ):
                strategy_config = dict(backtest_snapshot["strategy_config"])
                if save_note:
                    strategy_config["description"] = save_note

                saved_path = save_strategy_yaml(
                    directory=saved_backtests_dir,
                    strategy_config=strategy_config,
                    results=backtest_snapshot["results"],
                )
                st.session_state["multi_factor_saved_path"] = saved_path
                st.rerun()
        else:
            st.caption("请先运行一次回测，再保存参数与结果")

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
                    industry_sw1_map: Dict[str, str] = {}
                    industry_sw2_map: Dict[str, str] = {}
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

                        if {"stock_code", "industry_sw1"}.issubset(basic_df.columns):
                            industry_sw1_map = (
                                basic_df.dropna(subset=["stock_code", "industry_sw1"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["industry_sw1"]
                                .astype(str)
                                .to_dict()
                            )

                        if {"stock_code", "industry_sw2"}.issubset(basic_df.columns):
                            industry_sw2_map = (
                                basic_df.dropna(subset=["stock_code", "industry_sw2"])
                                .drop_duplicates(subset=["stock_code"])
                                .set_index("stock_code")["industry_sw2"]
                                .astype(str)
                                .to_dict()
                            )

                    selected_industry_map = (
                        industry_sw2_map
                        if neutralization_industry_col == "industry_sw2"
                        else industry_sw1_map
                    )
                    multi_factor_calculator.set_industry_map(selected_industry_map)

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

                    if filter_pipeline:
                        filter_context = StockFilterContext(
                            trade_date=signal_date,
                            universe_df=df,
                            listed_dates=listed_dates,
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
                    ranked_df["industry_sw1"] = ranked_df["stock_code"].map(
                        industry_sw1_map
                    )
                    ranked_df["industry_sw2"] = ranked_df["stock_code"].map(
                        industry_sw2_map
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
                        "industry_sw1",
                        "industry_sw2",
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

                    order_rows = _build_order_lines(
                        executable_df,
                        next_trade_date,
                    )

                    if not order_rows:
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

                    order_buffer = io.StringIO()
                    csv.writer(order_buffer, lineterminator="\n").writerows(order_rows)
                    order_content = order_buffer.getvalue()

                    order_file_path = os.path.join(os.path.dirname(__file__), "order.csv")
                    with open(order_file_path, "w", newline="", encoding="utf-8") as f:
                        csv.writer(f, lineterminator="\n").writerows(order_rows)

                    st.info(
                        f"已生成次日买入清单，日期: {next_trade_date.strftime('%Y-%m-%d')}，共 {len(order_rows)} 条"
                    )
                    st.code(order_content, language="text")
                    st.download_button(
                        "⬇️ 下载 order.csv",
                        data=order_content.encode("utf-8"),
                        file_name="order.csv",
                        mime="text/csv",
                        width="stretch",
                    )

                except Exception as e:
                    st.error(f"盘后选股过程中发生错误: {str(e)}")
                    st.exception(e)


if __name__ == "__main__":
    main()
