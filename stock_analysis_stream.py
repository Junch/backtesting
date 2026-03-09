"""
基于Streamlit的股票分位分析程序
用户可以通过Web界面输入起止日期和分位数，分析沪深A股的收益率分位情况
"""

import sys
import os
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from datetime import datetime, date
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from cjdata import LocalData

# 设置matplotlib中文字体
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False

# 设置页面配置
st.set_page_config(
    page_title="股票分位分析程序",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


def calculate_return(group):
    """计算单只股票的收益率"""
    group = group.sort_values("trade_date")
    first_preclose = group.iloc[0]["preclose"]
    last_close = group.iloc[-1]["close"]

    if first_preclose != 0:
        return_pct = (last_close - first_preclose) / first_preclose
    else:
        return_pct = 0
    return return_pct


def get_period_average_data(group):
    """获取期间前7个交易日的平均市值和PE数据"""
    group = group.sort_values("trade_date")
    initial = group.iloc[0]

    # 取前7个交易日的数据，如果不足7个则取所有数据
    early_data = group.head(7)

    return pd.Series(
        {
            "market_cap": early_data["market_cap"].mean(),
            "pe": early_data["peTTM"].mean(),
            "turn": early_data["turn"].mean(),
            "initial_close": initial["close"],
            "initial_date": initial["trade_date"],
        }
    )


@st.cache_data
def analyze_stock_quantiles(start_date, end_date, n_quantiles, quantile_basis="收益率"):
    """分析股票分位数据（使用缓存提高性能）"""
    try:
        # 获取数据
        db_path = os.environ.get(
            "STOCK_DATA_DB", "C:/github/cjdata/data/stock_data_hfq.db"
        )
        findata = LocalData(db_path)
        df = findata.get_stock_data_frame_in_sector("沪深A股", start_date, end_date)
        df["market_cap"] = df["amount"] / df["turn"] * 100

        # 有些股票根本还没来得及上市，比如601399.SH，是在2020-06-08日才上市的。
        # 这些股票在上市前的交易数据是缺失的，统计的时候会导致数据不正确，比如市值等。
        df = df.dropna()

        df_name = findata.get_stock_name_in_sector("沪深A股")

        # 计算收益率
        stock_returns = df.groupby("stock_code").apply(
            calculate_return, include_groups=False
        )

        # 获取前7日平均市值和PE数据
        stock_market_data = df.groupby("stock_code").apply(
            get_period_average_data, include_groups=False
        )

        # 合并数据
        result_df = df_name.merge(
            pd.DataFrame(
                {"stock_code": stock_returns.index, "returns": stock_returns.values}
            ),
            on="stock_code",
            how="inner",
        )

        market_data_df = stock_market_data.reset_index()
        result_df = result_df.merge(market_data_df, on="stock_code", how="left")

        # 过滤异常数据
        result_df = result_df.dropna(subset=["returns"])
        result_df = result_df[np.isfinite(result_df["returns"])]

        # 根据分位依据选择排序列
        basis_mapping = {
            "收益率": "returns",
            "市值": "market_cap",
            "PE": "pe",
            "换手率": "turn",
        }
        sort_column = basis_mapping.get(quantile_basis, "returns")

        # 过滤分位依据列的异常数据
        if sort_column != "returns":
            result_df = result_df.dropna(subset=[sort_column])
            result_df = result_df[np.isfinite(result_df[sort_column])]

        # 分位划分
        result_df = result_df.sort_values(sort_column)
        labels = [f"Q{i}" for i in range(1, n_quantiles + 1)]

        try:
            # 尝试使用等频分位（qcut）
            quantiles = pd.qcut(
                result_df[sort_column], n_quantiles, labels=labels, duplicates="drop"
            )
            result_df["quantile"] = quantiles
        except ValueError as e:
            # 如果等频分位失败（由于重复值过多），改用等距分位（cut）
            st.warning(
                f"由于数据中存在大量重复值，自动切换为等距分位方法。原错误: {str(e)}"
            )
            quantiles = pd.cut(
                result_df[sort_column], n_quantiles, labels=labels, duplicates="drop"
            )
            result_df["quantile"] = quantiles

            # 如果等距分位也失败，进一步处理
            if result_df["quantile"].isna().any():
                st.warning("部分股票无法分配到分位，将被移除。")
                result_df = result_df.dropna(subset=["quantile"])

        return result_df

    except Exception as e:
        st.error(f"数据分析过程中出现错误: {str(e)}")
        return None


def sort_quantile_labels(quantile_labels):
    """按分位数字排序分位标签"""
    return sorted(quantile_labels, key=lambda q: int(q[1:]))


def calculate_correlations(df):
    """计算收益率与市值、PE、换手率的相关性"""
    # 选择用于相关性分析的列
    analysis_cols = ["returns", "market_cap", "pe", "turn"]

    # 过滤掉包含NaN的行
    corr_data = df[analysis_cols].dropna()

    if len(corr_data) == 0:
        return None

    # 计算相关系数矩阵
    corr_matrix = corr_data.corr()

    # 提取收益率与其他变量的相关系数
    correlations = {
        "收益率_vs_市值": corr_matrix.loc["returns", "market_cap"],
        "收益率_vs_PE": corr_matrix.loc["returns", "pe"],
        "收益率_vs_换手率": corr_matrix.loc["returns", "turn"],
    }

    return correlations


def calculate_quantile_statistics(df, n_quantiles):
    """计算分位统计数据"""
    stats = []
    quantile_labels = sort_quantile_labels(df["quantile"].unique())

    for quantile in quantile_labels:
        quantile_data = df[df["quantile"] == quantile]

        avg_return = quantile_data["returns"].mean()
        avg_market_cap = quantile_data["market_cap"].mean() / 1e6
        avg_turn = quantile_data["turn"].mean()

        # PE分析：区分盈利和亏损公司
        pe_data = quantile_data["pe"].dropna()
        total_pe_count = len(pe_data)
        positive_pe_count = (pe_data > 0).sum()
        negative_pe_count = (pe_data < 0).sum()

        if total_pe_count > 0:
            # 只计算正PE的平均值（盈利公司）
            positive_pe = pe_data[pe_data > 0]
            avg_positive_pe = positive_pe.mean() if len(positive_pe) > 0 else np.nan

            # PE统计信息
            pe_info = (
                f"盈利:{positive_pe_count}只(均PE:{avg_positive_pe:.1f}) 亏损:{negative_pe_count}只"
                if not np.isnan(avg_positive_pe)
                else f"盈利:{positive_pe_count}只 亏损:{negative_pe_count}只"
            )
        else:
            pe_info = "N/A"
            avg_positive_pe = np.nan

        stock_count = len(quantile_data)

        min_return = quantile_data["returns"].min()
        max_return = quantile_data["returns"].max()

        stats.append(
            {
                "分位": quantile,
                "股票数量": stock_count,
                "平均收益率": f"{avg_return:.4f}",
                "收益率(%)": f"{avg_return * 100:.2f}%",
                "收益率范围": f"{min_return:.4f} ~ {max_return:.4f}",
                "前7日平均市值(亿元)": f"{avg_market_cap:.2f}"
                if not np.isnan(avg_market_cap)
                else "N/A",
                "前7日平均换手率(%)": f"{avg_turn:.2f}"
                if not np.isnan(avg_turn)
                else "N/A",
                "前7日平均PE分析": pe_info,
            }
        )

    return pd.DataFrame(stats)


def create_plotly_charts(df, n_quantiles, quantile_basis="收益率"):
    """使用Plotly创建交互式图表"""
    quantile_labels = sort_quantile_labels(df["quantile"].unique())

    # 创建子图
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=[
            "收益率分布直方图",
            f"各{n_quantiles}分位平均收益率",
            f"各{n_quantiles}分位前7日平均市值",
            f"各{n_quantiles}分位前7日平均PE",
        ],
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ],
    )

    # 1. 收益率分布直方图
    fig.add_trace(
        go.Histogram(x=df["returns"], nbinsx=50, name="收益率分布", showlegend=False),
        row=1,
        col=1,
    )

    # 2. 各分位平均收益率
    avg_returns = [df[df["quantile"] == q]["returns"].mean() for q in quantile_labels]
    fig.add_trace(
        go.Bar(
            x=quantile_labels,
            y=avg_returns,
            name="平均收益率",
            showlegend=False,
            text=[f"{r:.3f}" for r in avg_returns],
            textposition="auto",
        ),
        row=1,
        col=2,
    )

    # 3. 各分位平均市值
    avg_market_caps = []
    for q in quantile_labels:
        quantile_data = df[df["quantile"] == q]
        avg_cap = quantile_data["market_cap"].mean() / 1e6
        avg_market_caps.append(avg_cap if not np.isnan(avg_cap) else 0)

    fig.add_trace(
        go.Bar(
            x=quantile_labels,
            y=avg_market_caps,
            name="平均市值",
            showlegend=False,
            text=[f"{c:.2f}" for c in avg_market_caps],
            textposition="auto",
        ),
        row=2,
        col=1,
    )

    # 4. 各分位盈利公司平均PE
    avg_positive_pes = []
    profitable_ratios = []
    for q in quantile_labels:
        quantile_data = df[df["quantile"] == q]
        pe_data = quantile_data["pe"].dropna()
        total_count = len(pe_data)
        positive_pe_data = pe_data[pe_data > 0]
        positive_count = len(positive_pe_data)

        if len(positive_pe_data) > 0:
            avg_positive_pe = positive_pe_data.mean()
            avg_positive_pes.append(avg_positive_pe)
        else:
            avg_positive_pes.append(0)

        # 计算盈利公司比例
        profitable_ratio = (
            (positive_count / total_count * 100) if total_count > 0 else 0
        )
        profitable_ratios.append(profitable_ratio)

    fig.add_trace(
        go.Bar(
            x=quantile_labels,
            y=avg_positive_pes,
            name="盈利公司平均PE",
            showlegend=False,
            text=[
                f"PE:{p:.1f}<br>盈利率:{r:.1f}%"
                for p, r in zip(avg_positive_pes, profitable_ratios)
            ],
            textposition="auto",
        ),
        row=2,
        col=2,
    )

    # 更新布局
    fig.update_layout(
        height=800, title_text=f"沪深A股{n_quantiles}分位分析（按{quantile_basis}分位）"
    )
    fig.update_xaxes(title_text="收益率", row=1, col=1)
    fig.update_yaxes(title_text="股票数量", row=1, col=1)
    fig.update_xaxes(title_text="分位", row=1, col=2)
    fig.update_yaxes(title_text="平均收益率", row=1, col=2)
    fig.update_xaxes(title_text="分位", row=2, col=1)
    fig.update_yaxes(title_text="前7日平均市值(亿元)", row=2, col=1)
    fig.update_xaxes(title_text="分位", row=2, col=2)
    fig.update_yaxes(title_text="盈利公司平均PE", row=2, col=2)

    return fig


def main():
    """主程序"""
    # 页面标题
    st.title("📈 股票分位分析程序")
    st.markdown("---")

    # 侧边栏参数设置
    st.sidebar.header("📊 分析参数设置")

    # 日期选择
    st.sidebar.subheader("时间范围")
    col1, col2 = st.sidebar.columns(2)

    with col1:
        start_date = st.date_input(
            "开始日期",
            value=date(2016, 1, 28),
            min_value=date(2010, 1, 1),
            max_value=date.today(),
        )

    with col2:
        end_date = st.date_input(
            "结束日期",
            value=date(2018, 1, 24),
            min_value=date(2010, 1, 1),
            max_value=date.today(),
        )

    # 分位数选择
    st.sidebar.subheader("分位设置")
    n_quantiles = st.sidebar.slider(
        "分位数", min_value=3, max_value=20, value=7, help="将股票按收益率分成几个等级"
    )

    # 分位依据选择
    quantile_basis = st.sidebar.selectbox(
        "分位依据",
        options=["收益率", "市值", "PE", "换手率"],
        index=0,
        help="选择用于分位划分的依据",
    )

    # 转换日期格式
    start_date_str = start_date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    # 验证日期
    if start_date >= end_date:
        st.sidebar.error("开始日期必须早于结束日期！")
        return

    # 分析按钮
    analyze_button = st.sidebar.button("🚀 开始分析", type="primary")

    # 初始化session state
    if "analysis_results" not in st.session_state:
        st.session_state.analysis_results = None
    if "analysis_params" not in st.session_state:
        st.session_state.analysis_params = None

    # 检查是否需要重新分析（参数改变时）
    current_params = (start_date_str, end_date_str, n_quantiles, quantile_basis)
    need_reanalysis = st.session_state.analysis_params != current_params

    # 执行分析
    if analyze_button or (
        st.session_state.analysis_results is not None and not need_reanalysis
    ):
        if analyze_button or need_reanalysis:
            # 显示分析参数
            st.subheader("📋 分析参数")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.info(f"**开始日期:** {start_date}")
            with col2:
                st.info(f"**结束日期:** {end_date}")
            with col3:
                st.info(f"**分位数:** {n_quantiles}")
            with col4:
                st.info(f"**分位依据:** {quantile_basis}")

            # 进度条
            progress_bar = st.progress(0)
            status_text = st.empty()

            # 开始分析
            status_text.text("正在获取和分析股票数据...")
            progress_bar.progress(20)

            # 分析数据
            df_result = analyze_stock_quantiles(
                start_date_str, end_date_str, n_quantiles, quantile_basis
            )

            if df_result is None:
                st.error("数据分析失败，请检查数据源或调整参数后重试。")
                return

            progress_bar.progress(60)
            status_text.text("正在计算统计数据...")

            # 计算统计数据
            stats_df = calculate_quantile_statistics(df_result, n_quantiles)

            progress_bar.progress(80)
            status_text.text("正在生成图表...")

            # 创建图表
            fig = create_plotly_charts(df_result, n_quantiles, quantile_basis)

            progress_bar.progress(90)
            status_text.text("正在计算相关性...")

            # 计算相关性
            correlations = calculate_correlations(df_result)

            progress_bar.progress(100)
            status_text.text("分析完成！")

            # 保存结果到session state
            st.session_state.analysis_results = {
                "df_result": df_result,
                "stats_df": stats_df,
                "fig": fig,
                "correlations": correlations,
                "start_date": start_date,
                "end_date": end_date,
                "n_quantiles": n_quantiles,
                "quantile_basis": quantile_basis,
                "start_date_str": start_date_str,
                "end_date_str": end_date_str,
            }
            st.session_state.analysis_params = current_params

            # 清除进度条
            progress_bar.empty()
            status_text.empty()

        # 从session state获取结果
        results = st.session_state.analysis_results
        if results is None:
            st.error("分析结果不可用，请重新分析。")
            return

        df_result = results["df_result"]
        stats_df = results["stats_df"]
        fig = results["fig"]

        # 显示结果
        st.markdown("---")
        st.subheader("📊 分析结果")

        # 总体统计
        col1, col2, col3, col4 = st.columns(4)

        total_stocks = len(df_result)
        overall_return = df_result["returns"].mean()
        best_return = df_result["returns"].max()
        worst_return = df_result["returns"].min()

        with col1:
            st.metric("总股票数量", f"{total_stocks:,}")
        with col2:
            st.metric("整体平均收益率", f"{overall_return:.2%}")
        with col3:
            st.metric("最高收益率", f"{best_return:.2%}")
        with col4:
            st.metric("最低收益率", f"{worst_return:.2%}")

        # 显示统计表格
        st.subheader("📈 分位统计表")
        st.dataframe(stats_df, width="stretch")

        # 相关性分析
        st.subheader("📊 相关性分析")

        correlations = results.get("correlations")

        if correlations:
            col1, col2, col3 = st.columns(3)

            with col1:
                corr_value = correlations["收益率_vs_市值"]
                color = "🟢" if corr_value > 0 else "🔴"
                st.metric(
                    "收益率 vs 市值相关性",
                    f"{corr_value:.3f}",
                    help="正相关表示大市值股票收益率更高，负相关表示小市值股票收益率更高",
                )

            with col2:
                corr_value = correlations["收益率_vs_PE"]
                color = (
                    "🟢" if corr_value < 0 else "🔴"
                )  # PE相关性通常应该是负的（低PE高收益）
                st.metric(
                    "收益率 vs PE相关性",
                    f"{corr_value:.3f}",
                    help="负相关表示低PE股票收益率更高，正相关表示高PE股票收益率更高",
                )

            with col3:
                corr_value = correlations["收益率_vs_换手率"]
                color = "🟢" if corr_value > 0 else "🔴"
                st.metric(
                    "收益率 vs 换手率相关性",
                    f"{corr_value:.3f}",
                    help="正相关表示高换手率股票收益率更高，负相关表示低换手率股票收益率更高",
                )

            # 相关性解释
            st.markdown("""
            **相关性解读：**
            - **相关系数范围**: -1 到 +1
            - **正相关**: 两个变量同向变化
            - **负相关**: 两个变量反向变化  
            - **相关系数绝对值 > 0.3**: 相关性较强
            - **相关系数绝对值 > 0.7**: 相关性很强
            """)
        else:
            st.warning("数据不足，无法进行相关性分析")

        # 显示交互式图表
        st.subheader("📊 可视化分析")
        st.plotly_chart(fig, width="stretch")

        # 详细数据展示
        with st.expander("📋 查看详细数据"):
            st.subheader("个股详细数据")

            # 选择要显示的分位
            quantile_options = sort_quantile_labels(df_result["quantile"].unique())
            selected_quantiles = st.multiselect(
                "选择要显示的分位",
                options=quantile_options,
                default=quantile_options[:3],
            )

            if selected_quantiles:
                filtered_df = df_result[df_result["quantile"].isin(selected_quantiles)]
                display_df = filtered_df[
                    [
                        "stock_code",
                        "stock_name",
                        "quantile",
                        "returns",
                        "market_cap",
                        "pe",
                        "turn",
                    ]
                ].copy()

                # 转换市值为亿元，但保持数值类型用于排序
                display_df["market_cap_billion"] = display_df["market_cap"] / 1e6
                display_df = display_df.drop("market_cap", axis=1)

                # 重命名列
                display_df.columns = [
                    "股票代码",
                    "股票名称",
                    "分位",
                    "收益率",
                    "前7日平均PE",
                    "前7日平均换手率",
                    "前7日平均市值(亿元)",
                ]

                # 重新排列列顺序
                display_df = display_df[
                    [
                        "股票代码",
                        "股票名称",
                        "分位",
                        "收益率",
                        "前7日平均市值(亿元)",
                        "前7日平均PE",
                        "前7日平均换手率",
                    ]
                ]

                # 使用st.dataframe，它支持数值排序并能自动格式化显示
                st.dataframe(
                    display_df,
                    width="stretch",
                    column_config={
                        "收益率": st.column_config.NumberColumn(
                            "收益率", help="股票收益率", format="%.4f"
                        ),
                        "前7日平均市值(亿元)": st.column_config.NumberColumn(
                            "前7日平均市值(亿元)",
                            help="股票前7日平均市值（亿元）",
                            format="%.2f",
                        ),
                        "前7日平均PE": st.column_config.NumberColumn(
                            "前7日平均PE", help="股票前7日平均PE比率", format="%.2f"
                        ),
                        "前7日平均换手率": st.column_config.NumberColumn(
                            "前7日平均换手率",
                            help="股票前7日平均换手率（%）",
                            format="%.2f",
                        ),
                    },
                )

        # 下载数据
        with st.expander("💾 下载数据"):
            col1, col2 = st.columns(2)

            with col1:
                # 统计摘要CSV
                csv_summary = stats_df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    label="下载统计摘要 (CSV)",
                    data=csv_summary,
                    file_name=f"stock_{results['n_quantiles']}quantile_{results['quantile_basis']}_summary_{results['start_date_str']}_{results['end_date_str']}.csv",
                    mime="text/csv",
                )

            with col2:
                # 详细数据CSV
                detail_df = df_result[
                    [
                        "stock_code",
                        "stock_name",
                        "quantile",
                        "returns",
                        "market_cap",
                        "pe",
                        "turn",
                        "initial_close",
                        "initial_date",
                    ]
                ].copy()
                detail_df["returns_pct"] = detail_df["returns"] * 100
                detail_df["market_cap_billion"] = detail_df["market_cap"] / 1e6

                csv_detail = detail_df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    label="下载详细数据 (CSV)",
                    data=csv_detail,
                    file_name=f"stock_{results['n_quantiles']}quantile_{results['quantile_basis']}_detailed_{results['start_date_str']}_{results['end_date_str']}.csv",
                    mime="text/csv",
                )

        # 分析洞察
        st.subheader(f"💡 分析洞察（按{results['quantile_basis']}分位）")

        # 找出表现最好和最差的股票
        best_stock = df_result.loc[df_result["returns"].idxmax()]
        worst_stock = df_result.loc[df_result["returns"].idxmin()]

        col1, col2 = st.columns(2)

        with col1:
            st.success(f"""
            **表现最好的股票:**
            - 股票名称: {best_stock["stock_name"]} ({best_stock["stock_code"]})
            - 收益率: {best_stock["returns"]:.2%}
            - 所属分位: {best_stock["quantile"]}
            """)

        with col2:
            st.error(f"""
            **表现最差的股票:**
            - 股票名称: {worst_stock["stock_name"]} ({worst_stock["stock_code"]})
            - 收益率: {worst_stock["returns"]:.2%}
            - 所属分位: {worst_stock["quantile"]}
            """)

    else:
        # 默认显示说明
        st.markdown("""
        ### 🎯 程序功能
        
        本程序可以分析沪深A股在指定时间段内的分位情况，支持按收益率、市值、PE或换手率进行分位划分，主要功能包括：
        
        1. **📊 多维度分位分析**: 将所有股票按收益率、市值、PE或换手率分成N个等级
        2. **📈 统计计算**: 计算各分位的平均收益率、平均市值、平均PE、平均换手率等指标
        3. **🎨 可视化展示**: 提供交互式图表展示分析结果
        4. **💾 数据导出**: 支持下载分析结果和详细数据
        
        ### 🚀 使用方法
        
        1. 在左侧边栏设置分析参数（时间范围、分位数、分位依据）
        2. 点击"开始分析"按钮
        3. 查看分析结果和可视化图表
        4. 可选择下载相关数据
        
        ### 📝 分析说明
        
        - **多维度分位分析**: 支持按收益率、市值或PE进行分位划分，探索不同特征与收益的关系
        - **期间平均特征 vs 最终收益率**: 分析基于股票的前7日平均特征（如平均市值、平均PE、平均换手率）与最终收益率的关系
        - **等频分位**: 使用pd.qcut进行等频分位，确保每个分位包含相同数量的股票
        - **数据过滤**: 自动过滤异常数据，确保分析结果的可靠性
        """)


if __name__ == "__main__":
    main()
