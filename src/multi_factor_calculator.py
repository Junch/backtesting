from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from sklearn.preprocessing import StandardScaler

from single_factor_analysis import FactorCalculator
from stock_filters import StockFilterContext, StockFilterPipeline


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
