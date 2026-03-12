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
        self.neutralization_config: Dict[str, Dict[str, bool]] = {}
        self.industry_col = "industry_sw1"
        self.industry_map: Dict[str, str] = {}
        self.min_neutralization_samples = 10

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

    def set_neutralization_config(
        self, factor_name: str, industry: bool = False, market_cap: bool = False
    ):
        """设置单个因子的中性化配置。"""
        self.neutralization_config[factor_name] = {
            "industry": bool(industry),
            "market_cap": bool(market_cap),
        }

    def set_industry_column(self, industry_col: str):
        """设置行业字段（industry_sw1 或 industry_sw2）。"""
        self.industry_col = industry_col

    def set_industry_map(self, industry_map: Optional[Dict[str, str]]):
        """设置行业映射（stock_code -> 行业）。"""
        self.industry_map = industry_map or {}

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

        # 市值对数处理，并准备中性化所需的对数市值字段
        df_result = self._prepare_log_market_cap(df_result)

        # 因子中性化（可选）
        if self.neutralization_config:
            print("正在进行因子中性化...")
            df_result = self._neutralize_factors(df_result)

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

    def _prepare_log_market_cap(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        准备对数市值字段：
        1) 市值因子本身进行对数变换（满足“市值因子做对数处理”）
        2) 生成中性化用的 _neutralize_log_market_cap
        """
        df_result = df.copy()

        market_cap_series = self._resolve_market_cap_series(df_result)
        market_cap_series = pd.to_numeric(market_cap_series, errors="coerce")

        log_market_cap = pd.Series(np.nan, index=df_result.index, dtype=float)
        valid_market_cap = market_cap_series > 0
        log_market_cap.loc[valid_market_cap] = np.log(market_cap_series.loc[valid_market_cap])
        df_result["_neutralize_log_market_cap"] = log_market_cap

        market_factor_col = None
        for factor_calculator in self.factor_calculators.values():
            if factor_calculator.name == "市值":
                market_factor_col = factor_calculator.get_factor_column()
                break

        if market_factor_col and market_factor_col in df_result.columns:
            factor_values = pd.to_numeric(df_result[market_factor_col], errors="coerce")
            log_factor_values = pd.Series(np.nan, index=df_result.index, dtype=float)
            valid_factor_values = factor_values > 0
            log_factor_values.loc[valid_factor_values] = np.log(
                factor_values.loc[valid_factor_values]
            )
            df_result[market_factor_col] = log_factor_values

        return df_result

    def _resolve_market_cap_series(self, df: pd.DataFrame) -> pd.Series:
        """按优先级解析市值序列（亿元）。"""
        if "market" in df.columns:
            return df["market"]
        if "market_cap" in df.columns:
            return df["market_cap"]
        if "amount" in df.columns and "turn" in df.columns:
            turn = pd.to_numeric(df["turn"], errors="coerce").replace(0, np.nan)
            amount = pd.to_numeric(df["amount"], errors="coerce")
            return amount / turn / 1e6
        return pd.Series(np.nan, index=df.index, dtype=float)

    def _neutralize_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """按交易日进行横截面中性化，使用残差作为中性化后因子值。"""
        df_result = df.copy()

        if self.industry_map and "stock_code" in df_result.columns:
            df_result[self.industry_col] = df_result["stock_code"].map(self.industry_map)

        factor_col_to_name = {
            factor_calculator.get_factor_column(): factor_name
            for factor_name, factor_calculator in self.factor_calculators.items()
        }

        for trade_date in df_result["trade_date"].dropna().unique():
            date_mask = df_result["trade_date"] == trade_date
            date_df = df_result.loc[date_mask]

            for factor_col, factor_name in factor_col_to_name.items():
                if factor_col not in date_df.columns:
                    continue

                config = self.neutralization_config.get(factor_name, {})
                use_industry = bool(config.get("industry", False))
                use_market_cap = bool(config.get("market_cap", False))

                # 市值因子不对市值本身做中性化，避免自回归
                if factor_name == "市值因子":
                    use_market_cap = False

                if not use_industry and not use_market_cap:
                    continue

                y = pd.to_numeric(date_df[factor_col], errors="coerce")
                valid_mask = y.notna()

                model_parts = []

                if use_industry and self.industry_col in date_df.columns:
                    industry_series = date_df[self.industry_col]
                    valid_mask = valid_mask & industry_series.notna()
                    industry_dummies = pd.get_dummies(
                        industry_series.astype(str),
                        prefix="ind",
                        drop_first=True,
                        dtype=float,
                    )
                    if not industry_dummies.empty:
                        model_parts.append(industry_dummies)

                if use_market_cap and "_neutralize_log_market_cap" in date_df.columns:
                    log_market_cap = pd.to_numeric(
                        date_df["_neutralize_log_market_cap"], errors="coerce"
                    )
                    valid_mask = valid_mask & log_market_cap.notna()
                    model_parts.append(pd.DataFrame({"log_market_cap": log_market_cap}))

                if valid_mask.sum() < self.min_neutralization_samples:
                    continue

                if not model_parts:
                    continue

                x_df = pd.concat(model_parts, axis=1).loc[valid_mask]
                y_fit = y.loc[valid_mask]

                x_df = x_df.dropna()
                y_fit = y_fit.loc[x_df.index]

                if len(x_df) < self.min_neutralization_samples:
                    continue

                # 样本数需大于参数数，避免欠定问题
                if x_df.shape[0] <= (x_df.shape[1] + 1):
                    continue

                x_np = x_df.to_numpy(dtype=float)
                intercept = np.ones((x_np.shape[0], 1), dtype=float)
                x_with_intercept = np.concatenate([intercept, x_np], axis=1)
                y_np = y_fit.to_numpy(dtype=float)

                try:
                    beta, *_ = np.linalg.lstsq(x_with_intercept, y_np, rcond=None)
                except np.linalg.LinAlgError:
                    continue

                residuals = y_np - x_with_intercept @ beta
                df_result.loc[y_fit.index, factor_col] = residuals

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
    industry_map: Optional[Dict[str, str]] = None,
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
    if industry_map:
        multi_factor_calculator.set_industry_map(industry_map)

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

        # 仅使用当日可见信息生成信号，避免读取下一交易日数据导致前视偏差
        valid_stocks = df[
            (df["trade_date"] == all_trade_dates[i])
            & (df[composite_factor_col].notna())
        ]  # 过滤掉复合因子值为空的股票

        if filter_pipeline:
            filter_context = StockFilterContext(
                trade_date=pd.Timestamp(all_trade_dates[i]),
                universe_df=df,
                listed_dates=listed_dates,
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
