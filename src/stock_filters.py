from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd


@dataclass
class StockFilterContext:
    """过滤上下文，保存调仓日和全量行情数据。"""

    trade_date: pd.Timestamp
    universe_df: pd.DataFrame
    listed_dates: Optional[pd.Series] = None


class BaseStockFilter:
    """股票过滤器基类。"""

    name = "base_filter"

    def apply(
        self, candidates: pd.DataFrame, context: StockFilterContext
    ) -> pd.DataFrame:
        raise NotImplementedError("子类必须实现 apply 方法")

    def description(self) -> str:
        return self.name


class MarketCapRangeFilter(BaseStockFilter):
    """按市值范围过滤。市值单位统一为亿元。"""

    name = "market_cap_range"

    def __init__(
        self, min_cap: Optional[float] = None, max_cap: Optional[float] = None
    ):
        self.min_cap = min_cap
        self.max_cap = max_cap

    def _resolve_market_cap(self, df: pd.DataFrame) -> pd.Series:
        if "market_cap" in df.columns:
            market_cap = pd.to_numeric(df["market_cap"], errors="coerce")
            if isinstance(market_cap, pd.Series):
                return market_cap
            return pd.Series(market_cap, index=df.index)
        if "market" in df.columns:
            market_cap = pd.to_numeric(df["market"], errors="coerce")
            if isinstance(market_cap, pd.Series):
                return market_cap
            return pd.Series(market_cap, index=df.index)
        if "amount" in df.columns and "turn" in df.columns:
            turn = df["turn"].replace(0, np.nan)
            market_cap = df["amount"] / turn / 1e6
            return pd.Series(market_cap, index=df.index)
        return pd.Series(np.nan, index=df.index)

    def apply(
        self, candidates: pd.DataFrame, context: StockFilterContext
    ) -> pd.DataFrame:
        if candidates.empty:
            return candidates

        result = candidates.copy()
        result["_market_cap_filter_tmp"] = self._resolve_market_cap(result)

        lower = -np.inf if self.min_cap is None else float(self.min_cap)
        upper = np.inf if self.max_cap is None else float(self.max_cap)
        mask = result["_market_cap_filter_tmp"].between(lower, upper, inclusive="both")

        result = result.loc[mask].drop(columns=["_market_cap_filter_tmp"])
        return result

    def description(self) -> str:
        return f"市值范围过滤: [{self.min_cap}, {self.max_cap}] 亿元"


class ListingAgeFilter(BaseStockFilter):
    """按上市时长过滤。按自然日计算。"""

    name = "listing_age"

    def __init__(self, min_days: int = 365):
        self.min_days = int(min_days)

    def _resolve_listed_dates(self, context: StockFilterContext) -> pd.Series:
        if context.listed_dates is None:
            return pd.Series(dtype="datetime64[ns]")

        listed_raw = context.listed_dates
        if not isinstance(listed_raw, pd.Series):
            listed_raw = pd.Series(listed_raw)

        if pd.api.types.is_datetime64_any_dtype(listed_raw):
            return listed_raw

        text = listed_raw.astype(str).str.strip()
        text = text.replace({"": np.nan, "None": np.nan, "nan": np.nan, "NaT": np.nan})

        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
        fallback_mask = parsed.isna() & text.notna()
        if fallback_mask.any():
            parsed.loc[fallback_mask] = pd.to_datetime(
                text.loc[fallback_mask], errors="coerce"
            )

        return parsed

    def apply(
        self, candidates: pd.DataFrame, context: StockFilterContext
    ) -> pd.DataFrame:
        if candidates.empty:
            return candidates

        listed_dates = self._resolve_listed_dates(context)
        if listed_dates.empty:
            return candidates.iloc[0:0]

        result = candidates.copy()
        listed_dates_df = listed_dates.rename("_listed_date_tmp").reset_index()
        result = result.merge(listed_dates_df, on="stock_code", how="left")

        trade_date = pd.Timestamp(context.trade_date)
        listing_days = (trade_date - result["_listed_date_tmp"]).dt.days
        eligible_mask = listing_days >= self.min_days

        result = result.loc[eligible_mask].drop(columns=["_listed_date_tmp"])
        return result

    def description(self) -> str:
        return f"上市时长过滤: 至少 {self.min_days} 个自然日(仅listed_date)"


class StockFilterPipeline:
    """可扩展的股票过滤器管道。"""

    def __init__(self, filters: Optional[List[BaseStockFilter]] = None):
        self.filters = filters or []

    def add_filter(self, filter_obj: BaseStockFilter):
        self.filters.append(filter_obj)

    def apply(
        self, candidates: pd.DataFrame, context: StockFilterContext
    ) -> pd.DataFrame:
        result = candidates
        for filter_obj in self.filters:
            result = filter_obj.apply(result, context)
            if result.empty:
                break
        return result

    def get_filter_descriptions(self) -> List[str]:
        return [f.description() for f in self.filters]

    def __bool__(self):
        return bool(self.filters)
