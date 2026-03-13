from __future__ import annotations

from datetime import date, datetime
import os
import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import yaml


def _to_plain(value: Any) -> Any:
    """Convert pandas/numpy/date values into YAML-safe plain python values."""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return pd.Timestamp(value).strftime("%Y-%m-%d")
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, dict):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_plain(v) for v in value]
    return value


def build_strategy_config(
    *,
    selected_factors: Dict[str, float],
    factor_params: Dict[str, Dict[str, Any]],
    enable_factor_neutralization: bool,
    neutralization_industry_col: str,
    neutralization_config: Dict[str, Dict[str, bool]],
    sector_name: str,
    start_date: date,
    end_date: date,
    rebalance_period: int,
    hold_top: int,
    standardize_factors: bool,
    enable_market_cap_filter: bool,
    min_market_cap: float,
    max_market_cap: float,
    enable_listing_age_filter: bool,
    listing_min_days: Optional[int],
    enable_stop_loss: bool,
    stop_loss_pct: Optional[float],
    trailing_stop: bool,
) -> Dict[str, Any]:
    factors: Dict[str, Dict[str, Any]] = {}
    for factor_name, weight in selected_factors.items():
        factors[factor_name] = {
            "weight": float(weight),
            "params": _to_plain(factor_params.get(factor_name, {})),
        }

    return {
        "strategy_type": "multi_factor",
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "factors": factors,
        "neutralization": {
            "enabled": bool(enable_factor_neutralization),
            "industry_column": neutralization_industry_col,
            "per_factor": _to_plain(neutralization_config),
        },
        "backtest_params": {
            "sector": sector_name,
            "start_date": _to_plain(start_date),
            "end_date": _to_plain(end_date),
            "rebalance_period": int(rebalance_period),
            "hold_top": int(hold_top),
            "standardize": bool(standardize_factors),
        },
        "filters": {
            "market_cap": {
                "enabled": bool(enable_market_cap_filter),
                "min_billion": float(min_market_cap),
                "max_billion": float(max_market_cap),
            },
            "listing_age": {
                "enabled": bool(enable_listing_age_filter),
                "min_days": int(listing_min_days) if listing_min_days is not None else None,
            },
        },
        "risk": {
            "stop_loss": {
                "enabled": bool(enable_stop_loss),
                "percentage": float(stop_loss_pct * 100)
                if stop_loss_pct is not None
                else None,
                "trailing": bool(trailing_stop),
            }
        },
    }


def build_backtest_results(
    *,
    initial_value: float,
    final_value: float,
    total_return_pct: float,
    annual_return_pct: Optional[float],
    sharpe_ratio: Optional[float],
    max_drawdown_pct: Optional[float],
    max_drawdown_days: Optional[int],
    loaded_stock_count: int,
    summary_data: Optional[Dict[str, Any]] = None,
    total_commission: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "initial_value": float(initial_value),
        "final_value": float(final_value),
        "total_return_pct": float(total_return_pct),
        "annual_return_pct": float(annual_return_pct)
        if annual_return_pct is not None
        else None,
        "sharpe_ratio": float(sharpe_ratio) if sharpe_ratio is not None else None,
        "max_drawdown_pct": float(max_drawdown_pct)
        if max_drawdown_pct is not None
        else None,
        "max_drawdown_days": int(max_drawdown_days)
        if max_drawdown_days is not None
        else None,
        "loaded_stock_count": int(loaded_stock_count),
        "total_commission": float(total_commission)
        if total_commission is not None
        else None,
        "summary": _to_plain(summary_data) if summary_data else None,
    }


def _sanitize_filename_part(text: str) -> str:
    text = text.strip() or "strategy"
    return re.sub(r'[<>:"/\\|?*]+', "_", text)


def save_strategy_yaml(
    *,
    directory: str,
    strategy_config: Dict[str, Any],
    results: Optional[Dict[str, Any]] = None,
) -> str:
    os.makedirs(directory, exist_ok=True)

    payload = dict(strategy_config)
    if results is not None:
        payload["results"] = _to_plain(results)

    sector = payload.get("backtest_params", {}).get("sector", "strategy")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{_sanitize_filename_part(str(sector))}_{timestamp}.yaml"
    file_path = os.path.join(directory, filename)

    with open(file_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, allow_unicode=True, sort_keys=False)

    return file_path


def load_strategy_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise ValueError("配置文件格式无效，顶层应为字典")

    return data


def list_saved_strategies(directory: str) -> List[Dict[str, Any]]:
    if not os.path.isdir(directory):
        return []

    items: List[Dict[str, Any]] = []
    for filename in os.listdir(directory):
        if not filename.lower().endswith((".yaml", ".yml")):
            continue

        path = os.path.join(directory, filename)
        try:
            payload = load_strategy_yaml(path)
        except Exception:
            continue

        backtest = payload.get("backtest_params", {})
        results = payload.get("results", {})
        sector = backtest.get("sector", "未知板块")
        start_date = backtest.get("start_date", "")
        end_date = backtest.get("end_date", "")
        total_return = results.get("total_return_pct")
        sharpe = results.get("sharpe_ratio")

        metrics = []
        if total_return is not None:
            metrics.append(f"收益 {float(total_return):.2f}%")
        if sharpe is not None:
            metrics.append(f"夏普 {float(sharpe):.3f}")
        metrics_text = f" | {'; '.join(metrics)}" if metrics else ""

        label = f"{filename} | {sector} | {start_date}~{end_date}{metrics_text}"
        items.append(
            {
                "path": path,
                "filename": filename,
                "label": label,
                "saved_at": payload.get("saved_at"),
            }
        )

    items.sort(key=lambda x: x["filename"], reverse=True)
    return items
