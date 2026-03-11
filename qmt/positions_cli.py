import argparse
import csv
import datetime
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def load_xtquant_components() -> tuple[Any, Any, Any]:
    try:
        from xtquant import xtdata
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount
        xtdata.enable_hello = False
    except ImportError as exc:
        raise RuntimeError("缺少依赖 xtquant，请先在可用环境安装并确保QMT接口可用") from exc

    return xtdata, XtQuantTrader, StockAccount


def load_env_vars() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"

    if load_dotenv is not None:
        load_dotenv(dotenv_path=env_path)
        return

    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def connect_trader(
    qmt_path: str,
    account_id: str,
    trader_cls: Any,
    account_cls: Any,
) -> tuple[Any, Any]:
    session_id = int(time.time())
    account = account_cls(account_id, "STOCK")
    if not isinstance(account, account_cls):
        raise RuntimeError(f"创建账户对象失败: {account}")

    xt_trader = trader_cls(qmt_path, session_id)
    xt_trader.start()

    connect_result = xt_trader.connect()
    if connect_result != 0:
        raise RuntimeError(f"连接QMT失败，错误码: {connect_result}")

    subscribe_result = xt_trader.subscribe(account)
    if subscribe_result != 0:
        raise RuntimeError(f"订阅账户失败，错误码: {subscribe_result}")

    return xt_trader, account


def get_stock_name(stock_code: str, xtdata_api: Any) -> str:
    detail = xtdata_api.get_instrument_detail(stock_code) or {}
    name = detail.get("InstrumentName")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return "N/A"


def query_positions(xt_trader: Any, account: Any, xtdata_api: Any) -> list[dict]:
    raw_positions = xt_trader.query_stock_positions(account) or []
    rows: list[dict] = []

    for p in raw_positions:
        stock_code = str(getattr(p, "stock_code", "") or "").upper()
        volume = int(getattr(p, "volume", 0) or 0)
        can_use_volume = int(getattr(p, "can_use_volume", 0) or 0)
        last_price = float(getattr(p, "last_price", 0.0) or 0.0)
        market_value = float(getattr(p, "market_value", 0.0) or 0.0)

        if not stock_code:
            continue
        if volume <= 0 and can_use_volume <= 0:
            continue

        rows.append(
            {
                "stock_code": stock_code,
                "stock_name": get_stock_name(stock_code, xtdata_api),
                "volume": volume,
                "can_use_volume": can_use_volume,
                "last_price": last_price,
                "market_value": market_value,
            }
        )

    rows.sort(key=lambda x: x["stock_code"])
    return rows


def query_asset_summary(xt_trader: Any, account: Any) -> dict:
    asset = xt_trader.query_stock_asset(account)
    if asset is None:
        return {
            "total_asset": 0.0,
            "market_value": 0.0,
            "cash": 0.0,
        }

    return {
        "total_asset": float(getattr(asset, "total_asset", 0.0) or 0.0),
        "market_value": float(getattr(asset, "market_value", 0.0) or 0.0),
        "cash": float(getattr(asset, "cash", 0.0) or 0.0),
    }


def print_asset_summary(asset_summary: dict) -> None:
    print("资产汇总:")
    print(f"  总资产: {asset_summary['total_asset']:,.2f}")
    print(f"  持仓市值: {asset_summary['market_value']:,.2f}")
    print(f"  现金: {asset_summary['cash']:,.2f}")
    print()


def print_positions(rows: list[dict]) -> None:
    if not rows:
        print("当前无持仓")
        return

    header = (
        f"{'证券代码':<12} {'证券名称':<12} {'总持仓':>8} {'可卖数量':>8} "
        f"{'最新价':>10} {'市值':>14}"
    )
    print(header)
    print("-" * len(header))

    for item in rows:
        print(
            f"{item['stock_code']:<12} "
            f"{item['stock_name']:<12} "
            f"{item['volume']:>8d} "
            f"{item['can_use_volume']:>8d} "
            f"{item['last_price']:>10.3f} "
            f"{item['market_value']:>14.2f}"
        )


def export_sell_all_orders(rows: list[dict], output_path: Path) -> int:
    sellable = [item for item in rows if item["can_use_volume"] > 0]
    if not sellable:
        return 0

    today_text = datetime.date.today().strftime("%Y-%m-%d")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for item in sellable:
            writer.writerow(
                [
                    today_text,
                    "卖出",
                    item["stock_code"],
                    item["stock_name"],
                    int(item["can_use_volume"]),
                ]
            )

    return len(sellable)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查询当前持仓，并可导出清仓卖出订单CSV"
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="导出全部可卖持仓为卖出订单CSV",
    )
    parser.add_argument(
        "--output",
        default="order_sell_all.csv",
        help="导出文件路径，默认: order_sell_all.csv",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        load_env_vars()
        account_id = os.getenv("QMT_ACCOUNT")
        qmt_path = os.getenv("QMT_PATH", r"C:\国金证券QMT交易端\userdata_mini")

        if not account_id:
            raise RuntimeError("缺少环境变量 QMT_ACCOUNT")

        xtdata_api, trader_cls, account_cls = load_xtquant_components()
        xt_trader, account = connect_trader(
            qmt_path,
            account_id,
            trader_cls,
            account_cls,
        )
        print(f"QMT连接成功，账户: {account_id}")

        asset_summary = query_asset_summary(xt_trader, account)
        print_asset_summary(asset_summary)

        positions = query_positions(xt_trader, account, xtdata_api)
        print_positions(positions)

        if args.export:
            output_path = Path(args.output)
            if not output_path.is_absolute():
                output_path = Path.cwd() / output_path

            count = export_sell_all_orders(positions, output_path)
            if count <= 0:
                print("无可卖持仓，未生成导出文件")
            else:
                print(f"已导出清仓卖单: {output_path}")
                print(f"导出记录数: {count}")

        return 0
    except Exception as exc:
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
