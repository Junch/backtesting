import argparse
import datetime
import math
import os
import re
import time
from pathlib import Path

from xtquant import xtconstant, xtdata
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


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


def infer_limit_pct(stock_code: str, stock_name: str) -> float:
    code = stock_code.upper()
    name = stock_name.upper()

    if code.endswith(".BJ"):
        return 0.30

    if code.endswith(".SH") and code.startswith("688"):
        return 0.20

    if code.endswith(".SZ") and code.startswith("300"):
        return 0.20

    if "ST" in name:
        return 0.05

    return 0.10


def quantize_price(value: float, tick: float, direction: str) -> float:
    if value <= 0:
        return 0.0
    if tick <= 0:
        tick = 0.01

    units = value / tick
    if direction == "买入":
        q = math.floor(units)
    else:
        q = math.ceil(units)
    return round(q * tick, 3)


def calculate_limit_price(
    stock_code: str, direction: str, stock_name: str
) -> tuple[float, str]:
    detail = xtdata.get_instrument_detail(stock_code) or {}

    up_stop = detail.get("UpStopPrice")
    down_stop = detail.get("DownStopPrice")
    pre_close = detail.get("PreClose")
    price_tick = detail.get("PriceTick", 0.01)

    if direction == "买入" and isinstance(up_stop, (int, float)) and up_stop > 0:
        return quantize_price(
            float(up_stop), float(price_tick), direction
        ), "UpStopPrice"
    if direction == "卖出" and isinstance(down_stop, (int, float)) and down_stop > 0:
        return quantize_price(
            float(down_stop), float(price_tick), direction
        ), "DownStopPrice"

    if not isinstance(pre_close, (int, float)) or pre_close <= 0:
        raise ValueError(f"{stock_code} 无法获取有效昨收价，不能计算涨跌停")

    pct = infer_limit_pct(stock_code, stock_name)
    if direction == "买入":
        target = pre_close * (1 + pct)
    else:
        target = pre_close * (1 - pct)

    return quantize_price(
        float(target), float(price_tick), direction
    ), "PreCloseFallback"


def calculate_current_price(stock_code: str, direction: str) -> tuple[float, str]:
    ticks = xtdata.get_full_tick([stock_code]) or {}
    tick_item = ticks.get(stock_code) or {}

    if not isinstance(tick_item, dict):
        raise ValueError(f"{stock_code} 无法获取实时行情")

    primary_key = "askPrice1" if direction == "买入" else "bidPrice1"
    primary_value = tick_item.get(primary_key)
    fallback_value = tick_item.get("lastPrice")

    detail = xtdata.get_instrument_detail(stock_code) or {}
    price_tick = detail.get("PriceTick", 0.01)

    if isinstance(primary_value, (int, float)) and primary_value > 0:
        return (
            quantize_price(float(primary_value), float(price_tick), direction),
            primary_key,
        )

    if isinstance(fallback_value, (int, float)) and fallback_value > 0:
        return (
            quantize_price(float(fallback_value), float(price_tick), direction),
            "lastPriceFallback",
        )

    raise ValueError(f"{stock_code} 无法获取有效实时价格")


def parse_order_line(line: str, line_no: int) -> dict:
    text = line.strip()
    if not text:
        raise ValueError("空行")

    cols = re.split(r"\s+", text)
    if len(cols) < 5:
        raise ValueError(f"字段不足，至少需要5列，当前{len(cols)}列")

    date_text = cols[0]
    direction = cols[1]
    stock_code = cols[2].upper()
    stock_name = " ".join(cols[3:-1]).strip()
    volume_text = cols[-1]

    if direction not in {"买入", "卖出"}:
        raise ValueError(f"方向不合法: {direction}")

    if not re.match(r"^\d{6}\.(SH|SZ|BJ)$", stock_code):
        raise ValueError(f"股票代码格式不合法: {stock_code}")

    if not stock_name:
        raise ValueError("股票名称为空")

    try:
        volume = int(volume_text)
    except ValueError as exc:
        raise ValueError(f"数量不是整数: {volume_text}") from exc

    if volume <= 0:
        raise ValueError(f"数量必须大于0: {volume}")

    return {
        "line_no": line_no,
        "raw": text,
        "date": date_text,
        "direction": direction,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "volume": volume,
    }


def parse_orders(file_path: Path) -> list[dict]:
    lines = file_path.read_text(encoding="utf-8").splitlines()
    orders: list[dict] = []

    for idx, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        order = parse_order_line(line, idx)
        orders.append(order)

    if not orders:
        raise ValueError("订单文件为空")

    return orders


def get_available_volume_map(
    xt_trader: XtQuantTrader, account: StockAccount
) -> dict[str, int]:
    positions = xt_trader.query_stock_positions(account) or []
    volume_map: dict[str, int] = {}
    for p in positions:
        code = getattr(p, "stock_code", "")
        can_use = int(getattr(p, "can_use_volume", 0) or 0)
        if code:
            volume_map[code.upper()] = can_use
    return volume_map


def connect_trader(
    qmt_path: str, account_id: str
) -> tuple[XtQuantTrader, StockAccount]:
    session_id = int(time.time())
    account = StockAccount(account_id, "STOCK")
    if not isinstance(account, StockAccount):
        raise RuntimeError(f"创建账户对象失败: {account}")

    xt_trader = XtQuantTrader(qmt_path, session_id)
    xt_trader.start()

    connect_result = xt_trader.connect()
    if connect_result != 0:
        raise RuntimeError(f"连接QMT失败，错误码: {connect_result}")

    subscribe_result = xt_trader.subscribe(account)
    if subscribe_result != 0:
        raise RuntimeError(f"订阅账户失败，错误码: {subscribe_result}")

    return xt_trader, account


def execute_orders(
    orders: list[dict],
    xt_trader: XtQuantTrader,
    account: StockAccount,
    dry_run: bool,
    side_filter: str,
    on_error: str,
    price_mode: str,
) -> None:
    available_map = get_available_volume_map(xt_trader, account)
    strategy_name = "order_txt_batch"
    batch_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    for order in orders:
        direction = order["direction"]
        stock_code = order["stock_code"]
        stock_name = order["stock_name"]
        volume = order["volume"]
        line_no = order["line_no"]

        if side_filter == "buy" and direction != "买入":
            print(f"[第{line_no}行][跳过] side=buy，仅执行买入")
            continue

        if side_filter == "sell" and direction != "卖出":
            print(f"[第{line_no}行][跳过] side=sell，仅执行卖出")
            continue

        if direction == "买入" and volume % 100 != 0:
            print(
                f"[第{line_no}行][拒绝] {stock_code} 买入数量需为100整数倍，当前: {volume}"
            )
            if on_error == "stop":
                print(f"[第{line_no}行][停止] on-error=stop，终止后续订单")
                break
            continue

        if direction == "卖出":
            can_use = available_map.get(stock_code, 0)
            if volume > can_use:
                print(
                    f"[第{line_no}行][拒绝] {stock_code} 可卖{can_use}，请求卖出{volume}"
                )
                if on_error == "stop":
                    print(f"[第{line_no}行][停止] on-error=stop，终止后续订单")
                    break
                continue

        try:
            if price_mode == "current":
                limit_price, source = calculate_current_price(stock_code, direction)
            else:
                limit_price, source = calculate_limit_price(
                    stock_code, direction, stock_name
                )
        except Exception as exc:
            print(f"[第{line_no}行][拒绝] {stock_code} 价格计算失败: {exc}")
            if on_error == "stop":
                print(f"[第{line_no}行][停止] on-error=stop，终止后续订单")
                break
            continue

        order_type = (
            xtconstant.STOCK_BUY if direction == "买入" else xtconstant.STOCK_SELL
        )
        price_type = xtconstant.FIX_PRICE
        remark = f"from_order_txt_{batch_ts}_L{line_no}"

        print(
            f"[第{line_no}行][准备] {direction} {stock_code} {stock_name} 数量{volume} 限价{limit_price:.3f} 模式{price_mode} 来源{source}"
        )

        if dry_run:
            print(f"[第{line_no}行][DRY-RUN] 未实际下单")
            continue

        result = xt_trader.order_stock(
            account,
            stock_code,
            order_type,
            volume,
            price_type,
            limit_price,
            strategy_name=strategy_name,
            order_remark=remark,
        )

        if isinstance(result, int) and result > 0:
            print(f"[第{line_no}行][成功] 委托编号: {result}")
            if direction == "卖出":
                available_map[stock_code] = max(
                    0, available_map.get(stock_code, 0) - volume
                )
        else:
            print(f"[第{line_no}行][失败] 返回值: {result}")
            if on_error == "stop":
                print(f"[第{line_no}行][停止] on-error=stop，终止后续订单")
                break


def main() -> None:
    parser = argparse.ArgumentParser(description="根据order.txt批量下单（忽略日期）")
    parser.add_argument(
        "--file",
        default="order.txt",
        help="订单文件路径，默认: order.txt",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅解析和计算价格，不实际下单",
    )
    parser.add_argument(
        "--side",
        choices=["all", "buy", "sell"],
        default="all",
        help="执行方向过滤: all(默认)/buy/sell",
    )
    parser.add_argument(
        "--on-error",
        choices=["continue", "stop"],
        default="continue",
        help="单笔失败时行为: continue(默认)/stop",
    )
    parser.add_argument(
        "--price-mode",
        choices=["limit", "current"],
        default="limit",
        help="价格模式: limit(默认，涨跌停逻辑)/current(当前市价)",
    )
    args = parser.parse_args()

    load_env_vars()
    account_id = os.getenv("QMT_ACCOUNT")
    qmt_path = os.getenv("QMT_PATH", r"C:\国金证券QMT交易端\userdata_mini")

    order_file = Path(args.file)
    if not order_file.is_absolute():
        order_file = Path.cwd() / order_file

    if not order_file.exists():
        raise FileNotFoundError(f"订单文件不存在: {order_file}")

    orders = parse_orders(order_file)
    print(f"读取订单 {len(orders)} 条，文件: {order_file}")
    print("注意: 已忽略订单中的日期字段")

    xt_trader, account = connect_trader(qmt_path, account_id)
    print(f"QMT连接成功，账户: {account_id}")

    execute_orders(
        orders,
        xt_trader,
        account,
        args.dry_run,
        args.side,
        args.on_error,
        args.price_mode,
    )


if __name__ == "__main__":
    main()
