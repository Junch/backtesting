import argparse
import os
from pathlib import Path

from xtquant import xtconstant
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


def connect_trader(qmt_path: str, account_id: str) -> tuple[XtQuantTrader, StockAccount]:
    import time

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


def _query_orders_raw(xt_trader: XtQuantTrader, account: StockAccount) -> list:
    candidate_names = ["query_stock_orders", "query_orders"]
    last_error: Exception | None = None

    for name in candidate_names:
        method = getattr(xt_trader, name, None)
        if method is None:
            continue

        try:
            data = method(account)
        except TypeError:
            data = method()
        except Exception as exc:
            last_error = exc
            continue

        if data is None:
            return []
        if isinstance(data, list):
            return data
        return list(data)

    if last_error is not None:
        raise RuntimeError(f"查询委托失败: {last_error}") from last_error

    raise RuntimeError("当前xtquant版本未提供可用的委托查询接口")


def _order_direction_text(order: object) -> str:
    direction = getattr(order, "order_type", None)
    if direction == getattr(xtconstant, "STOCK_BUY", object()):
        return "买入"
    if direction == getattr(xtconstant, "STOCK_SELL", object()):
        return "卖出"

    # 某些版本字段名可能是direction
    direction = getattr(order, "direction", None)
    if direction == getattr(xtconstant, "STOCK_BUY", object()):
        return "买入"
    if direction == getattr(xtconstant, "STOCK_SELL", object()):
        return "卖出"

    return "未知"


def _matches_side(side_filter: str, direction_text: str) -> bool:
    if side_filter == "all":
        return True
    if side_filter == "buy":
        return direction_text == "买入"
    if side_filter == "sell":
        return direction_text == "卖出"
    return False


def query_orders(
    xt_trader: XtQuantTrader,
    account: StockAccount,
    side_filter: str,
) -> list[dict]:
    raw_orders = _query_orders_raw(xt_trader, account)
    selected: list[dict] = []
    seen_order_ids: set[int] = set()

    for order in raw_orders:
        order_id = getattr(order, "order_id", None)
        if not isinstance(order_id, int) or order_id <= 0:
            continue
        if order_id in seen_order_ids:
            continue

        direction_text = _order_direction_text(order)
        if not _matches_side(side_filter, direction_text):
            continue

        seen_order_ids.add(order_id)
        selected.append(
            {
                "order": order,
                "order_id": order_id,
                "stock_code": str(getattr(order, "stock_code", "") or ""),
                "direction": direction_text,
                "volume": int(getattr(order, "order_volume", 0) or 0),
                "status": str(
                    getattr(order, "order_status", getattr(order, "status", ""))
                ),
            }
        )

    return selected


def _cancel_one_order(
    xt_trader: XtQuantTrader,
    account: StockAccount,
    order_id: int,
) -> object:
    candidate_names = ["cancel_order_stock", "cancel_order", "cancel_stock_order"]
    last_error: Exception | None = None

    for name in candidate_names:
        method = getattr(xt_trader, name, None)
        if method is None:
            continue

        try:
            return method(account, order_id)
        except TypeError:
            try:
                return method(order_id)
            except Exception as exc:
                last_error = exc
                continue
        except Exception as exc:
            last_error = exc
            continue

    if last_error is not None:
        raise RuntimeError(f"撤单调用失败: {last_error}") from last_error
    raise RuntimeError("当前xtquant版本未提供可用的撤单接口")


def _is_cancel_success(result: object) -> bool:
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result == 0 or result > 0
    if result is None:
        return False
    return True


def execute_cancel(
    xt_trader: XtQuantTrader,
    account: StockAccount,
    orders: list[dict],
    dry_run: bool,
    on_error: str,
) -> None:
    success = 0
    failed = 0
    skipped = 0

    for item in orders:
        order_id = item["order_id"]
        stock_code = item["stock_code"]
        direction = item["direction"]
        volume = item["volume"]
        status = item["status"]

        print(
            f"[单号:{order_id}][准备] {direction} {stock_code} 数量{volume} 状态{status}"
        )

        if dry_run:
            print(f"[单号:{order_id}][DRY-RUN] 未实际撤单")
            skipped += 1
            continue

        try:
            result = _cancel_one_order(xt_trader, account, order_id)
        except Exception as exc:
            failed += 1
            print(f"[单号:{order_id}][失败] 异常: {exc}")
            if on_error == "stop":
                print("[停止] on-error=stop，终止后续撤单")
                break
            continue

        if _is_cancel_success(result):
            success += 1
            print(f"[单号:{order_id}][成功] 返回值: {result}")
            continue

        failed += 1
        print(f"[单号:{order_id}][失败] 返回值: {result}")
        if on_error == "stop":
            print("[停止] on-error=stop，终止后续撤单")
            break

    print("-" * 60)
    print(f"[汇总] 成功: {success} 失败: {failed} 跳过: {skipped}")


def should_continue_with_confirmation(orders: list[dict], confirm: bool) -> bool:
    if confirm:
        return True

    print("即将执行真实撤单，待撤列表:")
    for item in orders:
        print(
            f"  - 单号:{item['order_id']} {item['direction']} {item['stock_code']} 数量{item['volume']}"
        )

    answer = input("确认执行撤单? 输入 y 继续，其它任意键取消: ").strip().lower()
    return answer == "y"


def main() -> None:
    parser = argparse.ArgumentParser(description="批量撤销QMT当前委托订单")
    parser.set_defaults(dry_run=True)
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="仅查询并展示将撤订单，不实际撤单（默认）",
    )
    parser.add_argument(
        "--execute",
        dest="dry_run",
        action="store_false",
        help="执行真实撤单（关闭dry-run）",
    )
    parser.add_argument(
        "--side",
        choices=["all", "buy", "sell"],
        default="all",
        help="方向过滤: all(默认)/buy/sell",
    )
    parser.add_argument(
        "--on-error",
        choices=["continue", "stop"],
        default="continue",
        help="单笔失败时行为: continue(默认)/stop",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="执行模式下跳过交互确认，直接撤单",
    )
    args = parser.parse_args()

    load_env_vars()
    account_id = os.getenv("QMT_ACCOUNT")
    qmt_path = os.getenv("QMT_PATH", r"C:\国金证券QMT交易端\userdata_mini")

    if not account_id:
        raise RuntimeError("缺少环境变量 QMT_ACCOUNT")

    if args.dry_run:
        print("当前模式: DRY-RUN（仅演练，不会实际撤单）")
    else:
        print("当前模式: EXECUTE（将执行真实撤单）")

    xt_trader, account = connect_trader(qmt_path, account_id)
    print(f"QMT连接成功，账户: {account_id}")

    orders = query_orders(xt_trader, account, args.side)
    print(f"查询到符合条件的委托: {len(orders)} 条")

    if not orders:
        print("无可处理委托，结束")
        return

    if not args.dry_run and not should_continue_with_confirmation(orders, args.confirm):
        print("用户取消执行，结束")
        return

    execute_cancel(xt_trader, account, orders, args.dry_run, args.on_error)


if __name__ == "__main__":
    main()
