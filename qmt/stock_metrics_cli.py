import argparse
import csv
import datetime
import math
import os
import re
import sys
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


CODE_PATTERN = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")


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


def load_xtdata_component() -> Any:
    try:
        from xtquant import xtdata

        xtdata.enable_hello = False
    except ImportError as exc:
        raise RuntimeError("缺少依赖 xtquant，请在可连接 miniQMT 的环境运行") from exc

    return xtdata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取CSV中的股票并显示当前价、区间涨跌幅、流通市值和PE"
    )
    parser.add_argument(
        "--file",
        default="order.csv",
        help="输入CSV文件路径，默认: order.csv",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="CSV编码，默认: utf-8",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="严格模式：任意股票查询失败时返回非0",
    )
    return parser.parse_args()


def parse_csv_rows(file_path: Path, encoding: str) -> list[dict]:
    records: list[dict] = []

    with file_path.open("r", newline="", encoding=encoding) as f:
        reader = csv.reader(f)
        for line_no, row in enumerate(reader, start=1):
            if not row or not any(str(col).strip() for col in row):
                continue

            cols = [str(col).strip() for col in row]
            if len(cols) != 6:
                raise ValueError(f"第{line_no}行字段数量错误，需要6列，当前{len(cols)}列")

            _, _, stock_code, stock_name, _, _ = cols
            stock_code = stock_code.upper()
            if not CODE_PATTERN.match(stock_code):
                raise ValueError(f"第{line_no}行股票代码格式不合法: {stock_code}")

            records.append(
                {
                    "line_no": line_no,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                }
            )

    if not records:
        raise ValueError("CSV文件无有效记录")

    return records


def deduplicate_by_code(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    deduped: list[dict] = []
    for item in records:
        code = item["stock_code"]
        if code in seen:
            continue
        seen.add(code)
        deduped.append(item)
    return deduped


def to_positive_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0

    if not math.isfinite(number) or number <= 0:
        return 0.0
    return number


def compute_change_pct(current_price: float, base_price: float) -> float:
    if base_price <= 0:
        return 0.0
    return (current_price - base_price) / base_price * 100


def get_historical_close(xtdata_api: Any, stock_code: str, days_ago: int) -> float:
    now = datetime.datetime.now()
    target_date = now - datetime.timedelta(days=days_ago)
    buffer_days = max(15, int(days_ago * 0.2))
    start_date = (target_date - datetime.timedelta(days=buffer_days)).strftime("%Y%m%d")
    end_date = target_date.strftime("%Y%m%d")

    try:
        data = xtdata_api.get_market_data_ex(
            stock_list=[stock_code],
            period="1d",
            start_time=start_date,
            end_time=end_date,
            dividend_type="front",
            field_list=["close"],
        )
    except Exception:
        return 0.0

    df = data.get(stock_code) if isinstance(data, dict) else None
    if df is None or "close" not in df or len(df["close"]) <= 0:
        return 0.0

    try:
        close_value = float(df["close"].iloc[-1])
    except Exception:
        return 0.0

    return to_positive_float(close_value)


def extract_float_market_cap(detail: dict, current_price: float) -> float:
    share_keys = [
        "FloatVolume",
        "floatVolume",
        "float_volume",
        "FloatShares",
        "floatShares",
        "FloatShare",
        "float_share",
        "CirculatingVolume",
        "CirculatingShares",
        "流通股本",
    ]
    for key in share_keys:
        shares = to_positive_float(detail.get(key))
        if shares > 0 and current_price > 0:
            return shares * current_price

    direct_keys = [
        "FloatMarketValue",
        "FloatMktValue",
        "floatMarketValue",
        "float_market_value",
        "CirculatingMarketValue",
        "流通市值",
    ]
    for key in direct_keys:
        value = to_positive_float(detail.get(key))
        if value > 0:
            return value

    return 0.0


def extract_pe(detail: dict) -> float:
    pe_keys = [
        "PETTM",
        "PeTTM",
        "peTTM",
        "PE",
        "pe",
        "市盈率",
    ]
    for key in pe_keys:
        value = to_positive_float(detail.get(key))
        if value > 0:
            return value
    return 0.0


def query_stock_metrics(xtdata_api: Any, stock_code: str, input_name: str) -> tuple[dict, str]:
    error_message = ""

    tick_data = xtdata_api.get_full_tick([stock_code]) or {}
    tick_item = tick_data.get(stock_code) if isinstance(tick_data, dict) else None
    if not isinstance(tick_item, dict):
        tick_item = {}

    current_price = to_positive_float(tick_item.get("lastPrice"))
    if current_price <= 0:
        current_price = to_positive_float(tick_item.get("askPrice1"))
    if current_price <= 0:
        current_price = to_positive_float(tick_item.get("bidPrice1"))

    detail = xtdata_api.get_instrument_detail(stock_code) or {}
    instrument_name = str(detail.get("InstrumentName", "") or "").strip()
    stock_name = input_name or instrument_name or stock_code

    pre_close = to_positive_float(detail.get("PreClose"))
    price_7d_ago = get_historical_close(xtdata_api, stock_code, 7)
    price_1m_ago = get_historical_close(xtdata_api, stock_code, 30)
    price_1y_ago = get_historical_close(xtdata_api, stock_code, 365)

    today_change = compute_change_pct(current_price, pre_close)
    change_7d = compute_change_pct(current_price, price_7d_ago)
    change_1m = compute_change_pct(current_price, price_1m_ago)
    change_1y = compute_change_pct(current_price, price_1y_ago)

    float_market_cap = extract_float_market_cap(detail, current_price)
    pe_value = extract_pe(detail)

    if current_price <= 0:
        error_message = "缺少有效现价"

    row = {
        "股票代码": stock_code,
        "股票名称": stock_name,
        "当前市价": current_price,
        "今天的涨跌幅": today_change,
        "最近7天的涨跌幅": change_7d,
        "最近1月涨跌幅": change_1m,
        "最近1年涨跌幅": change_1y,
        "总流通股市值": float_market_cap,
        "PE": pe_value,
    }
    return row, error_message


def format_row_for_display(row: dict) -> dict:
    return {
        "股票代码": row["股票代码"],
        "股票名称": row["股票名称"],
        "当前市价": f"{row['当前市价']:.3f}",
        "今天的涨跌幅": f"{row['今天的涨跌幅']:.2f}%",
        "最近7天的涨跌幅": f"{row['最近7天的涨跌幅']:.2f}%",
        "最近1月涨跌幅": f"{row['最近1月涨跌幅']:.2f}%",
        "最近1年涨跌幅": f"{row['最近1年涨跌幅']:.2f}%",
        "总流通股市值(亿)": f"{(row['总流通股市值'] / 1e8):.2f}",
        "PE": f"{row['PE']:.2f}",
    }


def print_table(rows: list[dict]) -> None:
    headers = [
        "股票代码",
        "股票名称",
        "当前市价",
        "今天的涨跌幅",
        "最近7天的涨跌幅",
        "最近1月涨跌幅",
        "最近1年涨跌幅",
        "总流通股市值(亿)",
        "PE",
    ]

    display_rows = [format_row_for_display(r) for r in rows]
    widths: dict[str, int] = {h: len(h) for h in headers}

    for row in display_rows:
        for h in headers:
            widths[h] = max(widths[h], len(str(row[h])))

    def _line(values: list[str]) -> str:
        return " | ".join(v.ljust(w) for v, w in zip(values, [widths[h] for h in headers]))

    print(_line(headers))
    print("-+-".join("-" * widths[h] for h in headers))
    for row in display_rows:
        print(_line([str(row[h]) for h in headers]))


def main() -> int:
    args = parse_args()

    try:
        load_env_vars()
        xtdata_api = load_xtdata_component()

        input_path = Path(args.file)
        if not input_path.is_absolute():
            input_path = Path.cwd() / input_path

        if not input_path.exists():
            raise FileNotFoundError(f"输入文件不存在: {input_path}")

        parsed = parse_csv_rows(input_path, args.encoding)
        records = deduplicate_by_code(parsed)

        rows: list[dict] = []
        failed: list[str] = []
        fallback_count = 0

        for item in records:
            code = item["stock_code"]
            name = item["stock_name"]
            try:
                row, err = query_stock_metrics(xtdata_api, code, name)
                rows.append(row)
                if err:
                    failed.append(f"{code}: {err}")
            except Exception as exc:
                failed.append(f"{code}: {exc}")
                if args.strict:
                    raise
                rows.append(
                    {
                        "股票代码": code,
                        "股票名称": name or code,
                        "当前市价": 0.0,
                        "今天的涨跌幅": 0.0,
                        "最近7天的涨跌幅": 0.0,
                        "最近1月涨跌幅": 0.0,
                        "最近1年涨跌幅": 0.0,
                        "总流通股市值": 0.0,
                        "PE": 0.0,
                    }
                )
                fallback_count += 1

        print_table(rows)
        print()
        print(f"[汇总] 输入记录: {len(parsed)} 去重后: {len(records)} 成功输出: {len(rows)}")
        print(f"[汇总] 失败或缺失现价: {len(failed)} 回退填0: {fallback_count}")

        if failed:
            print("[明细] 异常股票:")
            for item in failed:
                print(f"  - {item}")

        if args.strict and failed:
            return 1

        return 0
    except Exception as exc:
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
