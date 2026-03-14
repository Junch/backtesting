# miniQMT `order.csv` 批量下单说明

## 功能概述

脚本 `qmt/place_orders_from_file.py` 用于按 `order.csv` 批量提交股票委托，适合集合竞价阶段快速执行。

- 忽略订单中的日期字段
- 支持买入/卖出混合批量下单
- 委托类型固定为限价单（`FIX_PRICE`）
- 可通过 `--price-mode` 选择价格来源
  - `limit`（默认）：按涨跌停逻辑计算
    - 买入优先使用涨停价 `UpStopPrice`
    - 卖出优先使用跌停价 `DownStopPrice`
    - 若无涨跌停字段，自动按昨收价和板块涨跌幅规则兜底
  - `current`：按当前市价取值
    - 买入优先使用 `askPrice1`
    - 卖出优先使用 `bidPrice1`
    - 若无买一/卖一，则回退 `lastPrice`

## 订单文件格式

默认读取项目根目录下的 `order.csv`，每行格式：

```text
日期 动作 股票代码 股票名称 数量 限价(可选)
```

示例：

```text
2021-01-04 买入 603893.SH 瑞芯微 300
2021-01-04 买入 600519.SH 贵州茅台 300 1460.00
2021-01-04 卖出 600519.SH 贵州茅台 300
```

说明：

- 日期会被忽略，仅用于记录展示
- 动作仅支持：`买入` / `卖出`
- 代码格式：`000001.SZ` / `600000.SH` / `430001.BJ`
- 数量必须是正整数
- 限价列可留空（留空时按 `--price-mode` 自动计算）
- 限价列若填写，必须为大于 0 的数字，且优先于 `--price-mode`

在 `multi_factor_analysis.py` 的盘后选股页中，`order.csv` 可由系统自动生成：

- 先按多因子排序选出前 N 只（N=购买股票数量）
- 总资金平均分配到 N 只股票
- 价格来自信号日收盘价：优先 QMT `xtdata.get_market_data_ex(close)`，失败时回退 baostock `query_history_k_data_plus(close)`
- 数量按 100 股整手向下取整
- 预算不足 100 股或无有效价格的股票不会写入 `order.csv`

## 参数说明

```bash
python qmt/place_orders_from_file.py [参数]
```

- `--file`：订单文件路径，默认 `order.csv`
- `--dry-run`：仅解析和计算价格，不实际下单（默认）
- `--execute`：执行真实下单（关闭默认 dry-run）
- `--side {all,buy,sell}`：方向过滤，默认 `all`
- `--on-error {continue,stop}`：单笔失败时行为，默认 `continue`
- `--price-mode {limit,current}`：价格模式，默认 `limit`

## 推荐用法

1) 先演练，不下真实单：

```bash
python qmt/place_orders_from_file.py --file order.csv
```

2) 只执行买单（集合竞价常用）：

```bash
python qmt/place_orders_from_file.py --side buy --file order.csv
```

3) 只执行卖单：

```bash
python qmt/place_orders_from_file.py --side sell --file order.csv
```

4) 遇到第一笔失败即停止后续：

```bash
python qmt/place_orders_from_file.py --on-error stop --file order.csv
```

5) 使用当前市价模式：

```bash
python qmt/place_orders_from_file.py --price-mode current --file order.csv
```

6) 执行真实下单（关闭默认演练模式）：

```bash
python qmt/place_orders_from_file.py --execute --file order.csv
```

## 撤销全部委托订单

脚本 `qmt/cancel_orders.py` 用于批量撤销当前账户委托，默认 `dry-run` 仅演练。

详细说明请参考：`docs/qmt_cancel_orders_guide.md`。

最短操作卡片：

```bash
# 1) 先演练（默认）
python qmt/cancel_orders.py

# 2) 实盘撤单（含确认）
python qmt/cancel_orders.py --execute

# 3) 实盘撤单（跳过确认）
python qmt/cancel_orders.py --execute --confirm
```

```bash
python qmt/cancel_orders.py [参数]
```

- `--dry-run`：仅查询并展示将撤订单（默认）
- `--execute`：执行真实撤单
- `--side {all,buy,sell}`：方向过滤，默认 `all`
- `--on-error {continue,stop}`：单笔失败时行为，默认 `continue`
- `--confirm`：执行模式下跳过交互确认，直接撤单

常用示例：

```bash
# 默认演练（不实际撤单）
python qmt/cancel_orders.py

# 只撤买单（先演练）
python qmt/cancel_orders.py --side buy

# 真实撤单（交互确认）
python qmt/cancel_orders.py --execute

# 真实撤单（跳过确认）
python qmt/cancel_orders.py --execute --confirm
```

## 下单与成交说明

- 下单接口调用是同步返回（会立即得到委托结果或错误码）
- 成交回报是异步产生（成交时间和数量取决于撮合）

## 校验与保护逻辑

- 买入数量必须为 100 股整数倍，否则拒绝该笔
- 卖出会校验可卖数量，超出可卖则拒绝该笔
- 失败处理由 `--on-error` 控制：
  - `continue`：记录失败并继续后续订单
  - `stop`：记录失败并立即终止批量

## 依赖与环境

- 需要本机已安装并登录 miniQMT 客户端
- 依赖 `xtquant`
- 支持从项目根目录 `.env` 读取：
  - `QMT_ACCOUNT`：资金账号
  - `QMT_PATH`：QMT userdata 路径（如 `C:\国金证券QMT交易端\userdata_mini`）

## 常见问题

- 连接失败（例如错误码 `-1`）
  - 检查 QMT 客户端是否已启动并登录
  - 检查 `QMT_PATH` 是否与本机实际路径一致
  - 检查 `QMT_ACCOUNT` 是否正确

- 卖单被拒绝（可卖数量不足）
  - 脚本会按当前持仓可卖数量校验，避免超卖
  - 请调整 `order.csv` 数量后重试

## 查询持仓与导出清仓卖单

脚本 `qmt/positions_cli.py` 用于查询当前持仓；默认只打印持仓信息。

当加上 `--export` 时，会导出全部可卖持仓到卖出订单 CSV（6 列、无表头），可直接给 `qmt/place_orders_from_file.py` 使用。

```bash
# 默认：仅查询并打印当前持仓
python qmt/positions_cli.py

# 导出清仓卖单到默认文件 order_sell_all.csv
python qmt/positions_cli.py --export

# 导出到指定文件
python qmt/positions_cli.py --export --output order_sell_all_custom.csv
```

导出文件每行格式：

```text
日期,卖出,股票代码,股票名称,数量,限价(留空)
```

说明：

- 日期为当天 `YYYY-MM-DD`
- 数量取当前可卖数量 `can_use_volume`
- 仅导出 `can_use_volume > 0` 的持仓
- 若无可卖持仓，会提示并且不生成空文件
