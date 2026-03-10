# miniQMT `order.txt` 批量下单说明

## 功能概述

脚本 `qmt/place_orders_from_file.py` 用于按 `order.txt` 批量提交股票委托，适合集合竞价阶段快速执行。

- 忽略订单中的日期字段
- 支持买入/卖出混合批量下单
- 委托类型固定为限价单（`FIX_PRICE`）
- 自动计算“尽量成交”的限价
  - 买入：优先使用涨停价 `UpStopPrice`
  - 卖出：优先使用跌停价 `DownStopPrice`
  - 若无涨跌停字段，自动按昨收价和板块涨跌幅规则兜底

## 订单文件格式

默认读取项目根目录下的 `order.txt`，每行格式：

```text
日期 动作 股票代码 股票名称 数量
```

示例：

```text
2021-01-04 买入 603893.SH 瑞芯微 300
2021-01-04 卖出 600519.SH 贵州茅台 300
```

说明：

- 日期会被忽略，仅用于记录展示
- 动作仅支持：`买入` / `卖出`
- 代码格式：`000001.SZ` / `600000.SH` / `430001.BJ`
- 数量必须是正整数

在 `multi_factor_analysis.py` 的盘后选股页中，`order.txt` 可由系统自动生成：

- 先按多因子排序选出前 N 只（N=购买股票数量）
- 总资金平均分配到 N 只股票
- 价格来自 xtdata 实时行情，优先 `askPrice1`，缺失时回退 `lastPrice`
- 数量按 100 股整手向下取整
- 预算不足 100 股或无有效价格的股票不会写入 `order.txt`

## 参数说明

```bash
python qmt/place_orders_from_file.py [参数]
```

- `--file`：订单文件路径，默认 `order.txt`
- `--dry-run`：仅解析和计算限价，不实际下单
- `--side {all,buy,sell}`：方向过滤，默认 `all`
- `--on-error {continue,stop}`：单笔失败时行为，默认 `continue`

## 推荐用法

1) 先演练，不下真实单：

```bash
python qmt/place_orders_from_file.py --dry-run --file order.txt
```

2) 只执行买单（集合竞价常用）：

```bash
python qmt/place_orders_from_file.py --side buy --file order.txt
```

3) 只执行卖单：

```bash
python qmt/place_orders_from_file.py --side sell --file order.txt
```

4) 遇到第一笔失败即停止后续：

```bash
python qmt/place_orders_from_file.py --on-error stop --file order.txt
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
  - 请调整 `order.txt` 数量后重试
