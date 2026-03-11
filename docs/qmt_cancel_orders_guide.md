# miniQMT 批量撤单说明

## 最短操作卡片

```bash
# 1) 先演练：查看将撤哪些单
python qmt/cancel_orders.py

# 2) 实盘撤单（含确认）
python qmt/cancel_orders.py --execute

# 3) 实盘撤单（无人值守）
python qmt/cancel_orders.py --execute --confirm
```

## 功能概述

脚本 `qmt/cancel_orders.py` 用于批量撤销当前账户可查询到的委托订单，适合集合竞价前后快速清理挂单。

- 默认模式为 `dry-run`（仅演练，不实际撤单）
- 支持按方向过滤：全部 / 仅买单 / 仅卖单
- 支持失败策略：继续后续或遇错即停
- 执行真实撤单时默认需要人工确认

## 命令格式

```bash
python qmt/cancel_orders.py [参数]
```

参数说明：

- `--dry-run`：仅查询和展示待撤订单，不实际撤单（默认）
- `--execute`：执行真实撤单（关闭默认 `dry-run`）
- `--side {all,buy,sell}`：方向过滤，默认 `all`
- `--on-error {continue,stop}`：单笔失败时行为，默认 `continue`
- `--confirm`：执行模式下跳过交互确认，直接撤单

## 推荐用法

1) 默认演练（推荐先执行）：

```bash
python qmt/cancel_orders.py
```

2) 仅查看买单撤单计划：

```bash
python qmt/cancel_orders.py --side buy
```

3) 仅查看卖单撤单计划：

```bash
python qmt/cancel_orders.py --side sell
```

4) 真实撤单（带人工确认）：

```bash
python qmt/cancel_orders.py --execute
```

5) 真实撤单（跳过确认）：

```bash
python qmt/cancel_orders.py --execute --confirm
```

6) 遇到第一笔失败立即停止：

```bash
python qmt/cancel_orders.py --execute --on-error stop
```

## 输出说明

脚本会打印逐笔状态，典型流程如下：

- 连接成功与账户信息
- 查询到的候选订单数量
- 每笔订单的准备信息（单号、方向、代码、数量、状态）
- 演练或真实撤单结果
- 汇总统计（成功 / 失败 / 跳过）

其中“跳过”通常表示 `dry-run` 模式下未实际执行。

## 安全机制

- 默认 `dry-run`，避免误操作
- 执行模式下默认要求输入 `y` 确认
- `--confirm` 仅建议在自动化流程或已充分确认时使用

## 依赖与环境

- 本机需已安装并登录 miniQMT 客户端
- 依赖 `xtquant`
- 支持从项目根目录 `.env` 读取：
  - `QMT_ACCOUNT`：资金账号
  - `QMT_PATH`：QMT userdata 路径（例如 `C:\国金证券QMT交易端\userdata_mini`）

## 常见问题

- 提示缺少 `QMT_ACCOUNT`
  - 检查项目根目录 `.env` 是否设置该变量

- 连接失败（如错误码 `-1`）
  - 检查 QMT 客户端是否已启动并登录
  - 检查 `QMT_PATH` 是否与本机路径一致

- 查询不到订单
  - 可能当前无可查询委托
  - 先在客户端确认是否存在待撤挂单

- 撤单接口不可用
  - 当前脚本会尝试多个常见接口名
  - 若你的 `xtquant` 版本差异较大，请升级或按本地接口名微调脚本
