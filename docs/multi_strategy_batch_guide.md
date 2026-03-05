# 多策略批量回测工具使用说明

## 概述

`multi_strategy_batch_test.py` 是一个全功能的回测程序，支持多种技术指标策略和不同的K线周期。该程序支持两种回测模式：

### 回测模式
1. **批量回测模式**: 对指定板块内的所有股票进行批量回测
2. **单股票回测模式**: 对单一股票进行详细回测分析，支持图表显示

### 支持的策略
- **VEGAS**: Vegas双通道策略（基于144日EMA和价格通道）
- **MACD**: MACD金叉死叉策略
- **MFI**: 资金流量指标策略（超买超卖）
- **SMA**: 简单移动平均线交叉策略
- **TURTLE**: 海龟策略（突破策略）

### 支持的数据周期
- **daily**: 日K线数据
- **weekly**: 周K线数据

## 程序特性

### 1. 双模式支持
- **批量模式**: 高效的并行批量回测，适合大规模策略筛选
- **单股票模式**: 详细的单股票分析，支持可视化图表展示

### 2. 多策略支持
- 可以同时测试多种技术指标策略
- 每种策略都有独立的交易逻辑和参数设置
- 支持策略间的性能对比分析

### 3. 多周期支持
- 支持日K线和周K线数据
- 可以对比同一策略在不同周期下的表现
- 自动处理不同周期数据的最小数据量要求

### 4. 并行处理
- 使用线程池并行处理多个股票
- 支持自定义并行线程数
- 显示实时进度条

### 5. 可视化支持
- 单股票模式支持回测图表显示
- 包含价格走势、技术指标和交易信号
- 支持多策略/周期组合的连续测试

### 6. 结果分析
- 详细的回测结果统计
- 按策略和周期分组的性能分析
- 自动生成汇总报告

### 7. 结果保存
- 保存详细的CSV格式回测结果
- 保存JSON格式的汇总统计
- 文件名包含时间戳，避免覆盖

## 使用方法

### 基本语法

#### 批量回测模式
```bash
python backtesting/multi_strategy_batch_test.py --sector <板块名称> [选项]
```

#### 单股票回测模式
```bash
python backtesting/multi_strategy_batch_test.py --ticker <股票代码> [选项]
```

### 参数说明

#### 主要参数（二选一）
- `--sector`: 板块名称（如：沪深300、创业板、中证500等）- 启用批量回测模式
- `--ticker`: 股票代码（如：510300.SH、000001.SZ等）- 启用单股票回测模式

#### 共同可选参数
- `--strategies`: 策略列表，可选 vegas, macd, mfi, sma, turtle（默认：vegas）
- `--periods`: 周期列表，可选 daily, weekly（默认：daily）
- `--start-date`: 开始日期，格式YYYYMMDD（默认：20200101）
- `--end-date`: 结束日期，格式YYYYMMDD（默认：20250801）
- `--cash`: 初始资金（默认：100000）
- `--commission`: 手续费率（默认：0.001）

#### 批量回测专用参数
- `--workers`: 并行线程数（默认：4）
- `--no-save`: 不保存结果到文件

#### 单股票回测专用参数
- `--plot`: 显示回测图表
- `--verbose`: 显示详细信息（默认：True）

## 使用示例

### 单股票回测模式

#### 1. 基本单股票回测
```bash
# 测试510300.SH使用MACD策略
python backtesting/multi_strategy_batch_test.py --ticker 510300.SH --strategies macd

# 测试000001.SZ使用Vegas策略，显示图表
python backtesting/multi_strategy_batch_test.py --ticker 000001.SZ --strategies vegas --plot
```

#### 2. 多策略单股票回测
```bash
# 测试多种策略的表现
python backtesting/multi_strategy_batch_test.py --ticker 510300.SH --strategies vegas macd mfi --plot

# 测试多策略多周期
python backtesting/multi_strategy_batch_test.py --ticker 000002.SZ --strategies turtle sma --periods daily weekly --plot
```

#### 3. 自定义参数单股票回测
```bash
# 自定义时间范围和资金
python backtesting/multi_strategy_batch_test.py --ticker 510300.SH \
    --strategies macd \
    --start-date 20230101 \
    --end-date 20240630 \
    --cash 200000 \
    --commission 0.0005 \
    --plot
```

### 批量回测模式

#### 1. 单策略单周期测试
```bash
# Vegas策略日线测试沪深300
python backtesting/multi_strategy_batch_test.py --sector 沪深300 --strategies vegas --periods daily
```

#### 2. 单策略多周期测试
```bash
# MACD策略日线+周线测试创业板
python backtesting/multi_strategy_batch_test.py --sector 创业板 --strategies macd --periods daily weekly

# 海龟策略日线+周线测试沪深300
python backtesting/multi_strategy_batch_test.py --sector 沪深300 --strategies turtle --periods daily weekly
```

#### 3. 多策略单周期测试
```bash
# 多种策略日线对比测试
python backtesting/multi_strategy_batch_test.py --sector 沪深300 --strategies vegas macd mfi sma turtle --periods daily
```

#### 4. 全面测试
```bash
# 所有策略所有周期测试
python backtesting/multi_strategy_batch_test.py --sector 中证500 --strategies vegas macd mfi sma turtle --periods daily weekly
```

#### 5. 自定义参数批量测试
```bash
# 自定义时间范围和参数
python backtesting/multi_strategy_batch_test.py --sector 沪深300 \
    --strategies vegas macd \
    --periods weekly \
    --start-date 20220101 \
    --end-date 20241231 \
    --cash 200000 \
    --commission 0.0005 \
    --workers 6
```

## 策略详细说明

### VEGAS策略
- **指标**: 12日EMA、144日EMA、价格通道
- **买入信号**: 价格上穿下轨，且EMA12>EMA144，且价格>EMA144
- **卖出信号**: 价格下穿上轨，或EMA12下穿EMA144
- **风控**: 5%止损、15%止盈

### MACD策略
- **指标**: MACD线、信号线、柱状图
- **买入信号**: MACD线上穿信号线且MACD>0
- **卖出信号**: MACD线下穿信号线
- **风控**: 5%止损、15%止盈

### MFI策略
- **指标**: 资金流量指数
- **买入信号**: MFI < 20（超卖）
- **卖出信号**: MFI > 80（超买）
- **特点**: 适合震荡市场

### SMA策略
- **指标**: 5日SMA、20日SMA
- **买入信号**: 快线上穿慢线（金叉）
- **卖出信号**: 快线下穿慢线（死叉）
- **特点**: 经典趋势跟踪策略

### TURTLE策略（海龟策略）
- **指标**: 最近10天最高价、最近10天最低价
- **买入信号**: 收盘价突破最近10天最高价
- **卖出信号**: 收盘价跌破最近10天最低价
- **风控**: 5%止损、15%止盈
- **特点**: 经典的趋势突破策略，适合强趋势市场

## 输出结果

### 单股票回测模式

#### 控制台输出
- 股票基本信息和测试参数
- 数据加载状态和天数
- 详细的回测结果统计：
  - 总收益率、年化收益率
  - 夏普比率
  - 最大回撤
  - 交易统计（总交易次数、盈利交易、亏损交易、胜率、盈亏比）

#### 图表输出（使用--plot参数）
- 价格K线图
- 技术指标图层
- 买卖信号标记
- 资金曲线
- 回撤曲线

### 批量回测模式

#### 控制台输出
- 实时进度显示
- 按策略和周期分组的统计信息
- 各策略最佳表现股票

#### 文件输出
1. **CSV结果文件**: `multi_strategy_backtest_<板块>_<策略>_<周期>_<时间戳>.csv`
   - 包含每只股票每种策略每个周期的详细回测结果

2. **JSON汇总文件**: `multi_strategy_summary_<板块>_<策略>_<周期>_<时间戳>.json`
   - 包含按策略和周期分组的统计汇总

### 结果字段说明
- `stock_code`: 股票代码
- `strategy`: 策略名称
- `period`: 数据周期
- `total_return`: 总收益率(%)
- `sharpe_ratio`: 夏普比率
- `max_drawdown`: 最大回撤(%)
- `annual_return`: 年化收益率(%)
- `total_trades`: 总交易次数
- `win_rate`: 胜率(%)

## 性能优化

### 并行处理
- 默认使用4个线程并行处理
- 可通过`--workers`参数调整线程数
- 建议根据CPU核心数设置

### 数据限制
- 自动限制测试股票数量（最多50只）
- 各策略有不同的最小数据要求
- 自动跳过数据不足的股票

### 内存管理
- 使用生成器和迭代器减少内存占用
- 及时释放不需要的数据对象
- 适合大规模批量测试

## 注意事项

1. **数据要求**: 确保数据库中有足够的历史数据
2. **计算时间**: 多策略多周期测试需要较长时间
3. **结果解读**: 考虑市场环境对不同策略的影响
4. **参数调优**: 各策略参数可在代码中自定义调整
5. **风险提示**: 回测结果不代表未来收益，仅供参考
6. **海龟策略特别说明**: 海龟策略在强趋势市场中表现较好，在震荡市场中可能产生较多虚假信号
7. **单股票模式**: 使用`--plot`参数时，如果测试多个策略/周期组合，系统会在每个图表间暂停等待用户确认
8. **模式选择**: 
   - 单股票模式适合深入分析特定股票
   - 批量模式适合策略筛选和大规模验证

## 策略选择建议

### 按市场环境选择
- **趋势市场**: 推荐使用VEGAS、TURTLE策略
- **震荡市场**: 推荐使用MFI策略
- **平衡配置**: 可同时测试多种策略进行对比
- **周期选择**: 短线交易使用日线，中长线投资使用周线

### 按分析目的选择
- **策略研究**: 使用单股票模式深入分析策略逻辑
- **股票筛选**: 使用批量模式找出适合特定策略的股票
- **策略对比**: 同时测试多种策略比较表现
- **参数优化**: 通过调整时间范围和参数寻找最优配置

## 扩展功能

如需添加新策略或自定义参数，可以：
1. 在策略定义区域添加新的策略类
2. 在STRATEGY_MAP中注册新策略
3. 在StrategyType枚举中添加新策略类型
4. 根据需要调整最小数据要求

## 快速开始

### 新手推荐

#### 1. 先尝试单股票模式了解策略
```bash
# 测试一只熟悉的股票，了解策略逻辑
python backtesting/multi_strategy_batch_test.py --ticker 000001.SZ --strategies macd --plot
```

#### 2. 比较不同策略的表现
```bash
# 同一股票测试多种策略
python backtesting/multi_strategy_batch_test.py --ticker 510300.SH --strategies vegas macd turtle --plot
```

#### 3. 进行小规模批量测试
```bash
# 测试创业板的MACD策略表现
python backtesting/multi_strategy_batch_test.py --sector 创业板 --strategies macd --workers 2
```

### 使用技巧

1. **逐步测试**: 先用单股票模式理解策略，再用批量模式验证
2. **图表分析**: 使用`--plot`参数观察买卖信号的准确性
3. **参数调优**: 通过不同时间范围测试找出最佳参数
4. **策略组合**: 同时测试多种策略，寻找互补的策略组合
5. **周期对比**: 比较日线和周线的表现差异

### 演示脚本
运行演示脚本查看使用示例：
```bash
python demo_multi_strategy_batch.py
```
