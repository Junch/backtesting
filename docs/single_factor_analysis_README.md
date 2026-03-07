# 单因子量化策略回测系统使用说明

## 概述

这个系统是一个可扩展的单因子量化策略回测平台，整合了市值因子、动量因子、价值因子等多种选股策略。用户可以通过下拉菜单选择不同的因子进行回测分析。

## 功能特点

### 1. 多因子支持
- **市值因子**: 基于总市值或流通市值选股，小市值股票优先
- **动量因子**: 基于价格动量选股，涨幅大的股票优先
- **价值因子(PE)**: 基于市盈率选股，低PE股票优先
- **ROE因子**: 基于净资产收益率选股，高ROE股票优先

### 2. 灵活的参数配置
- 板块选择：支持上证50、科创50、沪深300、中证500、中证1000、创业板
- 时间范围：可自定义回测开始和结束日期
- 调仓周期：5-60个交易日可选
- 持仓数量：5-30只股票可选
- 因子特定参数：每个因子都有专门的参数设置

### 3. 风险控制
- 可选的止损功能
- 固定止损和移动止损两种方式
- 可调节的止损比例

### 4. 详细的回测分析
- 策略性能指标：总收益率、夏普比率、最大回撤、年化收益率
- 策略收益曲线与基准指数对比
- 详细的股票交易记录
- 汇总统计数据

## 使用方法

### 1. 启动应用
```bash
cd /Users/jun/github/quant_trading_study
python -m streamlit run backtesting/single_factor_analysis.py --server.port 8502
```

### 2. 配置参数
1. 在左侧边栏选择要使用的因子
2. 选择目标板块
3. 设置回测时间范围
4. 配置策略参数（调仓周期、持仓数量等）
5. 设置因子特定参数
6. 可选配置风险控制参数

### 3. 运行回测
点击"🚀 开始回测"按钮，系统会自动：
1. 加载相关股票数据
2. 计算选择的因子值
3. 执行回测策略
4. 生成分析报告和图表

## 技术架构

### 1. 因子计算器基类 (FactorCalculator)
所有因子都继承自这个基类，提供统一的接口：
- `calculate()`: 计算因子值
- `get_factor_column()`: 返回因子列名
- `ascending`: 排序方式配置

### 2. 具体因子实现
每个因子都是一个独立的类：
- `MarketValueFactor`: 市值因子
- `MomentumFactor`: 动量因子  
- `ValueFactor`: 价值因子
- `ROEFactor`: ROE因子

### 3. 通用回测函数
`run_single_factor_backtesting()` 提供了统一的回测逻辑，支持任意因子。

## 扩展新因子

要添加新的因子，只需要：

### 1. 创建新的因子类
```python
class NewFactor(FactorCalculator):
    def __init__(self):
        super().__init__("新因子", "因子描述", ascending=True)
    
    def calculate(self, df, **kwargs):
        df = df.copy()
        factor_col = self.get_factor_column()
        
        # 在这里实现具体的因子计算逻辑
        df[factor_col] = your_factor_calculation(df)
        
        return df
```

### 2. 注册到可用因子列表
```python
AVAILABLE_FACTORS = {
    # ... 现有因子
    "新因子名称": NewFactor(),
}
```

### 3. 添加UI参数配置（可选）
在main()函数的因子参数部分添加新因子的特定参数配置。

## 示例：添加成交量因子

```python
class VolumeFactor(FactorCalculator):
    """成交量因子计算器"""
    
    def __init__(self):
        super().__init__("成交量", "基于成交量进行股票排序", ascending=False)
    
    def calculate(self, df, volume_period=20, **kwargs):
        """
        计算成交量因子
        
        Args:
            df: 股票数据DataFrame
            volume_period: 成交量平均周期
            
        Returns:
            DataFrame: 添加了成交量因子的数据框
        """
        df = df.copy()
        factor_col = self.get_factor_column()
        
        # 计算平均成交量
        df[factor_col] = df.groupby('stock_code')['volume'].transform(
            lambda x: x.rolling(window=volume_period, min_periods=1).mean()
        )
        
        return df

# 注册新因子
AVAILABLE_FACTORS["成交量因子"] = VolumeFactor()
```

## 注意事项

### 1. 数据要求
- 系统依赖LocalData类加载股票数据
- 需要包含基本的OHLCV数据
- 部分因子（如PE、ROE）需要额外的财务数据

### 2. 性能考虑
- 回测数据量大时会消耗较多内存
- 建议合理设置时间范围和股票数量
- 复杂因子计算可能需要更长时间

### 3. 风险提示
- 回测结果不代表未来表现
- 需要考虑交易成本、滑点等实际因素
- 建议结合多种分析方法进行投资决策

### 4. 交易时序说明
- 调仓信号在 t 日基于当日可见数据生成，不读取 t+1 数据
- Backtrader 在下一根 K 线执行订单（通常是 t+1）
- 若执行日停牌或不可交易，则该笔买卖跳过且不补单

## 技术依赖

- backtrader: 回测引擎
- streamlit: Web界面框架
- pandas: 数据处理
- numpy: 数值计算
- pyecharts: 图表绘制

## 更新日志

### v1.0.0
- 初始版本
- 支持市值、动量、价值、ROE四种因子
- 基础的回测和分析功能

## 联系方式

如有问题或建议，请联系开发团队。