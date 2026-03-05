"""
回测工具类模块
包含通用的回测策略和绘图功能
"""

import backtrader as bt
import streamlit as st
from pyecharts import options as opts
from pyecharts.charts import Line
from pyecharts.globals import ThemeType
import sys
import os
import pandas as pd
import logging
from datetime import datetime
from cjdata import LocalData

def get_trading_days(df, start_date) -> list:
    """
    获取从指定日期开始的所有交易日列表，将start_date前的一个交易日也包含在内。
    
    Args:
        df: 包含交易日期的DataFrame，必须包含'trade_date'列
        start_date: 开始日期，格式如 'YYYY-MM-DD'
    
    Returns:
        list: 交易日列表，从指定日期的前一个交易日开始到最后一天
    """
    start_ts = pd.Timestamp(start_date)
    all_trade_dates_full = sorted(df['trade_date'].unique())
    idx = next((i for i, d in enumerate(all_trade_dates_full) if d >= start_ts), len(all_trade_dates_full))
    if idx > 0:
        previous_trading_day = all_trade_dates_full[idx - 1]
    else:
        previous_trading_day = all_trade_dates_full[0]
    all_trade_dates = [date for date in all_trade_dates_full if date >= previous_trading_day]
    return all_trade_dates

class StockTradeAnalyzer(bt.Analyzer):
    """
    股票交易盈亏分析器
    分析每只股票的详细交易记录和盈亏情况
    """
    params = (
        ('data_source', None),
    )
    
    def __init__(self):
        self.initial_cash = None
        self.total_commission = 0.0
        self.stock_trades = {}  # 记录每只股票的交易记录
        self.portfolio_value = []
        self.portfolio_dates = []

    def start(self):
        """分析开始时初始化"""
        self.initial_cash = self.strategy.broker.getvalue()
        
    def next(self):
        """每个交易日记录组合价值"""
        self.portfolio_value.append(self.strategy.broker.getvalue())
        try:
            dt = self.strategy.datas[0].datetime.datetime(0)
        except Exception:
            dt = None
        self.portfolio_dates.append(dt)
        
    def notify_order(self, order):
        """订单状态通知"""
        if order.status != order.Completed:
            return
            
        stock_code = getattr(order.data, '_name', 'UNKNOWN')
        stock_name = self.p.data_source.get_stock_name(stock_code) if self.p.data_source else 'UNKNOWN'

        # 累计佣金
        self.total_commission += order.executed.comm
        
        # 记录每只股票的交易
        if stock_code not in self.stock_trades:
            self.stock_trades[stock_code] = {
                'name': stock_name,
                'buy_orders': [],
                'sell_orders': [],
                'total_buy_value': 0.0,
                'total_sell_value': 0.0,
                'total_commission': 0.0
            }
        
        trade_record = self.stock_trades[stock_code]
        trade_record['total_commission'] += order.executed.comm

        if order.isbuy():
            buy_value = order.executed.price * order.executed.size
            trade_record['buy_orders'].append({
                'price': order.executed.price,
                'size': order.executed.size,
                'value': buy_value,
                'commission': order.executed.comm
            })
            trade_record['total_buy_value'] += buy_value
            # print(f"{self.strategy.datas[0].datetime.date(0)} 买入{stock_code}({stock_name}), 成交价{order.executed.price:.2f}, 成交量{order.executed.size}, 佣金{order.executed.comm:.2f}")
        else:
            sell_value = order.executed.price * abs(order.executed.size)
            trade_record['sell_orders'].append({
                'price': order.executed.price,
                'size': abs(order.executed.size),
                'value': sell_value,
                'commission': order.executed.comm
            })
            trade_record['total_sell_value'] += sell_value
            # print(f"{self.strategy.datas[0].datetime.date(0)} 卖出{stock_code}({stock_name}), 成交价{order.executed.price:.2f}, 成交量{abs(order.executed.size)}, 佣金{order.executed.comm:.2f}")

    def stop(self):
        """分析结束时计算并输出盈亏统计"""
        final_value = self.strategy.broker.getvalue()
        
        print("=" * 80)
        print("📊 每只股票交易盈亏明细")
        print("=" * 80)
        
        total_stock_pnl = 0.0
        traded_stocks = 0
        
        # 创建用于保存数据的列表
        stock_analysis_data = []
        
        for stock_code, trade_record in self.stock_trades.items():
            stock_name = trade_record['name']
            total_buy = trade_record['total_buy_value']
            total_sell = trade_record['total_sell_value']
            commission = trade_record['total_commission']
            
            # 获取当前持仓
            current_position = None
            current_market_value = 0.0
            current_shares = 0
            
            # 查找对应的数据源来获取当前持仓
            for data in self.strategy.datas:
                if getattr(data, '_name', '') == stock_code:
                    current_position = self.strategy.broker.getposition(data)
                    if current_position.size > 0:
                        current_price = data.close[0]
                        current_market_value = current_position.size * current_price
                        current_shares = current_position.size
                    break
            
            # 计算该股票的净盈亏
            if current_position and current_position.size > 0:
                stock_pnl = total_sell + current_market_value - total_buy - commission
                is_holding = True
                holding_status = "持有中"
            else:
                stock_pnl = total_sell - total_buy - commission
                is_holding = False
                holding_status = "已清仓"
            
            # 计算收益率
            if total_buy > 0:
                return_rate = (stock_pnl / total_buy) * 100
            else:
                return_rate = 0.0
                
            total_stock_pnl += stock_pnl
            traded_stocks += 1
            
            # 保存到数据列表
            stock_data = {
                '股票代码': stock_code,
                '股票名称': stock_name,
                '买入次数': len(trade_record['buy_orders']),
                '卖出次数': len(trade_record['sell_orders']),
                '买入总额': total_buy,
                '卖出总额': total_sell,
                '当前持仓市值': current_market_value,
                '当前持仓股数': current_shares,
                '持仓状态': holding_status,
                '交易佣金': commission,
                '净盈亏': stock_pnl,
                '收益率(%)': return_rate
            }
            stock_analysis_data.append(stock_data)
            
            print(f"股票: {stock_code} ({stock_name})")
            print(f"  买入次数: {len(trade_record['buy_orders'])}, 买入总额: {total_buy:,.2f} 元")
            print(f"  卖出次数: {len(trade_record['sell_orders'])}, 卖出总额: {total_sell:,.2f} 元")
            if is_holding and current_position:
                print(f"  当前持仓市值: {current_market_value:,.2f} 元 (持仓 {current_position.size} 股)")
                print(f"  持仓状态: 🔵 持有中")
            else:
                print(f"  持仓状态: ✅ 已清仓")
            print(f"  交易佣金: {commission:.2f} 元")
            print(f"  净盈亏: {stock_pnl:,.2f} 元")
            print(f"  收益率: {return_rate:.2f}%")
            print("-" * 60)
        
        # 创建DataFrame并保存为实例属性
        self.stock_analysis_df = pd.DataFrame(stock_analysis_data)
        
        print("=" * 80)
        print("📈 交易汇总")
        print("=" * 80)
        
        if self.initial_cash is not None:
            total_pnl = final_value - self.initial_cash
            total_return = (final_value / self.initial_cash - 1) * 100
            
            print(f"交易股票数量: {traded_stocks} 只")
            print(f"初始资金: {self.initial_cash:,.2f} 元")
            print(f"最终资金: {final_value:,.2f} 元")
            print(f"总盈亏: {total_pnl:,.2f} 元")
            print(f"总收益率: {total_return:.2f}%")
            print(f"总佣金: {self.total_commission:.2f} 元")
            print(f"股票交易盈亏合计: {total_stock_pnl:,.2f} 元")
            print("=" * 80)
            
            # 保存汇总数据
            self.summary_data = {
                '交易股票数量': traded_stocks,
                '初始资金': self.initial_cash,
                '最终资金': final_value,
                '总盈亏': total_pnl,
                '总收益率(%)': total_return,
                '总佣金': self.total_commission,
                '股票交易盈亏合计': total_stock_pnl
            }
            
            print(f"\n✅ 股票分析数据已保存到 analyzer.stock_analysis_df")
            print(f"📊 DataFrame 形状: {self.stock_analysis_df.shape}")
        else:
            print("无法计算总盈亏：未记录初始资金")
    
    def get_analysis(self):
        """获取分析结果"""
        return {
            'stock_analysis_df': getattr(self, 'stock_analysis_df', None),
            'summary_data': getattr(self, 'summary_data', None),
            'portfolio_value': self.portfolio_value,
            'portfolio_dates': self.portfolio_dates,
            'total_commission': self.total_commission,
            'stock_trades': self.stock_trades
        }


class DateStrategy(bt.Strategy):
    """
    基于指定日期买卖的策略，支持止损功能
    
    Args:
        data_source: LocalData实例
        buy_dates (dict): 买入日期字典，格式 {'YYYY-MM-DD': [stock_list]}
        sell_dates (dict): 卖出日期字典，格式 {'YYYY-MM-DD': [stock_list]}
        stop_loss_pct (float): 止损百分比，如0.1表示10%止损，None表示不启用止损
        trailing_stop (bool): 是否启用移动止损，默认False
        log_file (str): 日志文件路径
    """
    params = (
        ('data_source', None),
        ('buy_dates', {}),
        ('sell_dates', {}),
        ('stop_loss_pct', None),  # 止损百分比
        ('trailing_stop', False),  # 是否启用移动止损
        ('log_file', None),
    )
    
    def __init__(self):
        self.data_source = self.p.data_source
        self.buy_dates = self.p.buy_dates
        self.sell_dates = self.p.sell_dates
        
        # 止损相关变量
        self.stop_orders = {}  # 记录每个数据源的止损订单
        self.entry_prices = {}  # 记录买入价格
        self.highest_prices = {}  # 记录最高价格（用于移动止损)
        
        # 设置日志文件
        if self.p.log_file is None:
            # 默认使用当前时间戳作为日志文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file = os.path.join("log", f"backtest_log_{timestamp}.log")
        else:
            self.log_file = self.p.log_file
            
        # 确保日志目录存在
        log_dir = os.path.dirname(os.path.abspath(self.log_file)) if os.path.dirname(self.log_file) else os.getcwd()
        os.makedirs(log_dir, exist_ok=True)

    def next(self):
        """策略主逻辑：根据日期执行买卖操作，并检查止损"""
        date0 = str(self.data.datetime.date(0))
        
        # 1. 检查止损（在调仓前执行）
        if self.p.stop_loss_pct is not None:
            self.check_stop_loss()
        
        # 2. 执行卖出操作（调仓卖出）
        if date0 in self.sell_dates.keys():
            stock_list = self.sell_dates[date0]
            for stock_code in stock_list:
                # 找到对应的数据对象
                data_obj = self.find_data_by_stock_code(stock_code)
                if data_obj:
                    self.close(data=data_obj)
                    # 清理止损相关记录
                    if data_obj in self.stop_orders:
                        del self.stop_orders[data_obj]
                    if data_obj in self.entry_prices:
                        del self.entry_prices[data_obj]
                    if data_obj in self.highest_prices:
                        del self.highest_prices[data_obj]

        # 3. 执行买入操作（调仓买入）
        if date0 in self.buy_dates.keys():
            stock_list = self.buy_dates[date0]
            for stock_code in stock_list:
                # 找到对应的数据对象
                data_obj = self.find_data_by_stock_code(stock_code)
                if data_obj:
                    # 执行买入
                    order = self.order_target_percent(data=data_obj, target=0.9/len(stock_list))

    def find_data_by_stock_code(self, stock_code):
        """根据股票代码找到对应的数据对象"""
        for data in self.datas:
            if getattr(data, '_name', '') == stock_code:
                return data
        return None

    def check_stop_loss(self):
        """检查并执行止损"""
        for data in self.datas:
            position = self.broker.getposition(data)
            
            # 只对持有的股票检查止损
            if position.size > 0 and data in self.entry_prices:
                current_price = data.close[0]
                entry_price = self.entry_prices[data]
                
                # 更新最高价格（用于移动止损）
                if self.p.trailing_stop:
                    if current_price > self.highest_prices[data]:
                        self.highest_prices[data] = current_price
                    
                    # 移动止损：从最高价下跌超过止损比例
                    stop_price = self.highest_prices[data] * (1 - self.p.stop_loss_pct)
                    should_stop = current_price <= stop_price
                else:
                    # 固定止损：从买入价下跌超过止损比例
                    stop_price = entry_price * (1 - self.p.stop_loss_pct)
                    should_stop = current_price <= stop_price
                
                # 执行止损
                if should_stop:
                    stock_code = getattr(data, '_name', 'UNKNOWN')
                    stock_name = self.data_source.get_stock_name(stock_code) if self.data_source else 'UNKNOWN'
                    
                    # 平仓
                    order = self.close(data=data)
                    
                    # 记录止损日志
                    stop_loss_pct = (current_price / entry_price - 1) * 100
                    if self.p.trailing_stop:
                        trailing_loss_pct = (current_price / self.highest_prices[data] - 1) * 100
                        self.log(f"🛑 移动止损: {stock_code}({stock_name}) 买入价{entry_price:.2f} 最高价{self.highest_prices[data]:.2f} 当前价{current_price:.2f} 止损{stop_loss_pct:.2f}% 回撤{trailing_loss_pct:.2f}%")
                    else:
                        self.log(f"🛑 固定止损: {stock_code}({stock_name}) 买入价{entry_price:.2f} 当前价{current_price:.2f} 止损{stop_loss_pct:.2f}%")
                    
                    # 清理记录
                    if data in self.entry_prices:
                        del self.entry_prices[data]
                    if data in self.highest_prices:
                        del self.highest_prices[data]
                    if data in self.stop_orders:
                        del self.stop_orders[data]

    def log(self, txt, dt=None):
        """记录日志，同时输出到控制台和文件"""
        dt = dt or self.datas[0].datetime.date(0)
        log_message = f'{dt.isoformat()} {txt}'
        
        # 输出到控制台
        print(log_message)
        
        # 写入日志文件
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_message + '\n')
        except Exception as e:
            print(f"写入日志文件失败: {e}")

    def notify_order(self, order):
        """订单状态通知 - 处理订单执行和止损记录"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            stock_code = getattr(order.data, '_name', 'UNKNOWN')
            stock_name = self.data_source.get_stock_name(stock_code) if self.data_source else 'UNKNOWN'
            
            if order.isbuy():
                self.log(f"买入{stock_code}({stock_name}), 成交价{order.executed.price:.2f}, 成交量{order.executed.size}, 佣金{order.executed.comm:.2f}")
                
                # 为止损功能记录买入价格（使用实际成交价格）
                if self.p.stop_loss_pct is not None:
                    actual_price = order.executed.price
                    self.entry_prices[order.data] = actual_price
                    self.highest_prices[order.data] = actual_price
                    
            else:
                self.log(f"卖出{stock_code}({stock_name}), 成交价{order.executed.price:.2f}, 成交量{abs(order.executed.size)}, 佣金{order.executed.comm:.2f}")
                
                # 卖出时清理止损记录
                if order.data in self.stop_orders:
                    del self.stop_orders[order.data]
                if order.data in self.entry_prices:
                    del self.entry_prices[order.data]
                if order.data in self.highest_prices:
                    del self.highest_prices[order.data]
                    
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("订单被取消/保证金不足/拒绝")
        self.order = None

def plot_strategy_performance(data_source: LocalData, analyzer, start_date, end_date, sector_name="000300.SH"):
    """
    使用 pyecharts 绘制策略收益率与指定指数的对比图
    
    Args:
        data_source: LocalData实例，用于获取数据
        analyzer: StockTradeAnalyzer实例，包含portfolio_dates和portfolio_value属性
        start_date: 开始日期，格式 'YYYYMMDD'
        end_date: 结束日期，格式 'YYYYMMDD'
        sector_name: 基准指数代码，默认为 '000300.SH'（沪深300）
        
    Returns:
        pyecharts Line chart object
    """
    if not hasattr(analyzer, 'portfolio_dates') or not analyzer.portfolio_dates:
        st.error("分析器数据不完整，无法绘制图表")
        return None
        
    portfolio_dates = analyzer.portfolio_dates
    portfolio_values = analyzer.portfolio_value

    # 转换日期格式用于显示
    date_strings = []
    for dt in portfolio_dates:
        if dt is not None:
            date_strings.append(dt.strftime('%Y-%m-%d'))
        else:
            date_strings.append('')
    
    # 计算策略收益率
    portfolio_returns = [(v / portfolio_values[0] - 1) * 100 for v in portfolio_values]
    
    # 获取指定指数数据进行对比
    try:
        index_df = data_source.get_daily(sector_name, start_date, end_date)
        
        if not index_df.empty:
            initial_index_price = index_df.iloc[0]['close']
            index_returns = [(v / initial_index_price - 1) * 100 for v in index_df['close']]
            index_dates = [dt.strftime('%Y-%m-%d') for dt in index_df.index]
            
            # 获取指数名称用于显示
            index_name = data_source.get_stock_name(sector_name) if data_source else sector_name
            if not index_name or index_name == 'UNKNOWN':
                index_name = sector_name
            
            # 创建 pyecharts 线图
            line_chart = (
                Line(init_opts=opts.InitOpts(
                    theme=ThemeType.LIGHT,
                    width="100%",
                    height="600px"
                ))
                .add_xaxis(date_strings)
                .add_yaxis(
                    "策略收益率(%)",
                    portfolio_returns,
                    color="blue",
                    linestyle_opts=opts.LineStyleOpts(width=2),
                    label_opts=opts.LabelOpts(is_show=False)
                )
                .add_yaxis(
                    f"{index_name}收益率(%)",
                    [index_returns[min(i, len(index_returns)-1)] for i in range(len(portfolio_returns))],
                    color="red", 
                    linestyle_opts=opts.LineStyleOpts(width=2),
                    label_opts=opts.LabelOpts(is_show=False)
                )
                .set_global_opts(
                    title_opts=opts.TitleOpts(title=f"策略 vs {index_name} - 收益率对比"),
                    xaxis_opts=opts.AxisOpts(name="日期"),
                    yaxis_opts=opts.AxisOpts(name="收益率 (%)"),
                    legend_opts=opts.LegendOpts(
                        pos_right="2%"  # 图例放在右边
                    ),
                    tooltip_opts=opts.TooltipOpts(trigger="axis"),
                    datazoom_opts=[opts.DataZoomOpts(range_start=0, range_end=100)]
                )
            )
            
            # 计算性能指标
            final_strategy_return = (portfolio_values[-1] / portfolio_values[0] - 1) * 100
            final_index_return = (index_df.iloc[-1]['close'] / initial_index_price - 1) * 100
            excess_return = final_strategy_return - final_index_return
            
            # 显示性能统计
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("策略总收益", f"{final_strategy_return:.2f}%")
            with col2:
                st.metric(f"{index_name}总收益", f"{final_index_return:.2f}%")
            with col3:
                st.metric("超额收益", f"{excess_return:.2f}%", delta=f"{excess_return:.2f}%", delta_color="inverse")

            return line_chart
            
        else:
            st.warning(f"无法获取{sector_name}指数数据，仅绘制策略收益")
            line_chart = (
                Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="100%", height="600px"))
                .add_xaxis(date_strings)
                .add_yaxis(
                    "策略收益率(%)",
                    portfolio_returns,
                    color="blue",
                    linestyle_opts=opts.LineStyleOpts(width=2)
                )
                .set_global_opts(
                    title_opts=opts.TitleOpts(title="策略收益率曲线"),
                    xaxis_opts=opts.AxisOpts(name="日期"),
                    yaxis_opts=opts.AxisOpts(name="收益率 (%)"),
                    tooltip_opts=opts.TooltipOpts(trigger="axis"),
                    datazoom_opts=[opts.DataZoomOpts(range_start=0, range_end=100)]
                )
            )
            return line_chart
            
    except Exception as e:
        st.error(f"获取指数数据时出错: {e}")
        line_chart = (
            Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="100%", height="600px"))
            .add_xaxis(date_strings)
            .add_yaxis(
                "策略收益率(%)",
                portfolio_returns,
                color="blue",
                linestyle_opts=opts.LineStyleOpts(width=2)
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="策略收益率曲线"),
                xaxis_opts=opts.AxisOpts(name="日期"),
                yaxis_opts=opts.AxisOpts(name="收益率 (%)"),
                tooltip_opts=opts.TooltipOpts(trigger="axis"),
                datazoom_opts=[opts.DataZoomOpts(range_start=0, range_end=100)]
            )
        )
        return line_chart


def run_date_strategy_backtest(data_source: LocalData, buy_dates, sell_dates, 
                             initial_cash=100000, commission=0.001, log_file=None):
    """
    运行基于日期的策略回测
    
    Args:
        data_source: LocalData实例
        buy_dates: 买入日期字典，格式 {'YYYY-MM-DD': [stock_list]}
        sell_dates: 卖出日期字典，格式 {'YYYY-MM-DD': [stock_list]}
        initial_cash: 初始资金，默认100000
        commission: 佣金费率，默认0.001
        log_file: 日志文件路径，如果为None则自动生成带时间戳的日志文件
        
    Returns:
        tuple: (strategy_instance, analyzer_instance, cerebro_instance)
    """
    # 创建Cerebro实例
    cerebro = bt.Cerebro()
    
    # 设置初始资金
    cerebro.broker.setcash(initial_cash)
    
    # 设置佣金
    cerebro.broker.setcommission(commission=commission)
    
    # 添加策略
    cerebro.addstrategy(DateStrategy, data_source=data_source, 
                       buy_dates=buy_dates, sell_dates=sell_dates, log_file=log_file)
    
    # 添加分析器
    analyzer = cerebro.addanalyzer(StockTradeAnalyzer, _name='trade_analyzer')
    
    # 添加数据 - 这部分需要根据实际情况添加股票数据
    # cerebro.adddata(data) # 用户需要自己添加数据
    
    print(f"初始资金: {cerebro.broker.getvalue():,.2f}")
    
    # 运行回测
    strategies = cerebro.run()
    strategy = strategies[0]
    
    # 获取分析器
    trade_analyzer = strategy.analyzers.trade_analyzer
    
    # 设置数据源（用于获取股票名称）
    trade_analyzer.set_data_source(data_source)
    
    print(f"最终资金: {cerebro.broker.getvalue():,.2f}")
    
    # 输出日志文件路径
    if hasattr(strategy, 'log_file'):
        print(f"交易日志已保存到: {strategy.log_file}")
    
    return strategy, trade_analyzer, cerebro


# 使用示例：
"""
# 示例用法：
from data.findata import LocalData

# 初始化数据源
data_source = LocalData()

# 定义买卖日期
buy_dates = {
    '2023-01-03': ['000001.SZ', '000002.SZ'],
    '2023-02-01': ['600000.SH']
}

sell_dates = {
    '2023-06-01': ['000001.SZ'],
    '2023-12-01': ['000002.SZ', '600000.SH']
}

# 运行回测
strategy, analyzer, cerebro = run_date_strategy_backtest(
    data_source=data_source,
    buy_dates=buy_dates,
    sell_dates=sell_dates,
    initial_cash=100000,
    commission=0.001
)

# 获取分析结果
analysis_result = analyzer.get_analysis()
stock_df = analysis_result['stock_analysis_df']
summary_data = analysis_result['summary_data']

# 绘制收益曲线
chart = plot_strategy_performance(data_source, analyzer, '20230101', '20231231')
"""
