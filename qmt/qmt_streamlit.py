import streamlit as st
from xtquant import xtdata
from xtquant import xttrader
from xtquant import xtconstant
import time
import pandas as pd
from xtquant.xttype import StockAccount
from xtquant.xttrader import XtQuantTrader
import os
import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _load_env_vars():
    """Load environment variables from the workspace root .env file."""
    env_path = Path(__file__).resolve().parents[1] / ".env"

    if load_dotenv is not None:
        load_dotenv(dotenv_path=env_path)
        return

    # Fallback parser when python-dotenv is not installed.
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())

# 固定配置参数
_load_env_vars()
ACCOUNT = os.getenv("QMT_ACCOUNT")
QMT_PATH = os.getenv("QMT_PATH", r"C:\国金证券QMT交易端\userdata_mini")
xtdata.enable_hello = False

def init_page_config():
    """初始化页面配置"""
    st.set_page_config(
        page_title="QMT股票账户查询系统",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )

def init_session_state():
    """初始化会话状态"""
    if 'connected' not in st.session_state:
        st.session_state.connected = False
        st.session_state.xt_trader = None
        st.session_state.acc = None
    if 'orders' not in st.session_state:
        st.session_state.orders = []
    if 'selected_order_ids' not in st.session_state:
        st.session_state.selected_order_ids = []
    if 'orders_last_refresh_ts' not in st.session_state:
        st.session_state.orders_last_refresh_ts = None


def _order_direction_text(order):
    """将委托方向转为中文"""
    direction = getattr(order, 'order_type', None)
    if direction == getattr(xtconstant, 'STOCK_BUY', object()):
        return '买入'
    if direction == getattr(xtconstant, 'STOCK_SELL', object()):
        return '卖出'

    direction = getattr(order, 'direction', None)
    if direction == getattr(xtconstant, 'STOCK_BUY', object()):
        return '买入'
    if direction == getattr(xtconstant, 'STOCK_SELL', object()):
        return '卖出'

    return '未知'


def _format_order_time(raw_time):
    """格式化委托时间显示"""
    if raw_time is None or raw_time == '':
        return '-'

    if isinstance(raw_time, datetime.datetime):
        return raw_time.strftime('%Y-%m-%d %H:%M:%S')

    if isinstance(raw_time, (int, float)):
        raw = str(int(raw_time))
    else:
        raw = str(raw_time)

    if raw.isdigit() and len(raw) == 14:
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]} {raw[8:10]}:{raw[10:12]}:{raw[12:14]}"
    if raw.isdigit() and len(raw) == 8:
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _query_orders_raw(xt_trader_client, account):
    """兼容不同xtquant版本的委托查询接口"""
    candidate_names = ['query_stock_orders', 'query_orders']
    last_error = None

    for name in candidate_names:
        method = getattr(xt_trader_client, name, None)
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
    raise RuntimeError('当前xtquant版本未提供可用的委托查询接口')


def query_pending_orders(xt_trader_client, account):
    """查询当前可展示委托并标准化为表格字段"""
    raw_orders = _query_orders_raw(xt_trader_client, account)
    result = []
    seen_order_ids = set()

    for order in raw_orders:
        order_id = getattr(order, 'order_id', None)
        if not isinstance(order_id, int) or order_id <= 0:
            continue
        if order_id in seen_order_ids:
            continue

        seen_order_ids.add(order_id)
        result.append(
            {
                '委托编号': order_id,
                '证券代码': str(getattr(order, 'stock_code', '') or ''),
                '方向': _order_direction_text(order),
                '委托数量': int(getattr(order, 'order_volume', 0) or 0),
                '已成数量': int(
                    getattr(order, 'traded_volume', getattr(order, 'deal_volume', 0)) or 0
                ),
                '委托价格': float(
                    getattr(order, 'price', getattr(order, 'order_price', 0.0)) or 0.0
                ),
                '状态': str(getattr(order, 'order_status', getattr(order, 'status', ''))),
                '委托时间': _format_order_time(
                    getattr(order, 'order_time', getattr(order, 'insert_time', ''))
                ),
            }
        )

    return result


def _cancel_one_order(xt_trader_client, account, order_id):
    """兼容不同xtquant版本的撤单接口"""
    candidate_names = ['cancel_order_stock', 'cancel_order', 'cancel_stock_order']
    last_error = None

    for name in candidate_names:
        method = getattr(xt_trader_client, name, None)
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
    raise RuntimeError('当前xtquant版本未提供可用的撤单接口')


def _is_cancel_success(result):
    """兼容不同撤单返回值类型"""
    if isinstance(result, bool):
        return result
    if isinstance(result, int):
        return result == 0 or result > 0
    if result is None:
        return False
    return True


def refresh_orders_data(show_error=True):
    """刷新委托订单数据"""
    try:
        orders = query_pending_orders(st.session_state.xt_trader, st.session_state.acc)
        st.session_state.orders = orders
        st.session_state.orders_last_refresh_ts = datetime.datetime.now()
        return True
    except Exception as e:
        st.session_state.orders = []
        st.session_state.orders_last_refresh_ts = datetime.datetime.now()
        if show_error:
            st.warning(f"⚠️ 委托查询失败: {str(e)}")
        return False


def cancel_orders_by_ids(order_ids):
    """按委托编号执行撤单"""
    success_ids = []
    failed_items = []

    for order_id in order_ids:
        try:
            result = _cancel_one_order(st.session_state.xt_trader, st.session_state.acc, int(order_id))
            if _is_cancel_success(result):
                success_ids.append(int(order_id))
            else:
                failed_items.append((int(order_id), f'返回值: {result}'))
        except Exception as e:
            failed_items.append((int(order_id), str(e)))

    return success_ids, failed_items

def connect_qmt_client():
    """连接QMT客户端"""
    session_id = int(time.time())
    
    if not st.session_state.connected:
        with st.spinner("正在连接QMT客户端..."):
            try:
                # 创建账户对象
                acc = StockAccount(ACCOUNT, 'STOCK')
                st.session_state.acc = acc
                
                # 创建交易实例
                xt_trader = XtQuantTrader(QMT_PATH, session_id)
                xt_trader.start()
                
                # 连接客户端
                connect_result = xt_trader.connect()
                
                if connect_result != 0:
                    st.error(f"❌ 连接QMT客户端失败，错误代码: {connect_result}")
                    st.error("请确保QMT客户端已启动并已登录账户")
                    st.session_state.connected = False
                    st.markdown(f"""
                    ### ⚠️ 注意事项
                    - 请确保QMT客户端已启动并已登录
                    - 当前配置路径: `{QMT_PATH}`
                    - 当前账户: `{ACCOUNT}`
                    """)
                else:
                    st.session_state.connected = True
                    st.session_state.xt_trader = xt_trader
                    
            except Exception as e:
                st.error(f"❌ 连接失败: {str(e)}")
                with st.expander("❗ 配置信息"):
                    st.write(f"QMT客户端路径: {QMT_PATH}")
                    st.write(f"股票账户号码: {ACCOUNT}")
                    st.write(f"会话ID: {session_id}")
                    st.write("此功能需要满足以下条件：")
                    st.write("1. QMT客户端已安装并启动")
                    st.write("2. 已登录相应的股票账户")
                    st.write("3. QMT客户端路径配置正确")
                st.session_state.connected = False

def render_sidebar():
    """渲染侧边栏"""
    with st.sidebar:
        st.header("🎛️ 控制面板")
        
        # 显示连接状态
        if st.session_state.connected:
            st.success("✅ QMT客户端已连接")
            st.info(f"📱 账户: {ACCOUNT}")
            
            # 刷新按钮放在侧边栏
            refresh_all = st.button("🔄 刷新数据", type="primary", width="stretch")
        else:
            st.error("❌ QMT客户端连接失败")
            refresh_all = False
        
        st.markdown("---")
        st.markdown("### 📊 功能说明")
        st.markdown("- 自动连接QMT客户端")
        st.markdown("- 实时显示资产信息")
        st.markdown("- 实时显示持仓信息")
        st.markdown("- 点击刷新获取最新数据")
        
        return refresh_all

def query_account_data(refresh_all):
    """查询账户数据"""
    if refresh_all or 'initial_loaded' not in st.session_state:
        if 'initial_loaded' not in st.session_state:
            st.session_state.initial_loaded = True
        
        with st.spinner("正在查询账户信息..."):
            try:
                # 等待一下确保连接稳定
                time.sleep(1)
                
                # 查询资产信息
                asset = st.session_state.xt_trader.query_stock_asset(st.session_state.acc)
                # 查询持仓信息  
                positions = st.session_state.xt_trader.query_stock_positions(st.session_state.acc)
                
                # 缓存查询结果
                st.session_state.asset = asset
                st.session_state.positions = positions
                refresh_orders_data(show_error=True)
                
            except Exception as e:
                st.error(f"❌ 查询信息失败: {str(e)}")
                st.session_state.asset = None
                st.session_state.positions = None
                st.session_state.orders = []

def display_asset_info():
    """显示资产信息"""
    if hasattr(st.session_state, 'asset') and st.session_state.asset:
        asset = st.session_state.asset
        st.header("💰 资产信息")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="💵 总资产",
                value=f"¥{asset.total_asset:,.2f}"
            )
        
        with col2:
            st.metric(
                label="💳 可用资金",
                value=f"¥{asset.cash:,.2f}"
            )
        
        with col3:
            st.metric(
                label="📈 持仓市值",
                value=f"¥{asset.market_value:,.2f}"
            )
        
        with col4:
            # 计算盈亏百分比
            if asset.total_asset > 0:
                profit_ratio = ((asset.market_value + asset.cash - asset.total_asset) / asset.total_asset) * 100
                if profit_ratio >= 0:
                    st.metric(
                        label="📊 总体盈亏",
                        value=f"+{profit_ratio:.2f}%",
                        delta=f"+{profit_ratio:.2f}%"
                    )
                else:
                    st.metric(
                        label="📊 总体盈亏", 
                        value=f"{profit_ratio:.2f}%",
                        delta=f"{profit_ratio:.2f}%"
                    )
    else:
        st.warning("⚠️ 未能获取资产信息")

def get_year_end_price(stock_code):
    """获取去年底收盘价"""
    try:
        current_year = datetime.datetime.now().year
        last_year_end = f"{current_year-1}1231"
        
        # 查询去年底收盘价
        year_end_data = xtdata.get_market_data_ex(
            stock_list=[stock_code],
            period='1d',
            start_time=last_year_end,
            end_time=last_year_end,
            field_list=['close']
        )
        
        if stock_code in year_end_data:
            return year_end_data[stock_code]['close'].iloc[-1] if len(year_end_data[stock_code]['close']) > 0 else 0
        else:
            return 0
    except:
        return 0

def get_historical_price(stock_code, days_ago):
    """获取指定天数前的收盘价"""
    try:
        # 计算目标日期
        target_date = datetime.datetime.now() - datetime.timedelta(days=days_ago)
        start_date = target_date.strftime('%Y%m%d')
        
        # 为了确保能获取到交易日数据，往前多查询几天
        buffer_start = (target_date - datetime.timedelta(days=10)).strftime('%Y%m%d')
        end_date = target_date.strftime('%Y%m%d')
        
        # 查询历史数据
        hist_data = xtdata.get_market_data_ex(
            stock_list=[stock_code],
            period='1d',
            start_time=buffer_start,
            end_time=end_date,
            dividend_type='front',
            field_list=['close']
        )
        
        if stock_code in hist_data and len(hist_data[stock_code]['close']) > 0:
            # 返回最后一个有效交易日的收盘价
            return hist_data[stock_code]['close'].iloc[-1]
        else:
            return 0
    except:
        return 0

def calculate_index_metrics(index_name, index_code):
    """计算指数的各项指标"""
    try:
        # 使用 get_full_tick 获取最新价格
        tick_data = xtdata.get_full_tick([index_code])
        if index_code in tick_data and len(tick_data[index_code]) > 0:
            current_price = tick_data[index_code]['lastPrice']
        else:
            current_price = 0
        
        # 使用 get_instrument_detail 获取前收盘价
        detail = xtdata.get_instrument_detail(index_code)
        pre_close = detail.get("PreClose", 0)

        # 获取去年底收盘价
        year_end_close = get_year_end_price(index_code)
        
        # 获取历史价格数据
        price_7d_ago = get_historical_price(index_code, 7)     # 7日前价格
        price_1m_ago = get_historical_price(index_code, 30)    # 1月前价格  
        price_1y_ago = get_historical_price(index_code, 365)   # 1年前价格
        
        # 计算各项指标
        price_change = ((current_price - pre_close) / pre_close * 100) if pre_close > 0 else 0
        ytd_change = ((current_price - year_end_close) / year_end_close * 100) if year_end_close > 0 else 0
        
        # 计算涨跌幅
        change_7d = ((current_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago > 0 else 0
        change_1m = ((current_price - price_1m_ago) / price_1m_ago * 100) if price_1m_ago > 0 else 0
        change_1y = ((current_price - price_1y_ago) / price_1y_ago * 100) if price_1y_ago > 0 else 0
        
        return {
            "证券代码": index_code,
            "证券名称": f"📈{index_name}",
            "持仓数量": None,
            "成本价": None,
            "当前价": current_price,
            "涨跌幅": price_change,
            "7日涨跌": change_7d,
            "1月涨跌": change_1m,
            "1年涨跌": change_1y,
            "今日盈亏": 0,
            "年初至今": ytd_change,
            "市值": None,
            "个股仓位": None,
            "盈亏金额": None,
            "盈亏比例": None
        }
    except Exception as e:
        # 如果获取指数数据失败，返回空数据
        return None

def calculate_position_metrics(position, total_available_funds):
    """计算单个持仓的各项指标"""
    detail = xtdata.get_instrument_detail(position.stock_code)
    stock_name = detail.get("InstrumentName", "N/A")
    PreClose = detail.get("PreClose", 0)
    
    # 获取去年底收盘价
    year_end_close = get_year_end_price(position.stock_code)
    
    # 获取历史价格数据
    price_7d_ago = get_historical_price(position.stock_code, 7)     # 7日前价格
    price_1m_ago = get_historical_price(position.stock_code, 30)    # 1月前价格  
    price_1y_ago = get_historical_price(position.stock_code, 365)   # 1年前价格
    
    # 计算各项指标
    profit_loss = (position.last_price - position.avg_price) * position.volume
    profit_ratio = ((position.last_price - position.avg_price) / position.avg_price) * 100 if position.avg_price > 0 else 0
    price_change = ((position.last_price - PreClose) / PreClose * 100) if PreClose > 0 else 0
    daily_profit_loss = (position.last_price - PreClose) * position.volume if PreClose > 0 else 0
    ytd_change = ((position.last_price - year_end_close) / year_end_close * 100) if year_end_close > 0 else 0
    
    # 计算新增的涨跌幅
    change_7d = ((position.last_price - price_7d_ago) / price_7d_ago * 100) if price_7d_ago > 0 else 0
    change_1m = ((position.last_price - price_1m_ago) / price_1m_ago * 100) if price_1m_ago > 0 else 0
    change_1y = ((position.last_price - price_1y_ago) / price_1y_ago * 100) if price_1y_ago > 0 else 0
    
    # 计算个股仓位（个股市值 / (总持仓市值 + 可用资金)）
    position_ratio = (position.market_value / total_available_funds * 100) if total_available_funds > 0 else 0
    
    return {
        "证券代码": position.stock_code,
        "证券名称": stock_name,
        "持仓数量": position.volume,
        "成本价": position.avg_price,
        "当前价": position.last_price,
        "涨跌幅": price_change,
        "7日涨跌": change_7d,
        "1月涨跌": change_1m,
        "1年涨跌": change_1y,
        "今日盈亏": daily_profit_loss,
        "年初至今": ytd_change,
        "市值": position.market_value,
        "个股仓位": position_ratio,
        "盈亏金额": profit_loss,
        "盈亏比例": profit_ratio
    }

def color_price_change(val):
    """定义涨跌幅相关列的颜色样式"""
    if val > 0:
        return 'color: red'
    elif val < 0:
        return 'color: green'
    else:
        return 'color: black'

def display_positions_table(positions):
    """显示持仓表格"""
    st.header("📊 持仓信息")
    st.info(f"📦 当前持仓数量: {len(positions)}")
    
    # 指数列表
    indices = {
        "上证50": "000016.SH",
        "沪深300": "000300.SH", 
        "中证500": "000905.SH",
        "中证1000": "000852.SH",
        "创业板": "399006.SZ",
        "科创50": "000688.SH"
    }
    
    # 指数选择器
    st.subheader("📈 指数对比")
    selected_indices = []
    
    # 创建复选框布局
    cols = st.columns(3)
    for i, (index_name, index_code) in enumerate(indices.items()):
        with cols[i % 3]:
            if st.checkbox(index_name, key=f"index_{index_code}"):
                selected_indices.append((index_name, index_code))
    
    # 计算总的可用资金（持仓市值 + 可用资金）
    total_market_value = sum(p.market_value for p in positions)
    available_cash = st.session_state.asset.cash if hasattr(st.session_state, 'asset') and st.session_state.asset else 0
    total_available_funds = total_market_value + available_cash
    
    # 构建持仓数据框
    position_data = [calculate_position_metrics(position, total_available_funds) for position in positions]
    df_positions = pd.DataFrame(position_data)
    
    # 添加选中的指数数据
    if selected_indices:
        with st.spinner("正在获取指数数据..."):
            index_data = []
            for index_name, index_code in selected_indices:
                index_metrics = calculate_index_metrics(index_name, index_code)
                if index_metrics:
                    index_data.append(index_metrics)
            
            if index_data:
                df_indices = pd.DataFrame(index_data)
                
                # 为了避免FutureWarning，创建一个统一的数据框
                # 将指数数据和持仓数据合并到一个列表中，然后一次性创建DataFrame
                all_data = index_data + position_data
                df_positions = pd.DataFrame(all_data)

    height = 40 + (len(df_positions) * 35)

    # 显示持仓表格，使用列配置来格式化显示
    st.dataframe(
        data=df_positions.style.map(color_price_change, subset=['涨跌幅', '7日涨跌', '1月涨跌', '1年涨跌', '今日盈亏', '年初至今']),
        width="stretch",
        hide_index=True,
        height=height,  # 设置为None以显示所有行，不出现滚动条
        column_config={
            "证券代码": st.column_config.TextColumn("证券代码", width="small"),
            "证券名称": st.column_config.TextColumn("证券名称", width="small"),
            "持仓数量": st.column_config.NumberColumn("持仓数量", format="%d"),
            "成本价": st.column_config.NumberColumn("成本价", format="%.3f"),
            "当前价": st.column_config.NumberColumn("当前价", format="%.3f"),
            "涨跌幅": st.column_config.NumberColumn("涨跌幅", format="%.2f%%"),
            "7日涨跌": st.column_config.NumberColumn("7日涨跌", format="%.2f%%"),
            "1月涨跌": st.column_config.NumberColumn("1月涨跌", format="%.2f%%"),
            "1年涨跌": st.column_config.NumberColumn("1年涨跌", format="%.2f%%"),
            "今日盈亏": st.column_config.NumberColumn("今日盈亏", format="%.2f"),
            "年初至今": st.column_config.NumberColumn("年初至今", format="%.2f%%"),
            "市值": st.column_config.NumberColumn("市值", format="accounting"),
            "个股仓位": st.column_config.NumberColumn("个股仓位", format="%.2f%%"),
            "盈亏金额": st.column_config.NumberColumn("盈亏金额", format="accounting"),
            "盈亏比例": st.column_config.NumberColumn("盈亏比例", format="%.2f%%")
        }
    )

def display_position_statistics(positions):
    """显示持仓统计"""
    st.subheader("📈 持仓统计")
    
    # 先计算今日盈亏总额，供总市值使用
    total_daily_profit = 0
    for position in positions:
        detail = xtdata.get_instrument_detail(position.stock_code)
        PreClose = detail.get("PreClose", 0)
        daily_profit_loss = (position.last_price - PreClose) * position.volume if PreClose > 0 else 0
        total_daily_profit += daily_profit_loss
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_positions = len(positions)
        st.metric("持仓只数", total_positions)
    
    with col2:
        total_market_value = sum(p.market_value for p in positions)
        # 总市值的delta使用今日盈亏数据
        if total_daily_profit >= 0:
            st.metric("总市值", f"¥{total_market_value:,.2f}", delta=f"{total_daily_profit:,.2f}", delta_color="inverse")
        else:
            st.metric("总市值", f"¥{total_market_value:,.2f}", delta=f"{total_daily_profit:,.2f}", delta_color="inverse")
    
    with col3:
        # 计算涨跌幅：今日盈亏 / (总市值 - 今日盈亏)
        yesterday_market_value = total_market_value - total_daily_profit
        if yesterday_market_value > 0:
            daily_change_ratio = (total_daily_profit / yesterday_market_value) * 100
            if daily_change_ratio >= 0:
                # 上涨时显示红色（通过CSS自定义）
                st.metric("涨跌幅", f"+{daily_change_ratio:.2f}%")
            else:
                # 下跌时显示绿色
                st.metric("涨跌幅", f"{daily_change_ratio:.2f}%")
        else:
            st.metric("涨跌幅", "0.00%")

def display_positions_info():
    """显示持仓信息"""
    if hasattr(st.session_state, 'positions') and st.session_state.positions:
        positions = st.session_state.positions
        display_positions_table(positions)
        display_position_statistics(positions)
    else:
        st.header("📊 持仓信息")
        st.info("📭 当前无持仓")


def display_orders_info():
    """显示当前委托订单，可单笔撤单和批量撤单"""
    st.header("🧾 委托订单")

    orders = st.session_state.orders if hasattr(st.session_state, 'orders') else []
    st.info(f"📦 当前委托数量: {len(orders)}")

    if hasattr(st.session_state, 'orders_last_refresh_ts') and st.session_state.orders_last_refresh_ts:
        st.caption(f"最近刷新时间: {st.session_state.orders_last_refresh_ts.strftime('%Y-%m-%d %H:%M:%S')}")

    if not orders:
        st.info("📭 当前无可撤委托")
        if st.button("🔄 刷新委托", key="refresh_orders_empty"):
            with st.spinner("正在刷新委托..."):
                refresh_orders_data(show_error=True)
            st.rerun()
        return

    df_orders = pd.DataFrame(orders)
    df_orders.insert(0, '选择', False)

    edited_df = st.data_editor(
        df_orders,
        hide_index=True,
        width='stretch',
        height=80 + len(df_orders) * 35,
        key='orders_editor',
        disabled=['委托编号', '证券代码', '方向', '委托数量', '已成数量', '委托价格', '状态', '委托时间'],
        column_config={
            '选择': st.column_config.CheckboxColumn('选择', width='small'),
            '委托编号': st.column_config.NumberColumn('委托编号', format='%d', width='small'),
            '证券代码': st.column_config.TextColumn('证券代码', width='small'),
            '方向': st.column_config.TextColumn('方向', width='small'),
            '委托数量': st.column_config.NumberColumn('委托数量', format='%d', width='small'),
            '已成数量': st.column_config.NumberColumn('已成数量', format='%d', width='small'),
            '委托价格': st.column_config.NumberColumn('委托价格', format='%.3f', width='small'),
            '状态': st.column_config.TextColumn('状态', width='medium'),
            '委托时间': st.column_config.TextColumn('委托时间', width='medium'),
        },
    )

    selected_ids = (
        edited_df.loc[edited_df['选择'] == True, '委托编号'].astype(int).tolist()
        if not edited_df.empty
        else []
    )
    st.session_state.selected_order_ids = selected_ids

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button('🧨 撤销选中订单', type='primary', width='stretch'):
            if not selected_ids:
                st.warning('请先勾选至少一条委托。')
            else:
                with st.spinner('正在批量撤单...'):
                    success_ids, failed_items = cancel_orders_by_ids(selected_ids)
                    refresh_orders_data(show_error=False)

                if success_ids:
                    st.success(f"✅ 批量撤单成功: {len(success_ids)} 笔")
                if failed_items:
                    failed_text = '，'.join([f"{oid}({msg})" for oid, msg in failed_items])
                    st.error(f"❌ 批量撤单失败: {len(failed_items)} 笔 | {failed_text}")
                if not success_ids and not failed_items:
                    st.info('没有执行任何撤单。')

    with col2:
        if st.button('🎯 撤销单笔订单', width='stretch'):
            if len(selected_ids) != 1:
                st.warning('单笔撤单需要且只能选择一条委托。')
            else:
                order_id = selected_ids[0]
                with st.spinner(f'正在撤销委托 {order_id} ...'):
                    success_ids, failed_items = cancel_orders_by_ids([order_id])
                    refresh_orders_data(show_error=False)

                if success_ids:
                    st.success(f'✅ 委托 {order_id} 撤单成功')
                else:
                    st.error(f'❌ 委托 {order_id} 撤单失败: {failed_items[0][1] if failed_items else "未知错误"}')

    with col3:
        if st.button('🔄 刷新委托', width='stretch'):
            with st.spinner('正在刷新委托...'):
                refresh_orders_data(show_error=True)
            st.rerun()

    if selected_ids:
        st.caption(f"已选中委托编号: {', '.join([str(i) for i in selected_ids])}")

def handle_connection_failure():
    """处理连接失败情况"""
    st.info("🔄 请刷新页面重新连接QMT客户端")
    
    # 手动重连按钮
    if st.button("🔄 重新连接", type="primary"):
        st.session_state.connected = False
        st.session_state.xt_trader = None
        st.session_state.acc = None
        st.rerun()

def main():
    """主函数"""
    # 初始化页面配置
    init_page_config()
    
    # 显示主标题
    st.title("📈 QMT股票账户查询系统")
    st.markdown("---")
    
    # 初始化会话状态
    init_session_state()
    
    # 连接QMT客户端
    connect_qmt_client()
    
    # 渲染侧边栏
    refresh_all = render_sidebar()
    
    # 主要逻辑
    if st.session_state.connected and st.session_state.xt_trader and st.session_state.acc:
        # 查询账户数据
        query_account_data(refresh_all)
        
        # 显示资产信息
        display_asset_info()
        
        st.markdown("---")
        
        # 显示持仓信息
        display_positions_info()

        st.markdown("---")

        # 显示委托订单信息
        display_orders_info()
    else:
        # 处理连接失败
        handle_connection_failure()
    
    # 底部信息
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>QMT股票账户查询系统 | "
        "基于 Streamlit 构建</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
