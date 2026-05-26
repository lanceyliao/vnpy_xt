from datetime import datetime, timedelta
from collections.abc import Callable
from threading import Thread, Event, Lock
from typing import Any
from copy import copy
import time
import threading

from xtquant import (
    xtdata,
    xtdatacenter as xtdc
)
from xtquant import xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import (
    StockAccount,
    XtAsset,
    XtOrder,
    XtPosition,
    XtTrade,
    XtOrderResponse,
    XtCancelOrderResponse,
    XtOrderError,
    XtCancelError
)
from filelock import FileLock, Timeout

from vnpy.event import EventEngine, EVENT_TIMER, Event
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    ContractData,
    TickData,
    HistoryRequest,
    OptionType,
    OrderData,
    Status,
    Direction,
    OrderType,
    AccountData,
    PositionData,
    TradeData,
    Offset,
    BarData
)
from vnpy.trader.constant import (
    Exchange,
    Product,
    Interval
)
from vnpy.trader.utility import (
    ZoneInfo,
    get_file_path,
    round_to
)

from .xt_config import VIP_ADDRESS_LIST, LISTEN_PORT


# 事件类型
EVENT_BAR = "eBarGen"
EVENT_BAR_RECORD = "eBarGenRec"

# 交易所映射
EXCHANGE_VT2XT: dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.BSE: "BJ",
    Exchange.SHFE: "SF",
    Exchange.CFFEX: "IF",
    Exchange.INE: "INE",
    Exchange.DCE: "DF",
    Exchange.CZCE: "ZF",
    Exchange.GFEX: "GF",
}

EXCHANGE_XT2VT: dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2XT.items()}
EXCHANGE_XT2VT["SHO"] = Exchange.SSE
EXCHANGE_XT2VT["SZO"] = Exchange.SZSE


# 委托状态映射
STATUS_XT2VT: dict[str, Status] = {
    xtconstant.ORDER_UNREPORTED: Status.SUBMITTING,
    xtconstant.ORDER_WAIT_REPORTING: Status.SUBMITTING,
    xtconstant.ORDER_REPORTED: Status.NOTTRADED,
    xtconstant.ORDER_REPORTED_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PARTSUCC_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PART_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_CANCELED: Status.CANCELLED,
    xtconstant.ORDER_PART_SUCC: Status.PARTTRADED,
    xtconstant.ORDER_SUCCEEDED: Status.ALLTRADED,
    xtconstant.ORDER_JUNK: Status.REJECTED
}

# 多空方向映射
DIRECTION_VT2XT: dict[tuple, str] = {
    (Direction.LONG, Offset.NONE): xtconstant.STOCK_BUY,
    (Direction.SHORT, Offset.NONE): xtconstant.STOCK_SELL,
    (Direction.LONG, Offset.OPEN): xtconstant.STOCK_OPTION_BUY_OPEN,
    (Direction.LONG, Offset.CLOSE): xtconstant.STOCK_OPTION_BUY_CLOSE,
    (Direction.SHORT, Offset.OPEN): xtconstant.STOCK_OPTION_SELL_OPEN,
    (Direction.SHORT, Offset.CLOSE): xtconstant.STOCK_OPTION_SELL_CLOSE,
}
DIRECTION_XT2VT: dict[str, tuple] = {v: k for k, v in DIRECTION_VT2XT.items()}

POSDIRECTION_XT2VT: dict[int, Direction] = {
    xtconstant.DIRECTION_FLAG_BUY: Direction.LONG,
    xtconstant.DIRECTION_FLAG_SELL: Direction.SHORT
}

# 委托类型映射
ORDERTYPE_VT2XT: dict[tuple, int] = {
    (Exchange.SSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.SZSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.BSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
}
ORDERTYPE_XT2VT: dict[int, OrderType] = {
    50: OrderType.LIMIT,
}

# 其他常量
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区

# A股交易时段（分钟）
STOCK_SESSION_WINDOWS: tuple[tuple[int, int], ...] = (
    (9 * 60 + 30, 11 * 60 + 30),   # 上午 9:30 ~ 11:30
    (13 * 60, 15 * 60),             # 下午 13:00 ~ 15:00
)
STOCK_SESSION_START_MINUTES: tuple[int, ...] = tuple(start for start, _ in STOCK_SESSION_WINDOWS)
STOCK_SESSION_END_MINUTES: tuple[int, ...] = tuple(end for _, end in STOCK_SESSION_WINDOWS)  # (690, 900) 即 11:30, 15:00

TICK_WALL_CUTOFF_MINUTE: int = 15 * 60
AUCTION_START_MINUTE: int = 9 * 60 + 25
AUCTION_END_MINUTE: int = 9 * 60 + 30

# 全局缓存字典
symbol_contract_map: dict[str, ContractData] = {}       # 合约数据
symbol_limit_map: dict[str, tuple[float, float]] = {}   # 涨跌停价


class XtGateway(BaseGateway):
    """
    VeighNa用于对接迅投研的实时行情接口。
    """

    default_name: str = "XT"

    default_setting: dict[str, Any] = {
        "token": "",
        "股票市场": ["是", "否"],
        "期货市场": ["是", "否"],
        "期权市场": ["是", "否"],
        "仿真交易": ["是", "否"],
        "账号类型": ["股票", "股票期权"],
        "QMT路径": "",
        "资金账号": ""
    }

    exchanges: list[str] = list(EXCHANGE_VT2XT.keys())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.md_api: XtMdApi = XtMdApi(self)
        self.td_api: XtTdApi = XtTdApi(self)

        self.trading: bool = False
        self.orders: dict[str, OrderData] = {}
        self.count: int = 0

        self.thread: Thread | None = None

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        if self.thread:
            return

        self.thread = Thread(target=self._connect, args=(setting,))
        self.thread.start()

    def _connect(self, setting: dict) -> None:
        """连接交易接口"""
        token: str = setting["token"]

        stock_active: bool = setting["股票市场"] == "是"
        futures_active: bool = setting["期货市场"] == "是"
        option_active: bool = setting["期权市场"] == "是"

        self.md_api.connect(token, stock_active, futures_active, option_active)

        self.trading = setting["仿真交易"] == "是"
        if self.trading:
            path: str = setting["QMT路径"] + "\\userdata"

            accountid: str = setting["资金账号"]

            if setting["账号类型"] == "股票":
                account_type: str = "STOCK"
            else:
                account_type = "STOCK_OPTION"

            self.td_api.connect(path, accountid, account_type)
            self.init_query()
        self.event_engine.register(EVENT_TIMER, self.md_api.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if self.trading:
            return self.td_api.send_order(req)
        else:
            return ""

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        if self.trading:
            self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        if self.trading:
            self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        if self.trading:
            self.td_api.query_position()

    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        return None

    def on_tick(self, tick: TickData) -> None:
        """推送 tick 数据（准入闸门，逻辑同主分支 xt_gateway）"""
        now = datetime.now(CHINA_TZ)
        wall_minute = now.hour * 60 + now.minute
        if wall_minute >= TICK_WALL_CUTOFF_MINUTE:
            return
        if tick.open_price <= 0:
            return
        if AUCTION_START_MINUTE <= wall_minute < AUCTION_END_MINUTE:
            super().on_tick(copy(tick))
        super().on_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        """推送K线数据（copy 避免下游 convert_tz 原地改 datetime 污染 state['bar']）"""
        bar_copy = copy(bar)
        self.on_event(EVENT_BAR, bar_copy)
        self.on_event(EVENT_BAR_RECORD, bar_copy)

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)

    def close(self) -> None:
        """关闭接口"""
        # 关闭行情接口
        self.md_api.close()
        
        # 关闭交易接口
        if self.trading:
            self.td_api.close()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询任务"""
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class XtMdApi:
    """行情API"""

    lock_filename = "xt_lock"
    lock_filepath = get_file_path(lock_filename)

    def __init__(self, gateway: XtGateway) -> None:
        """构造函数"""
        self.gateway: XtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False
        self.subscribed: set = set()

        self.token: str = ""
        self.stock_active: bool = False
        self.futures_active: bool = False
        self.option_active: bool = False

        # Tick 状态管理
        self.symbol_tick_states: dict[str, dict] = {}

        # K线状态管理
        self.symbol_bar_states: dict[str, dict] = {}  # 包含 bar 对象和补漏状态

        # 订阅批量管理（每秒收集一次）
        self.pending_subscribe: set[str] = set()
        self.subscribe_lock: threading.Lock = threading.Lock()
        self.whole_quote_seqs: list = []  # subscribe_whole_quote 返回的订阅号，close 时反订阅

    def onMarketData(self, data: dict) -> None:
        """行情推送回调（subscribe_whole_quote 格式：{code: dict}）
        
        回调格式（来自 xtdata.subscribe_whole_quote 文档）：
            datas: dict
                {'000001.SZ': {'time': 1733118954000, 'lastPrice': 11.39, ...}}
        """
        for xt_symbol, d in data.items():
            # 获取 tick 状态
            state = self.symbol_tick_states.get(xt_symbol)
            if state is None:
                continue

            # subscribe_whole_quote 回调：d 为单条 dict
            if not isinstance(d, dict):
                continue
            
            # 提取必需字段
            tick_ms = d.get("time")
            last_price = d.get("lastPrice")
            if tick_ms is None or last_price is None:
                continue

            symbol, xt_exchange = xt_symbol.split(".")
            exchange = EXCHANGE_XT2VT[xt_exchange]

            # 逆序重复过滤
            if tick_ms <= state["last_tick_ms"]:
                continue
            if tick_ms < state["anchor_ms"]:
                continue

            state["last_tick_ms"] = tick_ms

            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=generate_datetime(tick_ms),
                volume=d.get("volume", 0),
                turnover=d.get("amount", 0),
                open_interest=d.get("openInt", 0),
                gateway_name=self.gateway_name
            )

            contract = symbol_contract_map.get(tick.vt_symbol)
            if not contract:
                continue

            tick.name = contract.name

            bp_data: list = d.get("bidPrice", [0] * 5)
            ap_data: list = d.get("askPrice", [0] * 5)
            bv_data: list = d.get("bidVol", [0] * 5)
            av_data: list = d.get("askVol", [0] * 5)

            tick.bid_price_1 = round_to(bp_data[0], contract.pricetick)
            tick.bid_price_2 = round_to(bp_data[1], contract.pricetick)
            tick.bid_price_3 = round_to(bp_data[2], contract.pricetick)
            tick.bid_price_4 = round_to(bp_data[3], contract.pricetick)
            tick.bid_price_5 = round_to(bp_data[4], contract.pricetick)

            tick.ask_price_1 = round_to(ap_data[0], contract.pricetick)
            tick.ask_price_2 = round_to(ap_data[1], contract.pricetick)
            tick.ask_price_3 = round_to(ap_data[2], contract.pricetick)
            tick.ask_price_4 = round_to(ap_data[3], contract.pricetick)
            tick.ask_price_5 = round_to(ap_data[4], contract.pricetick)

            tick.bid_volume_1 = bv_data[0]
            tick.bid_volume_2 = bv_data[1]
            tick.bid_volume_3 = bv_data[2]
            tick.bid_volume_4 = bv_data[3]
            tick.bid_volume_5 = bv_data[4]

            tick.ask_volume_1 = av_data[0]
            tick.ask_volume_2 = av_data[1]
            tick.ask_volume_3 = av_data[2]
            tick.ask_volume_4 = av_data[3]
            tick.ask_volume_5 = av_data[4]

            tick.last_price = round_to(last_price, contract.pricetick)
            tick.open_price = round_to(d.get("open", 0), contract.pricetick)
            tick.high_price = round_to(d.get("high", 0), contract.pricetick)
            tick.low_price = round_to(d.get("low", 0), contract.pricetick)
            tick.pre_close = round_to(d.get("lastClose", 0), contract.pricetick)

            if tick.vt_symbol in symbol_limit_map:
                tick.limit_up, tick.limit_down = symbol_limit_map[tick.vt_symbol]

            # 判断收盘状态（使用 openInt 字段 - 证券状态编码）
            # 
            # XT tick 的 openInt 字段含义（实测）：
            #   股票：0,10=未知, 1=停牌, 11=开盘前S, 12=集合竞价C, 13=连续交易T, 14=休市B, 
            #         15=闭市E, 16=波动性中断V, 17=临时停牌P, 18=收盘集合竞价U, 19=盘中集合竞价M,
            #         20=暂停交易至闭市N, 21=获取字段异常, 22=盘后固定价格, 23=盘后固定价格完毕
            #   期货：0=未知, 1=开盘前S, 2=集合竞价C, 3=连续交易T, 4=休市B, 5=闭市E
            tick.extra = {
                "raw": d,
                "market_closed": False,
            }

            open_int = d.get("openInt", 0)
            settlement_price = d.get("settlementPrice", 0)

            # 非衍生品：openInt 是证券状态编码（15=闭市E, 18=收盘集合竞价U）
            if contract.product not in {Product.FUTURES, Product.OPTION}:
                tick.extra["market_closed"] = open_int == 15
            # 衍生品：openInt 是持仓量，通过结算价判断（期货 openInt: 5=闭市E）
            elif settlement_price > 0:
                tick.extra["market_closed"] = True

            self.gateway.on_tick(tick)

    def connect(
        self,
        token: str,
        stock_active: bool,
        futures_active: bool,
        option_active: bool
    ) -> None:
        """连接"""
        self.gateway.write_log("开始启动行情服务，请稍等")

        self.token = token
        self.stock_active = stock_active
        self.futures_active = futures_active
        self.option_active = option_active

        if self.inited:
            self.gateway.write_log("行情接口已经初始化，请勿重复操作")
            return

        try:
            self.init_xtdc()

            # 尝试查询合约信息，确认连接成功
            xtdata.get_instrument_detail("000001.SZ")
        except Exception as ex:
            self.gateway.write_log(f"迅投研数据服务初始化失败，发生异常：{ex}")
            return

        self.gateway.write_log("行情接口连接成功")

        self.query_contracts()
        self.inited = True

    def process_timer_event(self, event: Event) -> None:
        """定时任务（每秒触发；未 inited 时跳过，subscribe 可先入 pending）"""
        if not self.inited:
            return

        # 批量订阅新增标的
        self._batch_subscribe()
        
        # 轮询 K线数据
        self._poll_kline()

    def _batch_subscribe(self) -> None:
        """批量订阅新增标的（合约未入库的留在 pending，下秒重试）"""
        with self.subscribe_lock:
            if not self.pending_subscribe:
                return

            ready: list[str] = []
            not_ready: set[str] = set()
            for xt_symbol in self.pending_subscribe:
                symbol, xt_exchange = xt_symbol.split(".", 1)
                exchange = EXCHANGE_XT2VT.get(xt_exchange)
                vt_symbol = f"{symbol}.{exchange.value}" if exchange else ""
                if vt_symbol and vt_symbol in symbol_contract_map:
                    ready.append(xt_symbol)
                else:
                    not_ready.add(xt_symbol)
            self.pending_subscribe = not_ready

        if not ready:
            return

        new_codes = sorted(ready)

        # 批量注册 K线全推（首次调用 get_full_kline 注册客户端 1m 全推）
        # 注意：后续 _poll_kline 会全量获取所有已订阅合约的 K线，不是增量
        trading_date = datetime.now(CHINA_TZ).strftime("%Y%m%d")
        xtdata.get_full_kline([], new_codes, "1m", "", trading_date, 2)

        # 批量订阅全推 tick（增量订阅，避免重复 callback）
        seq = xtdata.subscribe_whole_quote(
            code_list=new_codes,
            callback=self.onMarketData
        )
        if seq:
            self.whole_quote_seqs.append(seq)
        self.subscribed.update(new_codes)
        self.gateway.write_log(f"批量订阅 {len(new_codes)} 个新增合约，seq={seq}")

    def _poll_kline(self) -> None:
        """轮询 K线数据（推送 [-2] 已完成的 bar，收盘时补发 [-1] 卡住的最后一根）
        
        收盘补发策略（参考 tq_gateway._flush_last_session_bar_if_needed）：
        - 正常推送：推送 [-2] 列（已完成的 bar）
        - 收盘补发：15:02~15:30 或 11:32~12:00 窗口内，检查 [-1] 是否为交易小节最后一根
                   如果是且未补发过，则推送 [-1] 列
        - 原因：15:00 后 [-1] 会卡在 150000（14:59 的 bar），不会再列变
        """
        if not self.subscribed:
            return

        trading_date = datetime.now(CHINA_TZ).strftime("%Y%m%d")
        codes = sorted(self.subscribed)

        # 全量获取所有已订阅合约的 K线数据
        kline_dict = xtdata.get_full_kline([], codes, "1m", "", trading_date, 2)
        if kline_dict is None or not isinstance(kline_dict, dict):
            return

        time_df = kline_dict.get("time")
        if time_df is None or time_df.empty:
            return

        # 当前时间（用于收盘补发判断）
        now = datetime.now(CHINA_TZ).replace(second=0, microsecond=0)
        now_minute = now.hour * 60 + now.minute
        
        # 收盘补发窗口和目标分钟
        target_minute: datetime | None = None
        if 11 * 60 + 31 <= now_minute <= 12 * 60:
            target_minute = now.replace(hour=11, minute=29, tzinfo=CHINA_TZ)
        elif 15 * 60 + 1 <= now_minute <= 15 * 60 + 30:
            target_minute = now.replace(hour=14, minute=59, tzinfo=CHINA_TZ)

        # 遍历每个合约
        for xt_symbol in codes:
            if xt_symbol not in time_df.index:
                continue

            state = self.symbol_bar_states.get(xt_symbol)
            if state is None:
                continue

            time_row = time_df.loc[xt_symbol]
            valid_cols = [col for col in time_row.index if time_row[col] == time_row[col]]
            if len(valid_cols) < 2:
                continue

            # 正常推送：处理 [-2] 列（已完成的 bar）
            prev_column = valid_cols[-2]
            self._process_bar_col(state, kline_dict, xt_symbol, prev_column)

            # 收盘补发：检查 [-1] 是否为目标分钟
            if target_minute is not None and state.get("last_session_flush_dt") != target_minute:
                curr_column = valid_cols[-1]
                curr_ms = time_row[curr_column]
                curr_end_dt = datetime.fromtimestamp(curr_ms / 1000, tz=CHINA_TZ).replace(second=0, microsecond=0)
                curr_dt = curr_end_dt - timedelta(minutes=1)  # XT 时间戳减 1 分钟
                
                if curr_dt == target_minute:
                    # 补发 [-1] 列
                    if self._process_bar_col(state, kline_dict, xt_symbol, curr_column):
                        state["last_session_flush_dt"] = target_minute

    def _process_bar_col(
        self,
        state: dict,
        kline_dict: dict,
        xt_symbol: str,
        column: Any
    ) -> bool:
        """处理单列 K线数据（参考 tq_gateway._process_bar_row）
        
        Args:
            state: 合约状态字典
            kline_dict: K线数据字典
            xt_symbol: XT 合约代码
            column: DataFrame 列键（时间字符串，如 "20260520101600"）
                   注意：XT 的 DataFrame 结构是 index=合约, columns=时间
                        与常规时间序列（index=时间）相反
        
        Returns:
            bool: 是否成功推送 bar
        """
        bar = state.get("bar")
        if bar is None:
            return False

        # 获取时间戳
        time_df = kline_dict.get("time")
        if time_df is None or xt_symbol not in time_df.index:
            return False
        
        time_row = time_df.loc[xt_symbol]
        if column not in time_row.index:
            return False
        
        timestamp_ms = time_row[column]
        if timestamp_ms <= 0:
            return False

        # 解析时间戳（XT 返回的是分钟结束时刻，需要减 1 分钟得到起始时刻）
        bar_end_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=CHINA_TZ).replace(second=0, microsecond=0)
        bar_dt = bar_end_dt - timedelta(minutes=1)

        # 逆序重复过滤
        last_pushed_ms = state.get("last_closed_bar_ms")
        if last_pushed_ms is not None and timestamp_ms <= last_pushed_ms:
            return False

        # 锚点过滤
        anchor_minute = state.get("anchor_minute")
        if anchor_minute and bar_dt < anchor_minute:
            return False

        # 交易时段检查
        session_index = self._get_stock_session_index(bar_dt)
        if session_index is None:
            return False

        # 提取 OHLCVA 数据（get_full_kline 含 amount=成交额，见 test_full_kline_poll.py §5b）
        # 注意：XT bar 的 openInt 字段也是证券状态编码（与 tick 相同）
        #      实测盘中 bar 的 openInt=13（连续交易T）
        #      期货的 openInt 可能是持仓量，需要区分品种
        bar_data = {}
        for field in ["open", "high", "low", "close", "volume", "amount", "openInt"]:
            field_df = kline_dict.get(field)
            if field_df is not None and xt_symbol in field_df.index:
                field_row = field_df.loc[xt_symbol]
                if column in field_row.index:
                    bar_data[field] = field_row[column]

        # 解析合约信息
        symbol, xt_exchange = xt_symbol.split(".")
        exchange = EXCHANGE_XT2VT.get(xt_exchange)
        if not exchange:
            return False

        contract = symbol_contract_map.get(f"{symbol}.{exchange.value}")
        if not contract:
            return False

        # 解析 bar 数据
        open_price = float(bar_data.get("open", 0))
        high_price = float(bar_data.get("high", 0))
        low_price = float(bar_data.get("low", 0))
        close_price = float(bar_data.get("close", 0))
        volume = float(bar_data.get("volume", 0))
        turnover = float(bar_data.get("amount", 0))
        open_interest = float(bar_data.get("openInt", 0))

        # 补漏逻辑
        last_bar_dt = bar.datetime if last_pushed_ms is not None else None
        if last_bar_dt is None:
            # 首次推送：从交易小节起始填充
            session_start_minute = STOCK_SESSION_START_MINUTES[session_index]
            session_start = bar_dt.replace(
                hour=session_start_minute // 60,
                minute=session_start_minute % 60,
                second=0,
                microsecond=0
            )
            
            fill_dt = session_start
            while fill_dt < bar_dt:
                bar.datetime = fill_dt
                bar.volume = 0
                bar.turnover = 0
                bar.open_interest = open_interest
                bar.open_price = open_price
                bar.high_price = open_price
                bar.low_price = open_price
                bar.close_price = open_price
                self.gateway.on_bar(bar)
                fill_dt += timedelta(minutes=1)
        else:
            # 后续推送：从上一根 bar+1 填充
            fill_dt = last_bar_dt + timedelta(minutes=1)
            fill_price = bar.close_price
            fill_oi = bar.open_interest
            
            while fill_dt < bar_dt:
                if self._get_stock_session_index(fill_dt) != session_index:
                    break
                bar.datetime = fill_dt
                bar.volume = 0
                bar.turnover = 0
                bar.open_interest = fill_oi
                bar.open_price = fill_price
                bar.high_price = fill_price
                bar.low_price = fill_price
                bar.close_price = fill_price
                self.gateway.on_bar(bar)
                fill_dt += timedelta(minutes=1)

        # 更新当前 bar
        bar.datetime = bar_dt
        bar.volume = volume
        bar.turnover = turnover
        bar.open_interest = open_interest
        bar.open_price = open_price
        bar.high_price = high_price
        bar.low_price = low_price
        bar.close_price = close_price
        
        # 推送当前 bar
        self.gateway.on_bar(bar)

        # 更新状态
        state["last_closed_bar_ms"] = timestamp_ms
        return True

    def get_lock(self) -> bool:
        """获取文件锁，确保单例运行"""
        self.lock = FileLock(self.lock_filepath)

        try:
            self.lock.acquire(timeout=1)
            return True
        except Timeout:
            return False

    def init_xtdc(self) -> None:
        """初始化xtdc服务进程"""
        if not self.get_lock():
            return

        # 设置token
        xtdc.set_token(self.token)

        # 设置连接池
        xtdc.set_allow_optmize_address(VIP_ADDRESS_LIST)

        # 开启使用期货真实夜盘时间
        xtdc.set_future_realtime_mode(True)

        # 设置 K线镜像市场（全市场 1m 由行情侧灌入本地）
        xtdc.set_kline_mirror_markets(["SH", "SZ", "BJ"])

        # 执行初始化，但不启动默认58609端口监听
        xtdc.init(False)

        # 设置监听端口
        xtdc.listen(port=LISTEN_PORT)

    def query_contracts(self) -> None:
        """查询合约信息"""
        if self.stock_active:
            self.query_stock_contracts()

        if self.futures_active:
            self.query_future_contracts()

        if self.option_active:
            self.query_option_contracts()

        self.gateway.write_log("合约信息查询成功")

    def query_stock_contracts(self) -> None:
        """查询股票合约信息"""
        xt_symbols: list[str] = []
        markets: list = [
            "沪深A股",
            "沪深转债",
            "沪深ETF",
            "沪深指数",
            "京市A股"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            # 筛选需要的合约
            product = None
            symbol, xt_exchange = xt_symbol.split(".")

            if xt_exchange == "SZ":
                if xt_symbol.startswith("00"):
                    product = Product.EQUITY
                elif xt_symbol.startswith("159"):
                    product = Product.FUND
                else:
                    product = Product.INDEX
            elif xt_exchange == "SH":
                if xt_symbol.startswith(("60", "68")):
                    product = Product.EQUITY
                elif xt_symbol.startswith("51"):
                    product = Product.FUND
                else:
                    product = Product.INDEX
            elif xt_exchange == "BJ":
                product = Product.EQUITY

            if not product:
                continue

            # 生成并推送合约信息
            data: dict = xtdata.get_instrument_detail(xt_symbol)
            if data is None:
                self.gateway.write_log(f"合约{xt_symbol}信息查询失败")
                continue

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                history_data=False,
                gateway_name=self.gateway_name
            )

            symbol_contract_map[contract.vt_symbol] = contract
            symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

            self.gateway.on_contract(contract)

    def query_future_contracts(self) -> None:
        """查询期货合约信息"""
        xt_symbols: list[str] = []
        markets: list = [
            "中金所期货",
            "上期所期货",
            "能源中心期货",
            "大商所期货",
            "郑商所期货",
            "广期所期货"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            # 筛选需要的合约
            product = None
            symbol, xt_exchange = xt_symbol.split(".")

            if xt_exchange == "ZF" and len(symbol) > 6 and "&" not in symbol:
                product = Product.OPTION
            elif xt_exchange in ("IF", "GF") and "-" in symbol:
                product = Product.OPTION
            elif xt_exchange in ("DF", "INE", "SF") and ("C" in symbol or "P" in symbol) and "SP" not in symbol:
                product = Product.OPTION
            else:
                product = Product.FUTURES

            # 生成并推送合约信息
            if product == Product.OPTION:
                data: dict = xtdata.get_instrument_detail(xt_symbol, True)
            else:
                data = xtdata.get_instrument_detail(xt_symbol)

            if not data["ExpireDate"]:
                if "00" not in symbol:
                    continue

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                history_data=False,
                gateway_name=self.gateway_name
            )

            symbol_contract_map[contract.vt_symbol] = contract
            symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

            self.gateway.on_contract(contract)

    def query_option_contracts(self) -> None:
        """查询期权合约信息"""
        xt_symbols: list[str] = []

        markets: list = [
            "上证期权",
            "深证期权",
            "中金所期权",
            "上期所期权",
            "能源中心期权",
            "大商所期权",
            "郑商所期权",
            "广期所期权"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            ""
            _, xt_exchange = xt_symbol.split(".")

            if xt_exchange in {"SHO", "SZO"}:
                contract = process_etf_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)
            else:
                contract = process_futures_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)

            if contract:
                symbol_contract_map[contract.vt_symbol] = contract

                self.gateway.on_contract(contract)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情（先入 pending，合约表入库后由定时任务批量发出）"""
        xt_exchange: str = EXCHANGE_VT2XT[req.exchange]
        if xt_exchange in {"SH", "SZ"} and len(req.symbol) > 6:
            xt_exchange += "O"

        xt_symbol: str = req.symbol + "." + xt_exchange

        with self.subscribe_lock:
            if xt_symbol in self.subscribed:
                return  # 已订阅，跳过

            # 创建 tick 状态（毫秒时间戳）
            anchor_ms = int(time.time() * 1000)
            self.symbol_tick_states[xt_symbol] = {
                "anchor_ms": anchor_ms,
                "last_tick_ms": 0,
            }

            # 创建 bar 状态（包含 bar 对象）
            symbol, xt_exchange = xt_symbol.split(".")
            exchange = EXCHANGE_XT2VT.get(xt_exchange)
            
            bar = None
            if exchange:
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    datetime=datetime.now(CHINA_TZ).replace(second=0, microsecond=0),  # 初始化为当前时间
                    interval=Interval.MINUTE,
                    volume=0,
                    open_interest=0,
                    open_price=0,
                    high_price=0,
                    low_price=0,
                    close_price=0,
                    gateway_name=self.gateway_name
                )
            
            self.symbol_bar_states[xt_symbol] = {
                "anchor_minute": datetime.now(CHINA_TZ).replace(second=0, microsecond=0),
                "last_closed_bar_ms": None,  # None 标记未推送过 bar
                "last_session_flush_dt": None,  # 收盘补发标记（记录已补发的目标分钟）
                "bar": bar,  # bar 对象存在状态里
            }

            # 加入待订阅队列（新增标的）
            self.pending_subscribe.add(xt_symbol)

    def _get_stock_session_index(self, dt: datetime) -> int | None:
        """获取股票交易时段索引"""
        minute_of_day = dt.hour * 60 + dt.minute
        for index, (start_minute, end_minute) in enumerate(STOCK_SESSION_WINDOWS):
            if start_minute <= minute_of_day < end_minute:
                return index
        return None

    def close(self) -> None:
        """关闭连接"""
        for seq in self.whole_quote_seqs:
            try:
                xtdata.unsubscribe_quote(seq)
                self.gateway.write_log(f"已反订阅全推行情 seq={seq}")
            except Exception as ex:
                self.gateway.write_log(f"反订阅全推行情失败 seq={seq}: {ex}")
        self.whole_quote_seqs.clear()

        with self.subscribe_lock:
            self.pending_subscribe.clear()
        self.subscribed.clear()
        self.symbol_tick_states.clear()
        self.symbol_bar_states.clear()


class XtTdApi(XtQuantTraderCallback):
    """交易API"""

    def __init__(self, gateway: XtGateway):
        """构造函数"""
        super().__init__()

        self.gateway: XtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.inited: bool = False
        self.connected: bool = False

        self.account_id: str = ""
        self.path: str = ""
        self.account_type: str = ""

        self.order_count: int = 0

        self.active_localid_sysid_map: dict[str, str] = {}

        self.xt_client: XtQuantTrader = None
        self.xt_account: StockAccount = None

    def on_connected(self) -> None:
        """
        连接成功推送
        """
        self.gateway.write_log("交易接口连接成功")

    def on_disconnected(self) -> None:
        """连接断开"""
        self.gateway.write_log("交易接口连接断开，请检查与客户端的连接状态")
        self.connected = False

        # 尝试重连，重连需要更换session_id
        session: int = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)
        connect_result: int = self.connect(self.path, self.account_id, self.account_type, session)

        if connect_result:
            self.gateway.write_log("交易接口重连失败")
        else:
            self.gateway.write_log("交易接口重连成功")

    def on_stock_trade(self, xt_trade: XtTrade) -> None:
        """成交变动推送"""
        if not xt_trade.order_remark:
            return

        symbol, xt_exchange = xt_trade.stock_code.split(".")

        direction, offset = DIRECTION_XT2VT.get(xt_trade.order_type, (None, None))
        if direction is None:
            return

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_trade.order_remark,
            tradeid=xt_trade.traded_id,
            direction=direction,
            offset=offset,
            price=xt_trade.traded_price,
            volume=xt_trade.traded_volume,
            datetime=generate_datetime(xt_trade.traded_time, False),
            gateway_name=self.gateway_name
        )

        contract: ContractData = symbol_contract_map.get(trade.vt_symbol, None)
        if contract:
            trade.price = round_to(trade.price, contract.pricetick)

        self.gateway.on_trade(trade)

    def on_stock_order(self, xt_order: XtOrder) -> None:
        """委托回报推送"""
        # 过滤非VeighNa Trader发出的委托
        if not xt_order.order_remark:
            return

        # 过滤不支持的委托类型
        type: OrderType = ORDERTYPE_XT2VT.get(xt_order.price_type, None)
        if not type:
            return

        direction, offset = DIRECTION_XT2VT.get(xt_order.order_type, (None, None))
        if direction is None:
            return

        symbol, xt_exchange = xt_order.stock_code.split(".")

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_order.order_remark,
            direction=direction,
            offset=offset,
            type=type,                  # 目前测出来与文档不同，限价返回50，市价返回88
            price=xt_order.price,
            volume=xt_order.order_volume,
            traded=xt_order.traded_volume,
            status=STATUS_XT2VT.get(xt_order.order_status, Status.SUBMITTING),
            datetime=generate_datetime(xt_order.order_time, False),
            gateway_name=self.gateway_name
        )

        if order.is_active():
            self.active_localid_sysid_map[xt_order.order_remark] = xt_order.order_sysid
        else:
            self.active_localid_sysid_map.pop(xt_order.order_remark, None)

        contract: ContractData = symbol_contract_map.get(order.vt_symbol, None)
        if contract:
            order.price = round_to(order.price, contract.pricetick)

        self.gateway.on_order(order)

    def on_query_order_async(self, xt_orders: list[XtOrder]) -> None:
        """委托信息异步查询回报"""
        if not xt_orders:
            return

        for data in xt_orders:
            self.on_stock_order(data)

        self.gateway.write_log("委托信息查询成功")

    def on_query_asset_async(self, xt_asset: XtAsset) -> None:
        """资金信息异步查询回报"""
        if not xt_asset:
            return

        account: AccountData = AccountData(
            accountid=xt_asset.account_id,
            balance=xt_asset.total_asset,
            frozen=xt_asset.frozen_cash,
            gateway_name=self.gateway_name
        )
        account.available = xt_asset.cash

        self.gateway.on_account(account)

    def on_query_trades_async(self, xt_trades: list[XtTrade]) -> None:
        """成交信息异步查询回报"""
        if not xt_trades:
            return

        for xt_trade in xt_trades:
            self.on_stock_trade(xt_trade)

        self.gateway.write_log("成交信息查询成功")

    def on_query_positions_async(self, xt_positions: list[XtPosition]) -> None:
        """持仓信息异步查询回报"""
        if not xt_positions:
            return

        for xt_position in xt_positions:
            if self.account_type == "STOCK":
                direction: Direction = Direction.NET
            else:
                direction = POSDIRECTION_XT2VT.get(xt_position.direction, "")

            if not direction:
                continue

            symbol, xt_exchange = xt_position.stock_code.split(".")

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                direction=direction,
                volume=xt_position.volume,
                yd_volume=xt_position.can_use_volume,
                frozen=xt_position.volume - xt_position.can_use_volume,
                price=xt_position.open_price,
                gateway_name=self.gateway_name
            )

            self.gateway.on_position(position)

    def on_order_error(self, xt_error: XtOrderError) -> None:
        """委托失败推送"""
        order: OrderData = self.gateway.get_order(xt_error.order_remark)
        if order:
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        self.gateway.write_log(f"交易委托失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_cancel_error(self, xt_error: XtCancelError) -> None:
        """撤单失败推送"""
        self.gateway.write_log(f"交易撤单失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_order_stock_async_response(self, response: XtOrderResponse) -> None:
        """异步下单回报推送"""
        if response.error_msg:
            self.gateway.write_log(f"委托请求提交失败：{response.error_msg}，本地委托号{response.order_remark}")
        else:
            self.gateway.write_log(f"委托请求提交成功，本地委托号{response.order_remark}")

    def on_cancel_order_stock_async_response(self, response: XtCancelOrderResponse) -> None:
        """异步撤单回报推送"""
        if response.error_msg:
            self.gateway.write_log(f"撤单请求提交失败：{response.error_msg}，系统委托号{response.order_sysid}")
        else:
            self.gateway.write_log(f"撤单请求提交成功，系统委托号{response.order_sysid}")

    def connect(self, path: str, accountid: str, account_type: str, session: int = 0) -> int:
        """发起连接"""
        self.inited = True
        self.account_id = accountid
        self.path = path
        self.account_type = account_type

        # 创建客户端和账号实例
        if not session:
            session = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)

        self.xt_client = XtQuantTrader(self.path, session)

        self.xt_account = StockAccount(self.account_id, account_type=self.account_type)

        # 注册回调接口
        self.xt_client.register_callback(self)

        # 启动交易线程
        self.xt_client.start()

        # 建立交易连接，返回0表示连接成功
        connect_result: int = self.xt_client.connect()
        if connect_result:
            self.gateway.write_log("交易接口连接失败")
            return connect_result

        self.connected = True
        self.gateway.write_log("交易接口连接成功")

        # 订阅交易回调推送
        subscribe_result: int = self.xt_client.subscribe(self.xt_account)
        if subscribe_result:
            self.gateway.write_log("交易推送订阅失败")
            return -1

        self.gateway.write_log("交易推送订阅成功")

        # 初始化数据查询
        self.query_account()
        self.query_position()
        self.query_order()
        self.query_trade()

        return connect_result

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("1%m%d%H%M%S")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(6, "0")

        orderid: str = prefix + suffix
        return orderid

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        contract: ContractData = symbol_contract_map.get(req.vt_symbol, None)
        if not contract:
            self.gateway.write_log(f"找不到该合约{req.vt_symbol}")
            return ""

        if contract.exchange not in {Exchange.SSE, Exchange.SZSE, Exchange.BSE}:
            self.gateway.write_log(f"不支持的合约{req.vt_symbol}")
            return ""

        if req.type not in {OrderType.LIMIT}:
            self.gateway.write_log(f"不支持的委托类型: {req.type.value}")
            return ""

        if req.offset == Offset.NONE and contract.product == Product.OPTION:
            self.gateway.write_log("委托失败，期权交易需要选择开平方向")
            return ""

        stock_code: str = req.symbol + "." + EXCHANGE_VT2XT[req.exchange]
        if self.account_type == "STOCK_OPTION":
            stock_code += "O"

        # 现货委托不考虑开平
        if contract.product == Product.OPTION:
            xt_direction: tuple = (req.direction, req.offset)
        else:
            xt_direction = (req.direction, Offset.NONE)

        orderid: str = self.new_orderid()

        self.xt_client.order_stock_async(
            account=self.xt_account,
            stock_code=stock_code,
            order_type=DIRECTION_VT2XT[xt_direction],
            order_volume=int(req.volume),
            price_type=ORDERTYPE_VT2XT[(req.exchange, req.type)],
            price=req.price,
            strategy_name=req.reference,
            order_remark=orderid
        )

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        vt_orderid: str = order.vt_orderid

        return vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        sysid: str | None = self.active_localid_sysid_map.get(req.orderid, None)
        if not sysid:
            self.gateway.write_log("撤单失败，找不到委托号")
            return

        if req.exchange == Exchange.SSE:
            market: int = 0
        else:
            market = 1

        self.xt_client.cancel_order_stock_sysid_async(self.xt_account, market, sysid)

    def query_position(self) -> None:
        """查询持仓"""
        if self.connected:
            self.xt_client.query_stock_positions_async(self.xt_account, self.on_query_positions_async)

    def query_account(self) -> None:
        """查询账户资金"""
        if self.connected:
            self.xt_client.query_stock_asset_async(self.xt_account, self.on_query_asset_async)

    def query_order(self) -> None:
        """查询委托信息"""
        if self.connected:
            self.xt_client.query_stock_orders_async(self.xt_account, self.on_query_order_async)

    def query_trade(self) -> None:
        """查询成交信息"""
        if self.connected:
            self.xt_client.query_stock_trades_async(self.xt_account, self.on_query_trades_async)

    def close(self) -> None:
        """关闭连接"""
        if self.inited:
            self.xt_client.stop()


def generate_datetime(timestamp: int, millisecond: bool = True) -> datetime:
    """生成带 Asia/Shanghai 时区的时间"""
    ts = timestamp / 1000 if millisecond else timestamp
    return datetime.fromtimestamp(ts, tz=CHINA_TZ)


def process_etf_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> ContractData | None:
    """处理ETF期权"""
    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 筛选期权合约合约（ETF期权代码为8位）
    if len(symbol) != 8:
        return None

    # 查询转换数据
    data: dict = get_instrument_detail(xt_symbol, True)

    name: str = data["InstrumentName"]
    if "购" in name:
        option_type = OptionType.CALL
    elif "沽" in name:
        option_type = OptionType.PUT
    else:
        return None

    if "A" in name:
        option_index = str(data["OptExercisePrice"]) + "-A"
    else:
        option_index = str(data["OptExercisePrice"]) + "-M"

    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_portfolio=data["OptUndlCode"] + "_O",
        option_index=option_index,
        option_type=option_type,
        option_underlying=data["OptUndlCode"] + "-" + str(data["ExpireDate"])[:6],
        gateway_name=gateway_name
    )

    symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

    return contract


def process_futures_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> ContractData | None:
    """处理期货期权"""
    # 筛选期权合约
    data: dict = get_instrument_detail(xt_symbol, True)

    option_strike: float = data["OptExercisePrice"]
    if not option_strike:
        return None

    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 移除产品前缀
    for _ix, w in enumerate(symbol):
        if w.isdigit():
            break

    suffix: str = symbol[_ix:]

    # 过滤非期权合约
    if "(" in symbol or " " in symbol:
        return None

    # 判断期权类型
    if "C" in suffix:
        option_type = OptionType.CALL
    elif "P" in suffix:
        option_type = OptionType.PUT
    else:
        return None

    # 获取期权标的
    if "-" in symbol:
        option_underlying: str = symbol.split("-")[0]
    else:
        option_underlying = data["OptUndlCode"]

    # 转换数据
    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_index=str(data["OptExercisePrice"]),
        option_type=option_type,
        option_underlying=option_underlying,
        gateway_name=gateway_name
    )

    if contract.exchange == Exchange.CZCE:
        contract.option_portfolio = data["ProductID"][:-1]
    else:
        contract.option_portfolio = data["ProductID"]

    symbol_limit_map[contract.vt_symbol] = (data["UpStopPrice"], data["DownStopPrice"])

    return contract
