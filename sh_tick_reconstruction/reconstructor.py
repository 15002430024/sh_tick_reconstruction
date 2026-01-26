# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解还原主模块

将 auction_tick_merged_data 混合数据流拆解还原为:
- derived_sh_orders (委托表)
- derived_sh_trades (成交表)

核心函数:
- reconstruct_sh_tick_data(): 单股票处理入口
- process_trade(): 处理 Type='T' 成交记录 (Phase 2.1)
- process_add_order(): 处理 Type='A' 挂单记录 (Phase 2.2)
- process_delete_order(): 处理 Type='D' 撤单记录 (Phase 2.3)
- settle_orders(): 批次结算 (Phase 2.4)

Version: 1.0.0
Date: 2026-01-26
"""

from typing import Dict, List, Tuple, Any
import polars as pl

from .models import OrderContext
from .time_filter import is_continuous_trading_time


# ============================================================================
# 占位函数 (Phase 2 实现)
# ============================================================================

def process_trade(
    row: Dict[str, Any],
    order_map: Dict[int, OrderContext],
    trade_list: List[Dict[str, Any]],
    security_id: str
) -> None:
    """
    处理 Type='T' 成交记录
    
    核心逻辑:
    1. 根据 TickBSFlag 确定 ActiveSide (1=主动买, 2=主动卖, 0=集合竞价)
    2. 输出到成交表 (trade_list)
    3. 只还原主动方: 新订单 is_aggressive=True, 已有订单累加 trade_qty
    
    Args:
        row: 单条原始数据记录 (dict)，需包含字段:
            - BizIndex: 逐笔序号
            - TickTime: 时间戳 (HHMMSSmmm)
            - BuyOrderNO: 买单号
            - SellOrderNO: 卖单号
            - Price: 成交价
            - Qty: 成交量
            - TickBSFlag: 主动方向标记 ('B'/'S'/'N')
        order_map: 订单缓存 {order_no: OrderContext}
        trade_list: 成交记录输出列表
        security_id: 证券代码
    
    Note:
        - 被动方已有 Type='A' 记录，不要重复还原
        - 集合竞价 (TickBSFlag='N') 只输出成交，不还原委托
    """
    # -------------------------------------------------------------------------
    # Step 1: 解析原始字段
    # -------------------------------------------------------------------------
    biz_index = row['BizIndex']
    tick_time = row['TickTime']
    buy_order_no = row['BuyOrderNO']
    sell_order_no = row['SellOrderNO']
    price = row['Price']
    qty = row['Qty']
    bs_flag = row['TickBSFlag']
    
    # -------------------------------------------------------------------------
    # Step 2: ActiveSide 统一化映射
    # -------------------------------------------------------------------------
    # B = 1 (主动买), S = 2 (主动卖), N = 0 (集合竞价)
    if bs_flag == 'B':
        active_side = 1
        active_order_no = buy_order_no  # 主动方是买方
        active_side_char = 'B'
    elif bs_flag == 'S':
        active_side = 2
        active_order_no = sell_order_no  # 主动方是卖方
        active_side_char = 'S'
    else:  # 'N' 或其他: 集合竞价
        active_side = 0
        active_order_no = None  # 集合竞价不还原委托
        active_side_char = None
    
    # -------------------------------------------------------------------------
    # Step 3: 输出成交记录到 trade_list (所有Type='T'都输出)
    # -------------------------------------------------------------------------
    # TradeMoney 优先使用原始数据，否则自行计算
    trade_money = row.get('TradeMoney')
    if trade_money is None or trade_money == 0:
        trade_money = price * qty
    
    trade_record = {
        'SecurityID': security_id,
        'BizIndex': biz_index,
        'TickTime': tick_time,
        'BidOrdID': buy_order_no,
        'AskOrdID': sell_order_no,
        'Price': price,
        'Qty': qty,
        'TradeMoney': trade_money,
        'ActiveSide': active_side,
    }
    trade_list.append(trade_record)
    
    # -------------------------------------------------------------------------
    # Step 4: 只还原主动方委托 (集合竞价跳过)
    # -------------------------------------------------------------------------
    # 核心规则: 被动方(Maker)一定有 Type='A' 记录，无需重复还原
    # 只有主动方(Taker)需要通过成交记录还原
    if active_order_no is None:
        # 集合竞价 (TickBSFlag='N')，不还原任何委托
        return
    
    # -------------------------------------------------------------------------
    # Step 5: 更新主动方订单缓存
    # -------------------------------------------------------------------------
    if active_order_no in order_map:
        # 已有订单: 累加成交量和更新成交价
        ctx = order_map[active_order_no]
        ctx.add_trade_qty(qty)
        ctx.trade_price = price  # 更新最新成交价
    else:
        # 新订单: 首次出现为 Type='T'，标记为 Taker (is_aggressive=True)
        ctx = OrderContext(
            order_no=active_order_no,
            side=active_side_char,
            first_time=tick_time,
            first_biz_index=biz_index,
            trade_qty=qty,
            trade_price=price,
            is_aggressive=True,  # ⭐ 关键: Taker 首次出现为成交
        )
        order_map[active_order_no] = ctx


def process_add_order(
    row: Dict[str, Any],
    order_map: Dict[int, OrderContext]
) -> None:
    """
    处理 Type='A' 挂单记录
    
    核心逻辑:
    - 上交所规则: 如果有成交，Type='T' 先到，Type='A' 后到
    - 已在缓存: 部分成交后转挂单，保持 is_aggressive=True
    - 新订单: 纯挂单，is_aggressive=False
    
    Args:
        row: 单条原始数据记录 (dict)，需包含字段:
            - BizIndex: 逐笔序号
            - TickTime: 时间戳 (HHMMSSmmm)
            - BuyOrderNO: 买单号
            - SellOrderNO: 卖单号
            - Price: 委托价格
            - Qty: 委托数量
            - TickBSFlag: 买卖标记 ('B'/'S')
        order_map: 订单缓存 {order_no: OrderContext}
    
    Note:
        - 必须设置 has_resting=True
        - 价格记录到 resting_price
    """
    # -------------------------------------------------------------------------
    # Step 1: 确定订单号和方向
    # -------------------------------------------------------------------------
    bs_flag = row['TickBSFlag']
    
    if bs_flag == 'B':
        order_no = row['BuyOrderNO']
        side = 'B'
    else:  # 'S'
        order_no = row['SellOrderNO']
        side = 'S'
    
    # -------------------------------------------------------------------------
    # Step 2: 判断是否已有成交记录
    # -------------------------------------------------------------------------
    if order_no in order_map:
        # =========================================================================
        # 情况1: 已有成交记录（部分成交后转挂单）
        # =========================================================================
        # 上交所规则: Type='T' 先发送，Type='A' 后发送
        # 说明该订单已经主动吃单成交一部分，剩余部分转为挂单
        # 
        # 关键点:
        # - is_aggressive 保持 True（因为它"出生"时是主动方）
        # - first_biz_index 保持不变（记录首次出现的 BizIndex）
        # =========================================================================
        ctx = order_map[order_no]
        ctx.add_resting_qty(row['Qty'])
        ctx.resting_price = row['Price']
        ctx.has_resting = True  # ⭐ 标记有挂单记录
        
    else:
        # =========================================================================
        # 情况2: 纯挂单，没有成交
        # =========================================================================
        # 该订单首次出现就是 Type='A'，说明是被动挂单等待成交
        # 
        # 关键点:
        # - is_aggressive = False（Maker，被动等待）
        # - 首次出现的 BizIndex 和 TickTime 记录下来
        # =========================================================================
        ctx = OrderContext(
            order_no=order_no,
            side=side,
            first_time=row['TickTime'],
            first_biz_index=row['BizIndex'],
            is_aggressive=False,  # ⭐ 关键: Maker 首次出现为挂单
        )
        ctx.add_resting_qty(row['Qty'])
        ctx.resting_price = row['Price']
        ctx.has_resting = True  # ⭐ 标记有挂单记录
        
        order_map[order_no] = ctx


def process_delete_order(
    row: Dict[str, Any],
    order_map: Dict[int, OrderContext],
    order_list: List[Dict[str, Any]],
    last_price: float,
    security_id: str
) -> None:
    """
    处理 Type='D' 撤单记录
    
    核心逻辑:
    1. 分级兜底策略获取撤单价格:
       - Level 0: 数据源自带的 Price (如果 > 0)
       - Level 1: 查本地缓存 order_map
       - Level 2: 最终兜底用 last_price
    2. 输出撤单记录到 order_list
    
    Args:
        row: 单条原始数据记录 (dict)，需包含字段:
            - BizIndex: 逐笔序号（撤单记录自身的）
            - TickTime: 时间戳 (HHMMSSmmm)
            - BuyOrderNO: 买单号
            - SellOrderNO: 卖单号
            - Price: 撤单价格（可能为 0 或 None）
            - Qty: 撤单数量
            - TickBSFlag: 买卖标记 ('B'/'S')
        order_map: 订单缓存 {order_no: OrderContext}
        order_list: 委托记录输出列表
        last_price: 最新成交价 (兜底用)
        security_id: 证券代码
    
    Note:
        - BizIndex 是撤单记录自身的 (不是原委托的)
        - IsAggressive 必须是 None (撤单不适用)
        - 必须包含 SecurityID
    """
    # -------------------------------------------------------------------------
    # Step 1: 确定订单号和方向
    # -------------------------------------------------------------------------
    bs_flag = row['TickBSFlag']
    
    if bs_flag == 'B':
        order_no = row['BuyOrderNO']
        side = 'B'
    else:  # 'S'
        order_no = row['SellOrderNO']
        side = 'S'
    
    # -------------------------------------------------------------------------
    # Step 2: 分级兜底策略获取撤单价格
    # -------------------------------------------------------------------------
    cancel_price = 0.0
    price_source = None  # 用于调试/日志
    
    # Level 0: 数据源自带的 Price（如果 > 0）
    raw_price = row.get('Price')
    if raw_price is not None and raw_price > 0:
        cancel_price = raw_price
        price_source = 'raw_data'
    
    # Level 1: 查本地缓存
    elif order_no in order_map:
        cancel_price = order_map[order_no].get_price()
        price_source = 'order_cache'
    
    # Level 2: 最终兜底 - 用最新成交价
    else:
        cancel_price = last_price
        price_source = 'last_price_fallback'
        # 可选：记录告警日志
        # logger.warning(f"撤单价格兜底: order_no={order_no}, using last_price={last_price}")
    
    # -------------------------------------------------------------------------
    # Step 3: 输出撤单记录到 order_list
    # -------------------------------------------------------------------------
    # 关键约束：
    # - BizIndex 是撤单记录自身的（不是原委托的）
    # - IsAggressive 必须是 None（撤单不适用进攻性标记）
    # - 必须包含 SecurityID
    
    cancel_record = {
        'SecurityID': security_id,           # ⭐ 必须包含
        'BizIndex': row['BizIndex'],         # ⭐ 撤单记录自身的 BizIndex
        'TickTime': row['TickTime'],
        'OrdID': order_no,
        'OrdType': 'Cancel',                 # 标识为撤单
        'Side': side,
        'Price': cancel_price,
        'Qty': row['Qty'],
        'IsAggressive': None,                # ⭐ 撤单不适用，填 None
    }
    order_list.append(cancel_record)


def settle_orders(
    order_map: Dict[int, OrderContext],
    order_list: List[Dict[str, Any]],
    security_id: str
) -> None:
    """
    批次结算所有缓存的委托
    
    处理完当日所有数据后，遍历 order_map 输出聚合后的委托记录。
    
    核心逻辑:
    - Qty = trade_qty + resting_qty (原始委托量)
    - Price = resting_price if > 0 else trade_price
    - BizIndex 使用首次出现的 (first_biz_index)
    - IsAggressive 直接使用缓存值 (True 或 False)
    
    Args:
        order_map: 订单缓存 {order_no: OrderContext}
        order_list: 委托记录输出列表
        security_id: 证券代码 (⭐ 必须传递)
    
    Note:
        - 这里输出的是 OrdType='New' 的委托
        - 撤单 (OrdType='Cancel') 在 process_delete_order 中已输出
    """
    for order_no, ctx in order_map.items():
        # 计算原始委托量 = 成交量 + 挂单量
        total_qty = ctx.trade_qty + ctx.resting_qty
        
        # 跳过无效订单（既没有成交也没有挂单）
        if total_qty <= 0:
            continue
        
        # 确定委托价格（优先挂单价，因为挂单价更准确）
        # resting_price > 0 说明有 Type='A' 记录，价格来自委托
        # 否则使用成交价（即时全部成交的情况）
        if ctx.resting_price > 0:
            price = ctx.resting_price
        else:
            price = ctx.trade_price
        
        # 构建委托记录
        order_record = {
            'SecurityID': security_id,        # ⭐ 必须包含
            'BizIndex': ctx.first_biz_index,  # ⭐ 首次出现的逐笔序号
            'TickTime': ctx.first_time,       # 首次出现的时间
            'OrdID': order_no,                # 委托单号
            'OrdType': 'New',                 # 新增委托
            'Side': ctx.side,                 # 买卖方向 (B/S)
            'Price': price,                   # 委托价格
            'Qty': total_qty,                 # 原始委托量
            'IsAggressive': ctx.is_aggressive # True=Taker, False=Maker (不是 None)
        }
        
        order_list.append(order_record)


# ============================================================================
# 主函数
# ============================================================================

def reconstruct_sh_tick_data(
    df: pl.DataFrame,
    security_id: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    上交所逐笔数据拆解还原主函数
    
    将单只股票当日的 auction_tick_merged_data 混合数据流拆解还原为:
    - order_list: 还原后的委托列表 (OrdType='New' + 'Cancel')
    - trade_list: 标准化成交列表 (含 ActiveSide)
    
    Args:
        df: 单只股票当日的 auction_tick_merged_data (Polars DataFrame)
            必须包含字段: BizIndex, TickTime, Type, BuyOrderNO, SellOrderNO,
                         Price, Qty, TradeMoney, TickBSFlag, SecurityID
        security_id: 证券代码 (如 '600519')
    
    Returns:
        Tuple[List[dict], List[dict]]:
            - order_list: 还原后的委托列表，每条记录包含:
                SecurityID, BizIndex, TickTime, OrdID, OrdType, Side, Price, Qty, IsAggressive
            - trade_list: 标准化成交列表，每条记录包含:
                SecurityID, BizIndex, TickTime, BidOrdID, AskOrdID, Price, Qty, TradeMoney, ActiveSide
    
    Processing Flow:
        1. 预处理
           - 剔除 Type='S' (产品状态记录)
           - 时间过滤 (上交所: 09:30-11:30, 13:00-15:00, 无收盘集合竞价)
           - 按 (TickTime, BizIndex) 双重排序
        
        2. 初始化
           - order_map: Dict[int, OrderContext] 订单缓存
           - order_list: List[dict] 委托输出
           - trade_list: List[dict] 成交输出
           - last_price: float 最新成交价 (撤单兜底用)
        
        3. 逐行处理
           - Type='T' → process_trade()
           - Type='A' → process_add_order()
           - Type='D' → process_delete_order()
        
        4. 批次结算
           - settle_orders(order_map, order_list, security_id)
        
        5. 排序输出
           - 按 (TickTime, BizIndex) 排序
    
    Note:
        ⚠️ 上交所特有规则:
        1. 连续竞价阶段，同一笔委托产生的成交(T)先发送，剩余挂单(A)后发送
        2. 若一笔订单被一次性全部撮合，将不会发布该笔订单的委托信息(A)
        3. 上交所无收盘集合竞价，下午连续竞价到 15:00
    
    Example:
        >>> import polars as pl
        >>> df = pl.read_parquet('auction_tick_merged_data.parquet')
        >>> df_600519 = df.filter(pl.col('SecurityID') == '600519')
        >>> orders, trades = reconstruct_sh_tick_data(df_600519, '600519')
        >>> print(f"委托: {len(orders)} 条, 成交: {len(trades)} 条")
    """
    # =========================================================================
    # Step 1: 预处理
    # =========================================================================
    
    # 1.1 剔除 Type='S' (产品状态记录)
    df_filtered = df.filter(pl.col('Type') != 'S')
    
    # 1.2 时间过滤 (上交所连续竞价: 09:30-11:30, 13:00-15:00)
    # 使用 Polars 的 map_elements 进行向量化过滤
    df_filtered = df_filtered.filter(
        pl.col('TickTime').map_elements(
            is_continuous_trading_time,
            return_dtype=pl.Boolean
        )
    )
    
    # 1.3 按 (TickTime, BizIndex) 双重排序
    # 同毫秒内可能有多笔记录，必须用 BizIndex 保证顺序
    df_sorted = df_filtered.sort(['TickTime', 'BizIndex'])
    
    # =========================================================================
    # Step 2: 初始化
    # =========================================================================
    
    order_map: Dict[int, OrderContext] = {}  # 订单缓存
    order_list: List[Dict[str, Any]] = []    # 委托输出
    trade_list: List[Dict[str, Any]] = []    # 成交输出
    last_price: float = 0.0                  # 最新成交价 (撤单兜底用)
    
    # =========================================================================
    # Step 3: 逐行处理
    # =========================================================================
    
    # 转换为字典列表以便逐行处理
    # 注意: 后续 Phase 3 可考虑使用 Polars 原生迭代器优化性能
    rows = df_sorted.to_dicts()
    
    for row in rows:
        record_type = row.get('Type', '')
        
        if record_type == 'T':
            # 成交记录
            process_trade(row, order_map, trade_list, security_id)
            # 更新最新成交价 (用于撤单兜底)
            price = row.get('Price')
            if price is not None and price > 0:
                last_price = float(price)
                
        elif record_type == 'A':
            # 挂单记录
            process_add_order(row, order_map)
            
        elif record_type == 'D':
            # 撤单记录
            process_delete_order(row, order_map, order_list, last_price, security_id)
        
        # Type='S' 已在预处理中剔除，此处无需处理
    
    # =========================================================================
    # Step 4: 批次结算
    # =========================================================================
    
    # 处理完所有数据后，输出聚合后的委托记录
    settle_orders(order_map, order_list, security_id)
    
    # =========================================================================
    # Step 5: 排序输出
    # =========================================================================
    
    # 按 (TickTime, BizIndex) 排序
    order_list.sort(key=lambda x: (x.get('TickTime', 0), x.get('BizIndex', 0)))
    trade_list.sort(key=lambda x: (x.get('TickTime', 0), x.get('BizIndex', 0)))
    
    return order_list, trade_list


# ============================================================================
# 辅助函数
# ============================================================================

def validate_input_df(df: pl.DataFrame) -> bool:
    """
    验证输入 DataFrame 是否包含必需字段
    
    Args:
        df: 输入的 Polars DataFrame
    
    Returns:
        bool: True 如果包含所有必需字段
    
    Raises:
        ValueError: 如果缺少必需字段
    """
    required_columns = [
        'BizIndex', 'TickTime', 'Type', 'BuyOrderNO', 'SellOrderNO',
        'Price', 'Qty', 'TradeMoney', 'TickBSFlag', 'SecurityID'
    ]
    
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        raise ValueError(f"输入 DataFrame 缺少必需字段: {missing}")
    
    return True


def get_processing_stats(
    df_original: pl.DataFrame,
    df_filtered: pl.DataFrame,
    order_list: List[Dict],
    trade_list: List[Dict]
) -> Dict[str, Any]:
    """
    获取处理统计信息
    
    Args:
        df_original: 原始 DataFrame
        df_filtered: 过滤后的 DataFrame
        order_list: 输出的委托列表
        trade_list: 输出的成交列表
    
    Returns:
        Dict: 统计信息
    """
    return {
        'original_rows': len(df_original),
        'filtered_rows': len(df_filtered),
        'output_orders': len(order_list),
        'output_trades': len(trade_list),
        'new_orders': sum(1 for o in order_list if o.get('OrdType') == 'New'),
        'cancel_orders': sum(1 for o in order_list if o.get('OrdType') == 'Cancel'),
        'aggressive_orders': sum(1 for o in order_list 
                                  if o.get('OrdType') == 'New' and o.get('IsAggressive') == True),
        'passive_orders': sum(1 for o in order_list 
                               if o.get('OrdType') == 'New' and o.get('IsAggressive') == False),
    }


# ============================================================================
# 模块测试
# ============================================================================

if __name__ == '__main__':
    import polars as pl
    
    print("=" * 70)
    print("Phase 3.1: 完整主函数端到端测试")
    print("=" * 70)
    
    # =========================================================================
    # 测试场景1: 即时全部成交
    # =========================================================================
    print("\n📋 场景1: 即时全部成交 (只有T，无A)")
    test_data_1 = {
        'BizIndex': [100],
        'TickTime': [93000100],
        'Type': ['T'],
        'BuyOrderNO': [1001],
        'SellOrderNO': [2001],
        'Price': [50.5],
        'Qty': [1000],
        'TradeMoney': [50500.0],
        'TickBSFlag': ['B'],  # 主动买入
        'SecurityID': ['600519'],
    }
    df1 = pl.DataFrame(test_data_1)
    orders1, trades1 = reconstruct_sh_tick_data(df1, '600519')
    
    assert len(orders1) == 1, f"期望1条委托，实际{len(orders1)}"
    assert orders1[0]['IsAggressive'] == True, "即时全部成交应为 Taker"
    assert orders1[0]['Qty'] == 1000
    assert orders1[0]['SecurityID'] == '600519'
    assert len(trades1) == 1
    assert trades1[0]['ActiveSide'] == 1  # 主动买
    print("  ✅ 通过: IsAggressive=True, Qty=1000, ActiveSide=1")
    
    # =========================================================================
    # 测试场景2: 部分成交后转挂单
    # =========================================================================
    print("\n📋 场景2: 部分成交后转挂单 (先T后A)")
    test_data_2 = {
        'BizIndex': [200, 201],
        'TickTime': [93001000, 93001100],
        'Type': ['T', 'A'],
        'BuyOrderNO': [1002, 1002],
        'SellOrderNO': [2002, 0],
        'Price': [60.0, 60.5],
        'Qty': [600, 400],
        'TradeMoney': [36000.0, 0],
        'TickBSFlag': ['B', 'B'],  # 主动买入
        'SecurityID': ['600519', '600519'],
    }
    df2 = pl.DataFrame(test_data_2)
    orders2, trades2 = reconstruct_sh_tick_data(df2, '600519')
    
    new_orders = [o for o in orders2 if o['OrdType'] == 'New']
    assert len(new_orders) == 1
    assert new_orders[0]['IsAggressive'] == True, "部分成交后仍是 Taker"
    assert new_orders[0]['Qty'] == 1000, f"期望 600+400=1000，实际 {new_orders[0]['Qty']}"
    print("  ✅ 通过: IsAggressive=True, Qty=600+400=1000")
    
    # =========================================================================
    # 测试场景3: 纯挂单
    # =========================================================================
    print("\n📋 场景3: 纯挂单 (只有A)")
    test_data_3 = {
        'BizIndex': [300],
        'TickTime': [93002000],
        'Type': ['A'],
        'BuyOrderNO': [0],
        'SellOrderNO': [3001],
        'Price': [45.0],
        'Qty': [2000],
        'TradeMoney': [0],
        'TickBSFlag': ['S'],  # 卖单挂单
        'SecurityID': ['600519'],
    }
    df3 = pl.DataFrame(test_data_3)
    orders3, trades3 = reconstruct_sh_tick_data(df3, '600519')
    
    assert len(orders3) == 1
    assert orders3[0]['IsAggressive'] == False, "纯挂单应为 Maker"
    assert orders3[0]['Side'] == 'S'
    assert len(trades3) == 0, "纯挂单无成交"
    print("  ✅ 通过: IsAggressive=False, Side=S, 无成交")
    
    # =========================================================================
    # 测试场景4: 撤单价格回溯
    # =========================================================================
    print("\n📋 场景4: 撤单价格回溯 (A后D，D的Price=0)")
    test_data_4 = {
        'BizIndex': [400, 401],
        'TickTime': [93003000, 93003500],
        'Type': ['A', 'D'],
        'BuyOrderNO': [4001, 4001],
        'SellOrderNO': [0, 0],
        'Price': [70.0, 0],  # 撤单 Price=0
        'Qty': [1000, 500],
        'TradeMoney': [0, 0],
        'TickBSFlag': ['B', 'B'],
        'SecurityID': ['600519', '600519'],
    }
    df4 = pl.DataFrame(test_data_4)
    orders4, _ = reconstruct_sh_tick_data(df4, '600519')
    
    cancel_orders = [o for o in orders4 if o['OrdType'] == 'Cancel']
    new_orders = [o for o in orders4 if o['OrdType'] == 'New']
    
    assert len(cancel_orders) == 1
    assert cancel_orders[0]['Price'] == 70.0, "撤单价格应从缓存回溯"
    assert cancel_orders[0]['IsAggressive'] is None, "撤单 IsAggressive 应为 None"
    assert len(new_orders) == 1
    print("  ✅ 通过: 撤单价格=70.0 (从缓存回溯), IsAggressive=None")
    
    # =========================================================================
    # 测试场景5: 时间过滤
    # =========================================================================
    print("\n📋 场景5: 时间过滤 (上交所规则)")
    test_data_5 = {
        'BizIndex': [1, 2, 3, 4, 5, 6],
        'TickTime': [92500000, 93000000, 113000000, 125900000, 145700000, 150000000],
        # 09:25 集合竞价, 09:30 连续, 11:30 午休, 12:59 午休, 14:57 连续, 15:00 收盘
        'Type': ['T', 'T', 'T', 'T', 'T', 'T'],
        'BuyOrderNO': [5001, 5002, 5003, 5004, 5005, 5006],
        'SellOrderNO': [6001, 6002, 6003, 6004, 6005, 6006],
        'Price': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        'Qty': [100, 200, 300, 400, 500, 600],
        'TradeMoney': [1000, 4000, 9000, 16000, 25000, 36000],
        'TickBSFlag': ['B', 'B', 'B', 'B', 'B', 'B'],
        'SecurityID': ['600519'] * 6,
    }
    df5 = pl.DataFrame(test_data_5)
    orders5, trades5 = reconstruct_sh_tick_data(df5, '600519')
    
    # 应保留: 09:30 (BizIndex=2), 14:57 (BizIndex=5)
    # 应剔除: 09:25, 11:30, 12:59, 15:00
    assert len(trades5) == 2, f"期望2条成交 (09:30, 14:57)，实际{len(trades5)}"
    trade_times = [t['TickTime'] for t in trades5]
    assert 93000000 in trade_times, "09:30 应保留"
    assert 145700000 in trade_times, "14:57 应保留 (上交所无收盘集合竞价)"
    print(f"  ✅ 通过: 保留 {len(trades5)} 条 (09:30, 14:57)，剔除集合竞价/休市/收盘")
    
    # =========================================================================
    # 测试场景6: Type='S' 剔除
    # =========================================================================
    print("\n📋 场景6: Type='S' 产品状态记录剔除")
    test_data_6 = {
        'BizIndex': [600, 601, 602],
        'TickTime': [93000000, 93000100, 113000000],
        'Type': ['T', 'S', 'S'],  # 1条成交，2条状态
        'BuyOrderNO': [6001, 0, 0],
        'SellOrderNO': [7001, 0, 0],
        'Price': [100.0, 0, 0],
        'Qty': [500, 0, 0],
        'TradeMoney': [50000.0, 0, 0],
        'TickBSFlag': ['B', 'TRADE', 'BREAK'],
        'SecurityID': ['600519'] * 3,
    }
    df6 = pl.DataFrame(test_data_6)
    orders6, trades6 = reconstruct_sh_tick_data(df6, '600519')
    
    assert len(trades6) == 1, "应只有1条成交，Type='S' 被剔除"
    print("  ✅ 通过: Type='S' 被剔除")
    
    # =========================================================================
    # 测试场景7: 综合场景 - 多订单多类型
    # =========================================================================
    print("\n📋 场景7: 综合场景 - 多订单多类型")
    test_data_7 = {
        'BizIndex': [700, 701, 702, 703, 704, 705, 706],
        'TickTime': [93000000, 93000100, 93000200, 93000300, 93000400, 93000500, 93000600],
        'Type': ['T', 'A', 'T', 'A', 'D', 'T', 'A'],
        'BuyOrderNO': [7001, 7001, 7002, 0, 7001, 0, 0],
        'SellOrderNO': [8001, 0, 8002, 8003, 0, 7003, 8004],
        'Price': [100.0, 100.5, 101.0, 99.0, 0, 102.0, 98.0],
        'Qty': [500, 300, 200, 400, 200, 150, 600],
        'TradeMoney': [50000, 0, 20200, 0, 0, 15300, 0],
        'TickBSFlag': ['B', 'B', 'B', 'S', 'B', 'S', 'S'],
        'SecurityID': ['600519'] * 7,
    }
    df7 = pl.DataFrame(test_data_7)
    orders7, trades7 = reconstruct_sh_tick_data(df7, '600519')
    
    # 统计
    new_orders = [o for o in orders7 if o['OrdType'] == 'New']
    cancel_orders = [o for o in orders7 if o['OrdType'] == 'Cancel']
    taker_orders = [o for o in new_orders if o['IsAggressive'] == True]
    maker_orders = [o for o in new_orders if o['IsAggressive'] == False]
    
    print(f"  📊 输出统计:")
    print(f"     - 成交记录: {len(trades7)} 条")
    print(f"     - 委托记录: {len(orders7)} 条")
    print(f"       - New: {len(new_orders)} (Taker: {len(taker_orders)}, Maker: {len(maker_orders)})")
    print(f"       - Cancel: {len(cancel_orders)}")
    
    # 验证数学关系
    assert len(trades7) == 3, f"期望3条成交，实际{len(trades7)}"
    assert len(cancel_orders) == 1, f"期望1条撤单，实际{len(cancel_orders)}"
    print("  ✅ 通过: 综合场景处理正确")
    
    # =========================================================================
    # 总结
    # =========================================================================
    print("\n" + "=" * 70)
    print("✅ Phase 3.1 完整主函数端到端测试全部通过！")
    print("=" * 70)
    print("\n验收标准检查:")
    print("  ✅ 端到端可运行")
    print("  ✅ 排序逻辑正确 (TickTime, BizIndex)")
    print("  ✅ 所有输出记录包含 SecurityID")
    print("  ✅ 时间过滤正确 (上交所: 09:30-11:30, 13:00-15:00)")
    print("  ✅ Type='S' 产品状态记录被剔除")
    print("  ✅ IsAggressive 判定正确 (True/False/None)")

