# -*- coding: utf-8 -*-
"""
test_process_trade.py - Phase 2.1 单元测试
"""

from sh_tick_reconstruction.reconstructor import process_trade
from sh_tick_reconstruction.models import OrderContext


def test_process_trade():
    """测试 process_trade 函数核心逻辑"""
    
    # 测试数据
    order_map = {}
    trade_list = []

    # 测试 1: 主动买入 (TickBSFlag='B')
    row_buy = {
        'BizIndex': 1001,
        'TickTime': 93000500,
        'BuyOrderNO': 100001,
        'SellOrderNO': 200001,
        'Price': 10.5,
        'Qty': 100,
        'TradeMoney': 1050.0,
        'TickBSFlag': 'B'
    }
    process_trade(row_buy, order_map, trade_list, '600519')

    print('=== 测试 1: 主动买入 ===')
    print(f'trade_list: {trade_list}')
    print(f'order_map keys: {list(order_map.keys())}')
    
    # 验证成交记录
    assert len(trade_list) == 1
    trade = trade_list[0]
    assert trade['SecurityID'] == '600519'
    assert trade['ActiveSide'] == 1  # 主动买
    assert trade['BidOrdID'] == 100001
    assert trade['AskOrdID'] == 200001
    print('成交记录 ✓')
    
    # 验证主动方订单被缓存
    assert 100001 in order_map
    ctx = order_map[100001]
    assert ctx.side == 'B'
    assert ctx.is_aggressive == True
    assert ctx.trade_qty == 100
    print(f'主动方订单: is_aggressive={ctx.is_aggressive}, trade_qty={ctx.trade_qty} ✓')

    # 测试 2: 主动卖出 (TickBSFlag='S')
    row_sell = {
        'BizIndex': 1002,
        'TickTime': 93000600,
        'BuyOrderNO': 100002,
        'SellOrderNO': 200002,
        'Price': 10.6,
        'Qty': 200,
        'TradeMoney': 2120.0,
        'TickBSFlag': 'S'
    }
    process_trade(row_sell, order_map, trade_list, '600519')

    print('\n=== 测试 2: 主动卖出 ===')
    assert len(trade_list) == 2
    trade = trade_list[1]
    assert trade['ActiveSide'] == 2  # 主动卖
    print('成交记录 ActiveSide=2 ✓')
    
    # 主动方应该是卖方 (200002)
    assert 200002 in order_map
    ctx = order_map[200002]
    assert ctx.side == 'S'
    assert ctx.is_aggressive == True
    print(f'主动方订单: side={ctx.side}, is_aggressive={ctx.is_aggressive} ✓')

    # 测试 3: 集合竞价 (TickBSFlag='N')
    row_auction = {
        'BizIndex': 1003,
        'TickTime': 92500000,
        'BuyOrderNO': 100003,
        'SellOrderNO': 200003,
        'Price': 10.4,
        'Qty': 500,
        'TradeMoney': 5200.0,
        'TickBSFlag': 'N'
    }
    process_trade(row_auction, order_map, trade_list, '600519')

    print('\n=== 测试 3: 集合竞价 ===')
    # 成交记录应该输出
    assert len(trade_list) == 3
    trade = trade_list[2]
    assert trade['ActiveSide'] == 0  # 集合竞价
    print('成交记录 ActiveSide=0 ✓')
    
    # 但不应该还原任何委托
    assert 100003 not in order_map
    assert 200003 not in order_map
    print(f'order_map 数量: {len(order_map)} (应为2) ✓')

    # 测试 4: 同一订单多次成交（累加）
    row_buy2 = {
        'BizIndex': 1004,
        'TickTime': 93000700,
        'BuyOrderNO': 100001,  # 同一买单
        'SellOrderNO': 200004,
        'Price': 10.55,
        'Qty': 150,
        'TradeMoney': 1582.5,
        'TickBSFlag': 'B'
    }
    process_trade(row_buy2, order_map, trade_list, '600519')

    print('\n=== 测试 4: 同一订单累加 ===')
    ctx = order_map[100001]
    assert ctx.trade_qty == 250  # 100 + 150
    assert ctx.trade_price == 10.55  # 最新成交价
    print(f'累计成交量: {ctx.trade_qty} (应为250) ✓')
    print(f'最新成交价: {ctx.trade_price} (应为10.55) ✓')

    # 测试 5: TradeMoney 为空时自动计算
    row_no_money = {
        'BizIndex': 1005,
        'TickTime': 93000800,
        'BuyOrderNO': 100005,
        'SellOrderNO': 200005,
        'Price': 10.0,
        'Qty': 100,
        'TradeMoney': None,  # 空值
        'TickBSFlag': 'B'
    }
    process_trade(row_no_money, order_map, trade_list, '600519')
    
    print('\n=== 测试 5: TradeMoney 自动计算 ===')
    trade = trade_list[-1]
    assert trade['TradeMoney'] == 1000.0  # 10.0 * 100
    print(f'TradeMoney: {trade["TradeMoney"]} (应为1000.0) ✓')

    print('\n' + '=' * 50)
    print('✅ 所有 process_trade 测试通过!')
    print('=' * 50)


if __name__ == '__main__':
    test_process_trade()
