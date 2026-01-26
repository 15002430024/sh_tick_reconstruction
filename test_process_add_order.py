# -*- coding: utf-8 -*-
"""
test_process_add_order.py - Phase 2.2 单元测试
"""

from sh_tick_reconstruction.reconstructor import process_add_order, process_trade
from sh_tick_reconstruction.models import OrderContext


def test_process_add_order():
    """测试 process_add_order 函数核心逻辑"""
    
    order_map = {}

    # =========================================================================
    # 测试 1: 纯挂单（首次出现为 Type='A'）
    # =========================================================================
    print('=== 测试 1: 纯挂单 ===')
    row_pure_resting = {
        'BizIndex': 2001,
        'TickTime': 93001000,
        'BuyOrderNO': 100010,
        'SellOrderNO': 0,
        'Price': 10.5,
        'Qty': 1000,
        'TickBSFlag': 'B'
    }
    process_add_order(row_pure_resting, order_map)
    
    # 验证新订单创建
    assert 100010 in order_map
    ctx = order_map[100010]
    assert ctx.side == 'B'
    assert ctx.is_aggressive == False  # ⭐ 纯挂单 = Maker
    assert ctx.resting_qty == 1000
    assert ctx.resting_price == 10.5
    assert ctx.has_resting == True
    assert ctx.trade_qty == 0  # 没有成交
    print(f'纯挂单: is_aggressive={ctx.is_aggressive}, resting_qty={ctx.resting_qty} ✓')

    # =========================================================================
    # 测试 2: 部分成交后转挂单（先 Type='T'，后 Type='A'）
    # =========================================================================
    print('\n=== 测试 2: 部分成交后转挂单 ===')
    
    # 2.1 先发送成交记录 (Type='T')
    row_trade = {
        'BizIndex': 2002,
        'TickTime': 93001100,
        'BuyOrderNO': 100011,
        'SellOrderNO': 200011,
        'Price': 10.6,
        'Qty': 600,
        'TradeMoney': 6360.0,
        'TickBSFlag': 'B'  # 主动买入
    }
    trade_list = []
    process_trade(row_trade, order_map, trade_list, '600519')
    
    # 验证成交记录产生的缓存
    assert 100011 in order_map
    ctx_before = order_map[100011]
    assert ctx_before.is_aggressive == True  # Taker
    assert ctx_before.trade_qty == 600
    assert ctx_before.resting_qty == 0  # 还没有挂单部分
    assert ctx_before.has_resting == False
    print(f'成交记录: is_aggressive={ctx_before.is_aggressive}, trade_qty={ctx_before.trade_qty} ✓')
    
    # 2.2 后发送挂单记录 (Type='A')
    row_add = {
        'BizIndex': 2003,
        'TickTime': 93001100,  # 同一毫秒
        'BuyOrderNO': 100011,  # 同一订单号
        'SellOrderNO': 0,
        'Price': 10.6,
        'Qty': 400,
        'TickBSFlag': 'B'
    }
    process_add_order(row_add, order_map)
    
    # 验证：is_aggressive 保持 True，累加 resting_qty
    ctx_after = order_map[100011]
    assert ctx_after.is_aggressive == True  # ⭐ 保持 True（因为先主动成交了）
    assert ctx_after.trade_qty == 600  # 成交部分不变
    assert ctx_after.resting_qty == 400  # 新增挂单部分
    assert ctx_after.resting_price == 10.6
    assert ctx_after.has_resting == True
    assert ctx_after.first_biz_index == 2002  # ⭐ 保持首次出现的 BizIndex（成交记录的）
    print(f'部分成交后挂单: is_aggressive={ctx_after.is_aggressive}, ' 
          f'trade_qty={ctx_after.trade_qty}, resting_qty={ctx_after.resting_qty} ✓')

    # =========================================================================
    # 测试 3: 卖单挂单
    # =========================================================================
    print('\n=== 测试 3: 卖单挂单 ===')
    row_sell = {
        'BizIndex': 2004,
        'TickTime': 93001200,
        'BuyOrderNO': 0,
        'SellOrderNO': 200012,
        'Price': 10.8,
        'Qty': 500,
        'TickBSFlag': 'S'
    }
    process_add_order(row_sell, order_map)
    
    assert 200012 in order_map
    ctx_sell = order_map[200012]
    assert ctx_sell.side == 'S'
    assert ctx_sell.is_aggressive == False
    assert ctx_sell.resting_qty == 500
    print(f'卖单挂单: side={ctx_sell.side}, is_aggressive={ctx_sell.is_aggressive} ✓')

    # =========================================================================
    # 测试 4: 同一订单多次挂单累加
    # =========================================================================
    print('\n=== 测试 4: 多次挂单累加 ===')
    row_add2 = {
        'BizIndex': 2005,
        'TickTime': 93001300,
        'BuyOrderNO': 100010,  # 同一订单号（测试1的）
        'SellOrderNO': 0,
        'Price': 10.5,
        'Qty': 200,
        'TickBSFlag': 'B'
    }
    process_add_order(row_add2, order_map)
    
    ctx_add2 = order_map[100010]
    assert ctx_add2.resting_qty == 1200  # 1000 + 200
    assert ctx_add2.is_aggressive == False  # 保持原值
    print(f'多次挂单累加: resting_qty={ctx_add2.resting_qty} (应为1200) ✓')

    # =========================================================================
    # 测试 5: get_total_qty() 计算正确性
    # =========================================================================
    print('\n=== 测试 5: 总量计算 ===')
    # 测试订单 100011: trade_qty=600, resting_qty=400
    total_qty = order_map[100011].get_total_qty()
    assert total_qty == 1000
    print(f'总量计算: {total_qty} (应为1000) ✓')

    # =========================================================================
    # 测试 6: get_price() 优先级正确
    # =========================================================================
    print('\n=== 测试 6: 价格获取优先级 ===')
    # 订单 100011: resting_price=10.6, trade_price=10.6
    price = order_map[100011].get_price()
    assert price == 10.6  # 优先 resting_price
    print(f'价格获取: {price} (应为10.6) ✓')

    print('\n' + '=' * 50)
    print('✅ 所有 process_add_order 测试通过!')
    print('=' * 50)


if __name__ == '__main__':
    test_process_add_order()
