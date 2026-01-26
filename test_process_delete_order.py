# -*- coding: utf-8 -*-
"""
test_process_delete_order.py - Phase 2.3 单元测试
"""

from sh_tick_reconstruction.reconstructor import (
    process_delete_order, 
    process_add_order, 
    process_trade
)
from sh_tick_reconstruction.models import OrderContext


def test_process_delete_order():
    """测试 process_delete_order 函数核心逻辑"""
    
    order_map = {}
    order_list = []

    # =========================================================================
    # 测试 1: Level 0 - 数据源自带 Price > 0
    # =========================================================================
    print('=== 测试 1: Level 0 - 数据源自带价格 ===')
    row_cancel_with_price = {
        'BizIndex': 3001,
        'TickTime': 93002000,
        'BuyOrderNO': 100020,
        'SellOrderNO': 0,
        'Price': 10.5,  # 有价格
        'Qty': 500,
        'TickBSFlag': 'B'
    }
    process_delete_order(row_cancel_with_price, order_map, order_list, 0.0, '600519')
    
    assert len(order_list) == 1
    cancel = order_list[0]
    assert cancel['SecurityID'] == '600519'
    assert cancel['OrdType'] == 'Cancel'
    assert cancel['Price'] == 10.5  # 使用数据源自带价格
    assert cancel['IsAggressive'] is None  # ⭐ 撤单必须是 None
    assert cancel['BizIndex'] == 3001  # ⭐ 撤单记录自身的 BizIndex
    assert cancel['Side'] == 'B'
    print(f'Level 0: Price={cancel["Price"]}, IsAggressive={cancel["IsAggressive"]} ✓')

    # =========================================================================
    # 测试 2: Level 1 - 从缓存回溯价格
    # =========================================================================
    print('\n=== 测试 2: Level 1 - 从缓存回溯价格 ===')
    
    # 2.1 先建立缓存（挂单）
    row_add = {
        'BizIndex': 3002,
        'TickTime': 93002100,
        'BuyOrderNO': 100021,
        'SellOrderNO': 0,
        'Price': 10.8,
        'Qty': 1000,
        'TickBSFlag': 'B'
    }
    process_add_order(row_add, order_map)
    
    # 2.2 撤单，Price=0
    row_cancel_no_price = {
        'BizIndex': 3003,
        'TickTime': 93002200,
        'BuyOrderNO': 100021,  # 同一订单号
        'SellOrderNO': 0,
        'Price': 0,  # 无价格
        'Qty': 500,
        'TickBSFlag': 'B'
    }
    process_delete_order(row_cancel_no_price, order_map, order_list, 9.9, '600519')
    
    cancel2 = order_list[-1]
    assert cancel2['Price'] == 10.8  # 从缓存回溯
    assert cancel2['IsAggressive'] is None
    print(f'Level 1 (缓存回溯): Price={cancel2["Price"]} (应为10.8) ✓')

    # =========================================================================
    # 测试 3: Level 2 - 兜底用 last_price
    # =========================================================================
    print('\n=== 测试 3: Level 2 - 兜底用 last_price ===')
    
    # 撤单一个不在缓存中的订单，Price=0
    row_cancel_unknown = {
        'BizIndex': 3004,
        'TickTime': 93002300,
        'BuyOrderNO': 0,
        'SellOrderNO': 200030,  # 不在缓存中
        'Price': None,  # 无价格
        'Qty': 200,
        'TickBSFlag': 'S'
    }
    last_price = 10.65
    process_delete_order(row_cancel_unknown, order_map, order_list, last_price, '600519')
    
    cancel3 = order_list[-1]
    assert cancel3['Price'] == 10.65  # 兜底用 last_price
    assert cancel3['Side'] == 'S'
    assert cancel3['IsAggressive'] is None
    print(f'Level 2 (last_price 兜底): Price={cancel3["Price"]} (应为10.65) ✓')

    # =========================================================================
    # 测试 4: 部分成交后撤单（从成交记录建立的缓存回溯）
    # =========================================================================
    print('\n=== 测试 4: 部分成交后撤单 ===')
    
    # 4.1 先有成交
    trade_list = []
    row_trade = {
        'BizIndex': 3005,
        'TickTime': 93002400,
        'BuyOrderNO': 100025,
        'SellOrderNO': 200025,
        'Price': 10.9,
        'Qty': 300,
        'TradeMoney': 3270.0,
        'TickBSFlag': 'B'  # 主动买
    }
    process_trade(row_trade, order_map, trade_list, '600519')
    
    # 4.2 然后撤销剩余（Price=0）
    row_cancel_after_trade = {
        'BizIndex': 3006,
        'TickTime': 93002500,
        'BuyOrderNO': 100025,
        'SellOrderNO': 0,
        'Price': 0,
        'Qty': 700,
        'TickBSFlag': 'B'
    }
    process_delete_order(row_cancel_after_trade, order_map, order_list, 9.0, '600519')
    
    cancel4 = order_list[-1]
    assert cancel4['Price'] == 10.9  # 从成交记录建立的缓存中回溯
    assert cancel4['OrdID'] == 100025
    print(f'部分成交后撤单: Price={cancel4["Price"]} (应为10.9) ✓')

    # =========================================================================
    # 测试 5: 验证 Price=0 和 Price=None 的区别处理
    # =========================================================================
    print('\n=== 测试 5: Price=0 vs Price=None ===')
    
    # Price=0 应该触发回溯
    row_price_zero = {
        'BizIndex': 3007,
        'TickTime': 93002600,
        'BuyOrderNO': 0,
        'SellOrderNO': 200035,
        'Price': 0,  # 明确为 0
        'Qty': 100,
        'TickBSFlag': 'S'
    }
    process_delete_order(row_price_zero, order_map, order_list, 11.0, '600519')
    assert order_list[-1]['Price'] == 11.0  # 兜底
    print('Price=0 触发兜底 ✓')

    # =========================================================================
    # 测试 6: 验证所有必需字段存在
    # =========================================================================
    print('\n=== 测试 6: 验证必需字段 ===')
    
    required_fields = ['SecurityID', 'BizIndex', 'TickTime', 'OrdID', 
                       'OrdType', 'Side', 'Price', 'Qty', 'IsAggressive']
    
    for field in required_fields:
        assert field in order_list[0], f"缺少字段: {field}"
    print(f'所有必需字段存在: {required_fields} ✓')

    print('\n' + '=' * 50)
    print('✅ 所有 process_delete_order 测试通过!')
    print('=' * 50)


if __name__ == '__main__':
    test_process_delete_order()
