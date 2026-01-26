#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
settle_orders 函数单元测试

测试场景:
1. 即时全部成交（只有 trade_qty，无 resting_qty）
2. 部分成交后转挂单（有 trade_qty + resting_qty）
3. 纯挂单（只有 resting_qty）
4. 多订单批量结算
5. IsAggressive 正确传递（True/False）
6. 验证所有必需字段存在
"""

import sys
from pathlib import Path

# 添加项目路径 - 支持直接运行和模块导入
try:
    _file_path = Path(__file__).parent.parent
except NameError:
    _file_path = Path('.').resolve()
sys.path.insert(0, str(_file_path))

from sh_tick_reconstruction.models import OrderContext
from sh_tick_reconstruction.reconstructor import settle_orders


def test_immediate_full_execution():
    """
    场景1: 即时全部成交
    
    特征: 只有 Type='T' 记录，无 Type='A' 记录
    期望:
    - Qty = trade_qty (无 resting_qty)
    - Price = trade_price (无 resting_price)
    - IsAggressive = True
    """
    # 准备数据
    order_map = {
        1001: OrderContext(
            order_no=1001,
            side='B',
            first_time=93000100,
            first_biz_index=100,
            is_aggressive=True  # 首次出现是成交
        )
    }
    # 模拟成交
    order_map[1001].trade_qty = 1000
    order_map[1001].trade_price = 50.5
    # 无挂单 (resting_qty=0, resting_price=0)
    
    order_list = []
    security_id = '600519'
    
    # 执行
    settle_orders(order_map, order_list, security_id)
    
    # 验证
    assert len(order_list) == 1
    order = order_list[0]
    
    assert order['SecurityID'] == '600519'
    assert order['BizIndex'] == 100
    assert order['TickTime'] == 93000100
    assert order['OrdID'] == 1001
    assert order['OrdType'] == 'New'
    assert order['Side'] == 'B'
    assert order['Price'] == 50.5  # 使用 trade_price
    assert order['Qty'] == 1000    # 只有 trade_qty
    assert order['IsAggressive'] == True
    
    print("✓ 测试通过: 即时全部成交")


def test_partial_execution_then_resting():
    """
    场景2: 部分成交后转挂单
    
    特征: 先 Type='T' 后 Type='A'
    期望:
    - Qty = trade_qty + resting_qty
    - Price = resting_price (优先使用)
    - IsAggressive = True (首次出现是成交)
    """
    # 准备数据
    order_map = {
        1002: OrderContext(
            order_no=1002,
            side='S',
            first_time=93000200,
            first_biz_index=200,
            is_aggressive=True  # 首次出现是成交
        )
    }
    # 模拟成交 + 挂单
    order_map[1002].trade_qty = 600
    order_map[1002].trade_price = 100.0
    order_map[1002].resting_qty = 400
    order_map[1002].resting_price = 100.5  # 挂单价略高
    
    order_list = []
    security_id = '600000'
    
    # 执行
    settle_orders(order_map, order_list, security_id)
    
    # 验证
    assert len(order_list) == 1
    order = order_list[0]
    
    assert order['Qty'] == 1000   # 600 + 400
    assert order['Price'] == 100.5  # 优先用 resting_price
    assert order['IsAggressive'] == True
    
    print("✓ 测试通过: 部分成交后转挂单")


def test_pure_resting_order():
    """
    场景3: 纯挂单
    
    特征: 只有 Type='A' 记录，无成交
    期望:
    - Qty = resting_qty
    - Price = resting_price
    - IsAggressive = False (被动等待成交)
    """
    # 准备数据
    order_map = {
        1003: OrderContext(
            order_no=1003,
            side='B',
            first_time=93000300,
            first_biz_index=300,
            is_aggressive=False  # 首次出现是挂单
        )
    }
    # 只有挂单
    order_map[1003].resting_qty = 2000
    order_map[1003].resting_price = 45.0
    # 无成交 (trade_qty=0)
    
    order_list = []
    security_id = '000001'
    
    # 执行
    settle_orders(order_map, order_list, security_id)
    
    # 验证
    assert len(order_list) == 1
    order = order_list[0]
    
    assert order['Qty'] == 2000
    assert order['Price'] == 45.0
    assert order['IsAggressive'] == False  # 纯挂单是 Maker
    
    print("✓ 测试通过: 纯挂单")


def test_multiple_orders_batch_settle():
    """
    场景4: 多订单批量结算
    
    验证能正确处理多个订单的批量结算
    """
    # 准备多个订单
    order_map = {
        1001: OrderContext(
            order_no=1001, side='B',
            first_time=93000100, first_biz_index=100,
            is_aggressive=True
        ),
        1002: OrderContext(
            order_no=1002, side='S',
            first_time=93000200, first_biz_index=200,
            is_aggressive=False
        ),
        1003: OrderContext(
            order_no=1003, side='B',
            first_time=93000300, first_biz_index=300,
            is_aggressive=True
        )
    }
    
    order_map[1001].trade_qty = 1000
    order_map[1001].trade_price = 50.0
    
    order_map[1002].resting_qty = 500
    order_map[1002].resting_price = 60.0
    
    order_map[1003].trade_qty = 300
    order_map[1003].trade_price = 70.0
    order_map[1003].resting_qty = 200
    order_map[1003].resting_price = 70.5
    
    order_list = []
    
    # 执行
    settle_orders(order_map, order_list, '600519')
    
    # 验证
    assert len(order_list) == 3
    
    # 按 OrdID 排序以便验证
    orders_by_id = {o['OrdID']: o for o in order_list}
    
    assert orders_by_id[1001]['Qty'] == 1000
    assert orders_by_id[1001]['IsAggressive'] == True
    
    assert orders_by_id[1002]['Qty'] == 500
    assert orders_by_id[1002]['IsAggressive'] == False
    
    assert orders_by_id[1003]['Qty'] == 500  # 300 + 200
    assert orders_by_id[1003]['Price'] == 70.5  # 优先 resting_price
    
    print("✓ 测试通过: 多订单批量结算")


def test_is_aggressive_correctly_passed():
    """
    场景5: IsAggressive 正确传递
    
    验证:
    - True (首次出现是成交 → Taker)
    - False (首次出现是挂单 → Maker)
    - 不应该是 None
    """
    # Taker
    order_map_taker = {
        1001: OrderContext(
            order_no=1001, side='B',
            first_time=93000100, first_biz_index=100,
            is_aggressive=True
        )
    }
    order_map_taker[1001].trade_qty = 1000
    order_map_taker[1001].trade_price = 50.0
    
    # Maker
    order_map_maker = {
        1002: OrderContext(
            order_no=1002, side='S',
            first_time=93000200, first_biz_index=200,
            is_aggressive=False
        )
    }
    order_map_maker[1002].resting_qty = 500
    order_map_maker[1002].resting_price = 60.0
    
    order_list = []
    
    settle_orders(order_map_taker, order_list, '600519')
    settle_orders(order_map_maker, order_list, '600519')
    
    # 验证
    taker_order = [o for o in order_list if o['OrdID'] == 1001][0]
    maker_order = [o for o in order_list if o['OrdID'] == 1002][0]
    
    assert taker_order['IsAggressive'] is True   # 不是 None
    assert maker_order['IsAggressive'] is False  # 不是 None
    
    print("✓ 测试通过: IsAggressive 正确传递")


def test_all_required_fields_present():
    """
    场景6: 验证所有必需字段存在
    
    必需字段:
    - SecurityID, BizIndex, TickTime, OrdID, OrdType
    - Side, Price, Qty, IsAggressive
    """
    order_map = {
        1001: OrderContext(
            order_no=1001, side='B',
            first_time=93000100, first_biz_index=100,
            is_aggressive=True
        )
    }
    order_map[1001].trade_qty = 1000
    order_map[1001].trade_price = 50.0
    
    order_list = []
    settle_orders(order_map, order_list, '600519')
    
    # 验证所有必需字段
    required_fields = [
        'SecurityID', 'BizIndex', 'TickTime', 'OrdID', 'OrdType',
        'Side', 'Price', 'Qty', 'IsAggressive'
    ]
    
    order = order_list[0]
    for field in required_fields:
        assert field in order, f"缺少必需字段: {field}"
    
    # 验证字段值类型
    assert isinstance(order['SecurityID'], str)
    assert isinstance(order['BizIndex'], int)
    assert isinstance(order['TickTime'], int)
    assert isinstance(order['OrdID'], int)
    assert isinstance(order['OrdType'], str)
    assert isinstance(order['Side'], str)
    assert isinstance(order['Price'], (int, float))
    assert isinstance(order['Qty'], int)
    assert isinstance(order['IsAggressive'], bool)
    
    print("✓ 测试通过: 所有必需字段存在")


def test_skip_zero_qty_orders():
    """
    场景7: 跳过零数量订单
    
    如果 trade_qty + resting_qty = 0，不应输出
    """
    order_map = {
        1001: OrderContext(
            order_no=1001, side='B',
            first_time=93000100, first_biz_index=100,
            is_aggressive=True
        )
    }
    # 既没有成交也没有挂单
    # trade_qty=0, resting_qty=0 (默认值)
    
    order_list = []
    settle_orders(order_map, order_list, '600519')
    
    # 验证: 不应有输出
    assert len(order_list) == 0
    
    print("✓ 测试通过: 跳过零数量订单")


if __name__ == '__main__':
    print("=" * 60)
    print("settle_orders 函数单元测试")
    print("=" * 60)
    
    test_immediate_full_execution()
    test_partial_execution_then_resting()
    test_pure_resting_order()
    test_multiple_orders_batch_settle()
    test_is_aggressive_correctly_passed()
    test_all_required_fields_present()
    test_skip_zero_qty_orders()
    
    print("=" * 60)
    print("✅ 所有测试通过！")
    print("=" * 60)
