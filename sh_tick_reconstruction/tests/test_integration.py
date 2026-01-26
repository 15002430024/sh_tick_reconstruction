#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Phase 3.1 完整主函数集成测试

测试 reconstruct_sh_tick_data() 端到端功能:
1. 即时全部成交
2. 部分成交后转挂单
3. 纯挂单
4. 撤单价格回溯
5. 时间过滤 (上交所规则)
6. Type='S' 剔除
7. 综合场景
"""

import sys
from pathlib import Path

# 添加项目路径
try:
    _file_path = Path(__file__).parent.parent
except NameError:
    _file_path = Path('.').resolve()
sys.path.insert(0, str(_file_path))

import polars as pl
from sh_tick_reconstruction.reconstructor import reconstruct_sh_tick_data
from sh_tick_reconstruction.time_filter import is_continuous_trading_time


def test_immediate_full_execution():
    """场景1: 即时全部成交 (只有T，无A)"""
    test_data = {
        'BizIndex': [100],
        'TickTime': [93000100],
        'Type': ['T'],
        'BuyOrderNO': [1001],
        'SellOrderNO': [2001],
        'Price': [50.5],
        'Qty': [1000],
        'TradeMoney': [50500.0],
        'TickBSFlag': ['B'],
        'SecurityID': ['600519'],
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    assert len(orders) == 1
    assert orders[0]['IsAggressive'] == True
    assert orders[0]['Qty'] == 1000
    assert orders[0]['SecurityID'] == '600519'
    assert len(trades) == 1
    assert trades[0]['ActiveSide'] == 1
    
    print("✓ 测试通过: 即时全部成交")


def test_partial_execution_then_resting():
    """场景2: 部分成交后转挂单 (先T后A)"""
    test_data = {
        'BizIndex': [200, 201],
        'TickTime': [93001000, 93001100],
        'Type': ['T', 'A'],
        'BuyOrderNO': [1002, 1002],
        'SellOrderNO': [2002, 0],
        'Price': [60.0, 60.5],
        'Qty': [600, 400],
        'TradeMoney': [36000.0, 0],
        'TickBSFlag': ['B', 'B'],
        'SecurityID': ['600519', '600519'],
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    new_orders = [o for o in orders if o['OrdType'] == 'New']
    assert len(new_orders) == 1
    assert new_orders[0]['IsAggressive'] == True
    assert new_orders[0]['Qty'] == 1000  # 600 + 400
    
    print("✓ 测试通过: 部分成交后转挂单")


def test_pure_resting_order():
    """场景3: 纯挂单 (只有A)"""
    test_data = {
        'BizIndex': [300],
        'TickTime': [93002000],
        'Type': ['A'],
        'BuyOrderNO': [0],
        'SellOrderNO': [3001],
        'Price': [45.0],
        'Qty': [2000],
        'TradeMoney': [0],
        'TickBSFlag': ['S'],
        'SecurityID': ['600519'],
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    assert len(orders) == 1
    assert orders[0]['IsAggressive'] == False
    assert orders[0]['Side'] == 'S'
    assert len(trades) == 0
    
    print("✓ 测试通过: 纯挂单")


def test_cancel_price_backfill():
    """场景4: 撤单价格回溯 (A后D，D的Price=0)"""
    test_data = {
        'BizIndex': [400, 401],
        'TickTime': [93003000, 93003500],
        'Type': ['A', 'D'],
        'BuyOrderNO': [4001, 4001],
        'SellOrderNO': [0, 0],
        'Price': [70.0, 0],
        'Qty': [1000, 500],
        'TradeMoney': [0, 0],
        'TickBSFlag': ['B', 'B'],
        'SecurityID': ['600519', '600519'],
    }
    df = pl.DataFrame(test_data)
    orders, _ = reconstruct_sh_tick_data(df, '600519')
    
    cancel_orders = [o for o in orders if o['OrdType'] == 'Cancel']
    assert len(cancel_orders) == 1
    assert cancel_orders[0]['Price'] == 70.0
    assert cancel_orders[0]['IsAggressive'] is None
    
    print("✓ 测试通过: 撤单价格回溯")


def test_time_filter_sh():
    """场景5: 时间过滤 (上交所规则: 09:30-11:30, 13:00-15:00)"""
    # 直接测试时间过滤函数
    assert is_continuous_trading_time(92500000) == False   # 09:25 集合竞价
    assert is_continuous_trading_time(93000000) == True    # 09:30 连续
    assert is_continuous_trading_time(113000000) == False  # 11:30 午休
    assert is_continuous_trading_time(125900000) == False  # 12:59 午休
    assert is_continuous_trading_time(130000000) == True   # 13:00 连续
    assert is_continuous_trading_time(145700000) == True   # 14:57 连续 (上交所!)
    assert is_continuous_trading_time(150000000) == False  # 15:00 收盘
    
    # 测试完整主函数的时间过滤
    test_data = {
        'BizIndex': [1, 2, 3, 4, 5, 6],
        'TickTime': [92500000, 93000000, 113000000, 125900000, 145700000, 150000000],
        'Type': ['T', 'T', 'T', 'T', 'T', 'T'],
        'BuyOrderNO': [5001, 5002, 5003, 5004, 5005, 5006],
        'SellOrderNO': [6001, 6002, 6003, 6004, 6005, 6006],
        'Price': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        'Qty': [100, 200, 300, 400, 500, 600],
        'TradeMoney': [1000, 4000, 9000, 16000, 25000, 36000],
        'TickBSFlag': ['B', 'B', 'B', 'B', 'B', 'B'],
        'SecurityID': ['600519'] * 6,
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    assert len(trades) == 2
    trade_times = [t['TickTime'] for t in trades]
    assert 93000000 in trade_times
    assert 145700000 in trade_times
    
    print("✓ 测试通过: 时间过滤 (上交所规则)")


def test_type_s_excluded():
    """场景6: Type='S' 产品状态记录剔除"""
    test_data = {
        'BizIndex': [600, 601, 602],
        'TickTime': [93000000, 93000100, 93000200],
        'Type': ['T', 'S', 'S'],
        'BuyOrderNO': [6001, 0, 0],
        'SellOrderNO': [7001, 0, 0],
        'Price': [100.0, 0, 0],
        'Qty': [500, 0, 0],
        'TradeMoney': [50000.0, 0, 0],
        'TickBSFlag': ['B', 'TRADE', 'BREAK'],
        'SecurityID': ['600519'] * 3,
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    assert len(trades) == 1
    
    print("✓ 测试通过: Type='S' 剔除")


def test_comprehensive_scenario():
    """场景7: 综合场景 - 多订单多类型"""
    test_data = {
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
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    new_orders = [o for o in orders if o['OrdType'] == 'New']
    cancel_orders = [o for o in orders if o['OrdType'] == 'Cancel']
    
    assert len(trades) == 3
    assert len(cancel_orders) == 1
    
    # 验证所有记录都有 SecurityID
    for order in orders:
        assert 'SecurityID' in order
        assert order['SecurityID'] == '600519'
    
    for trade in trades:
        assert 'SecurityID' in trade
        assert trade['SecurityID'] == '600519'
    
    print("✓ 测试通过: 综合场景")


def test_sorting_correctness():
    """场景8: 验证排序正确性 (TickTime, BizIndex)"""
    test_data = {
        'BizIndex': [103, 101, 102, 104],  # 乱序
        'TickTime': [93000000, 93000000, 93000000, 93000100],  # 同毫秒
        'Type': ['T', 'T', 'A', 'T'],
        'BuyOrderNO': [1003, 1001, 1001, 1004],
        'SellOrderNO': [2003, 2001, 0, 2004],
        'Price': [50.0, 50.0, 50.5, 51.0],
        'Qty': [300, 100, 200, 400],
        'TradeMoney': [15000, 5000, 0, 20400],
        'TickBSFlag': ['B', 'B', 'B', 'B'],
        'SecurityID': ['600519'] * 4,
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    # 验证输出按 (TickTime, BizIndex) 排序
    for i in range(len(orders) - 1):
        curr = (orders[i]['TickTime'], orders[i]['BizIndex'])
        next_item = (orders[i+1]['TickTime'], orders[i+1]['BizIndex'])
        assert curr <= next_item, f"委托排序错误: {curr} > {next_item}"
    
    for i in range(len(trades) - 1):
        curr = (trades[i]['TickTime'], trades[i]['BizIndex'])
        next_item = (trades[i+1]['TickTime'], trades[i+1]['BizIndex'])
        assert curr <= next_item, f"成交排序错误: {curr} > {next_item}"
    
    print("✓ 测试通过: 排序正确性")


def test_security_id_in_all_records():
    """场景9: 验证所有记录包含 SecurityID"""
    test_data = {
        'BizIndex': [900, 901, 902],
        'TickTime': [93000000, 93000100, 93000200],
        'Type': ['T', 'A', 'D'],
        'BuyOrderNO': [9001, 9002, 9002],
        'SellOrderNO': [9501, 0, 0],
        'Price': [100.0, 99.0, 0],
        'Qty': [500, 300, 100],
        'TradeMoney': [50000, 0, 0],
        'TickBSFlag': ['B', 'B', 'B'],
        'SecurityID': ['600519'] * 3,
    }
    df = pl.DataFrame(test_data)
    orders, trades = reconstruct_sh_tick_data(df, '600519')
    
    for order in orders:
        assert 'SecurityID' in order, "委托记录缺少 SecurityID"
        assert order['SecurityID'] == '600519'
    
    for trade in trades:
        assert 'SecurityID' in trade, "成交记录缺少 SecurityID"
        assert trade['SecurityID'] == '600519'
    
    print("✓ 测试通过: 所有记录包含 SecurityID")


if __name__ == '__main__':
    print("=" * 60)
    print("Phase 3.1 完整主函数集成测试")
    print("=" * 60)
    
    test_immediate_full_execution()
    test_partial_execution_then_resting()
    test_pure_resting_order()
    test_cancel_price_backfill()
    test_time_filter_sh()
    test_type_s_excluded()
    test_comprehensive_scenario()
    test_sorting_correctness()
    test_security_id_in_all_records()
    
    print("=" * 60)
    print("✅ 所有测试通过！")
    print("=" * 60)
    print("\n验收标准:")
    print("  ✅ 端到端可运行")
    print("  ✅ 排序逻辑正确 (TickTime, BizIndex)")
    print("  ✅ 所有输出记录包含 SecurityID")
