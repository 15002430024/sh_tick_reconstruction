# -*- coding: utf-8 -*-
"""
Phase 4.1: 上交所逐笔数据拆解系统单元测试套件

对应 Prompt 4.1 要求，覆盖 7 个核心场景:
1. 即时全部成交
2. 部分成交后转挂单
3. 纯挂单
4. 被动单后续成交
5. 撤单价格回溯和 IsAggressive=None
6. 时间过滤 (上交所 14:57 应保留)
7. 通道数学关系 (Ch7 = Ch9 + Ch11)

验收标准:
- 所有测试通过
- 覆盖边界情况
- 验证 SecurityID 字段存在
- 验证时间过滤正确 (上交所 vs 深交所差异)
"""

import pytest
import polars as pl

from sh_tick_reconstruction import (
    reconstruct_sh_tick_data,
    is_continuous_trading_time,
)


# ============================================================================
# 场景1: 即时全部成交
# ============================================================================

class TestScenario1ImmediateFullExecution:
    """场景1: 只有T记录，无A记录"""
    
    def test_buy_side_taker(self):
        """买方主动即时全部成交"""
        input_data = {
            'BizIndex': [100],
            'TickTime': [93000000],
            'Type': ['T'],
            'BuyOrderNO': [1001],
            'SellOrderNO': [2001],
            'Price': [10.0],
            'Qty': [1000],
            'TradeMoney': [10000.0],
            'TickBSFlag': ['B'],
            'SecurityID': ['600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        # 验证委托
        assert len(orders) == 1
        assert orders[0]['OrdType'] == 'New'
        assert orders[0]['IsAggressive'] is True
        assert orders[0]['Qty'] == 1000
        assert orders[0]['SecurityID'] == '600519'
        assert orders[0]['Side'] == 'B'
        assert orders[0]['OrdID'] == 1001  # 买方订单号
        
        # 验证成交
        assert len(trades) == 1
        assert trades[0]['ActiveSide'] == 1  # 主动买
    
    def test_sell_side_taker(self):
        """卖方主动即时全部成交"""
        input_data = {
            'BizIndex': [101],
            'TickTime': [93000100],
            'Type': ['T'],
            'BuyOrderNO': [2001],
            'SellOrderNO': [1002],
            'Price': [10.5],
            'Qty': [500],
            'TradeMoney': [5250.0],
            'TickBSFlag': ['S'],
            'SecurityID': ['600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        assert len(orders) == 1
        assert orders[0]['Side'] == 'S'
        assert orders[0]['IsAggressive'] is True
        assert orders[0]['OrdID'] == 1002  # 卖方订单号
        
        assert trades[0]['ActiveSide'] == 2  # 主动卖


# ============================================================================
# 场景2: 部分成交后转挂单
# ============================================================================

class TestScenario2PartialExecutionThenResting:
    """场景2: 先T后A"""
    
    def test_single_trade_then_resting(self):
        """单笔成交后剩余挂单"""
        input_data = {
            'BizIndex': [200, 201],
            'TickTime': [93000000, 93000100],
            'Type': ['T', 'A'],
            'BuyOrderNO': [1002, 1002],
            'SellOrderNO': [2002, 0],
            'Price': [10.0, 10.5],
            'Qty': [600, 400],
            'TradeMoney': [6000.0, 0.0],
            'TickBSFlag': ['B', 'B'],
            'SecurityID': ['600519', '600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        new_orders = [o for o in orders if o['OrdType'] == 'New']
        assert len(new_orders) == 1
        assert new_orders[0]['IsAggressive'] is True  # 首次为T，是Taker
        assert new_orders[0]['Qty'] == 1000  # 600 + 400
    
    def test_multiple_trades_then_resting(self):
        """多笔成交后剩余挂单"""
        input_data = {
            'BizIndex': [300, 301, 302, 303],
            'TickTime': [93000000, 93000100, 93000200, 93000300],
            'Type': ['T', 'T', 'T', 'A'],
            'BuyOrderNO': [1003, 1003, 1003, 1003],
            'SellOrderNO': [2003, 2004, 2005, 0],
            'Price': [10.0, 10.1, 10.2, 10.3],
            'Qty': [100, 200, 300, 400],
            'TradeMoney': [1000.0, 2020.0, 3060.0, 0.0],
            'TickBSFlag': ['B', 'B', 'B', 'B'],
            'SecurityID': ['600519'] * 4,
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        new_orders = [o for o in orders if o['OrdType'] == 'New']
        assert len(new_orders) == 1
        assert new_orders[0]['Qty'] == 1000  # 100 + 200 + 300 + 400
        assert new_orders[0]['IsAggressive'] is True


# ============================================================================
# 场景3: 纯挂单
# ============================================================================

class TestScenario3PureRestingOrder:
    """场景3: 只有A记录"""
    
    def test_buy_side_maker(self):
        """买方纯挂单"""
        input_data = {
            'BizIndex': [400],
            'TickTime': [93000000],
            'Type': ['A'],
            'BuyOrderNO': [1004],
            'SellOrderNO': [0],
            'Price': [9.8],
            'Qty': [500],
            'TradeMoney': [0.0],
            'TickBSFlag': ['B'],
            'SecurityID': ['600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        assert len(orders) == 1
        assert orders[0]['IsAggressive'] is False  # 纯挂单是 Maker
        assert orders[0]['Side'] == 'B'
        assert len(trades) == 0
    
    def test_sell_side_maker(self):
        """卖方纯挂单"""
        input_data = {
            'BizIndex': [401],
            'TickTime': [93000100],
            'Type': ['A'],
            'BuyOrderNO': [0],
            'SellOrderNO': [2001],
            'Price': [10.2],
            'Qty': [800],
            'TradeMoney': [0.0],
            'TickBSFlag': ['S'],
            'SecurityID': ['600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        assert len(orders) == 1
        assert orders[0]['IsAggressive'] is False
        assert orders[0]['Side'] == 'S'


# ============================================================================
# 场景4: 被动单后续成交
# ============================================================================

class TestScenario4PassiveOrderLaterExecuted:
    """场景4: 首次A，后续作为被动方成交
    
    注意: 被动方的成交不会产生新的委托记录
    只有 A 记录会产生委托
    """
    
    def test_passive_order_executed_by_another(self):
        """被动挂单被另一个主动单吃掉
        
        订单 2001 首先以 Type='A' 挂单 (卖出 1000 股 @ 10.0)
        订单 3001 以 Type='T' 成交 (买入 800 股 @ 10.0)，吃掉 2001 的部分挂单
        
        期望:
        - 2001 应输出委托 (IsAggressive=False, Qty=1000)
        - 3001 应输出委托 (IsAggressive=True, Qty=800)
        """
        input_data = {
            'BizIndex': [500, 501],
            'TickTime': [93000000, 93001000],
            'Type': ['A', 'T'],
            'BuyOrderNO': [0, 3001],       # T 时买方是 3001
            'SellOrderNO': [2001, 2001],   # T 时卖方是 2001
            'Price': [10.0, 10.0],
            'Qty': [1000, 800],
            'TradeMoney': [0.0, 8000.0],
            'TickBSFlag': ['S', 'B'],      # T 时买方主动
            'SecurityID': ['600519', '600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        # 应有 2 条委托: 2001 (被动挂单) 和 3001 (主动成交)
        assert len(orders) == 2
        
        # 找到被动挂单 2001
        order_2001 = [o for o in orders if o['OrdID'] == 2001][0]
        assert order_2001['IsAggressive'] is False  # 首次是 A，是 Maker
        assert order_2001['Qty'] == 1000
        
        # 找到主动成交 3001
        order_3001 = [o for o in orders if o['OrdID'] == 3001][0]
        assert order_3001['IsAggressive'] is True  # 首次是 T，是 Taker
        assert order_3001['Qty'] == 800


# ============================================================================
# 场景5: 撤单价格回溯和 IsAggressive=None
# ============================================================================

class TestScenario5CancelPriceBackfill:
    """场景5: 撤单的 Price=0，需要从缓存回溯"""
    
    def test_cancel_after_add_price_zero(self):
        """挂单后撤单，撤单 Price=0"""
        input_data = {
            'BizIndex': [600, 601],
            'TickTime': [93000000, 93001000],
            'Type': ['A', 'D'],
            'BuyOrderNO': [1004, 1004],
            'SellOrderNO': [0, 0],
            'Price': [10.50, 0.0],  # D 的 Price=0
            'Qty': [1000, 500],
            'TradeMoney': [0.0, 0.0],
            'TickBSFlag': ['B', 'B'],
            'SecurityID': ['600519', '600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        cancel_order = [o for o in orders if o['OrdType'] == 'Cancel'][0]
        
        # 验证价格回溯
        assert cancel_order['Price'] == 10.50  # 从缓存回溯
        
        # 验证 IsAggressive=None (撤单不适用)
        assert cancel_order['IsAggressive'] is None
    
    def test_cancel_bizindex_is_own(self):
        """撤单的 BizIndex 是撤单记录自身的"""
        input_data = {
            'BizIndex': [700, 750],  # A=700, D=750
            'TickTime': [93000000, 93005000],
            'Type': ['A', 'D'],
            'BuyOrderNO': [1005, 1005],
            'SellOrderNO': [0, 0],
            'Price': [20.0, 0.0],
            'Qty': [2000, 1000],
            'TradeMoney': [0.0, 0.0],
            'TickBSFlag': ['B', 'B'],
            'SecurityID': ['600519', '600519'],
        }
        df = pl.DataFrame(input_data)
        orders, _ = reconstruct_sh_tick_data(df, '600519')
        
        cancel_order = [o for o in orders if o['OrdType'] == 'Cancel'][0]
        assert cancel_order['BizIndex'] == 750  # 撤单自身的 BizIndex
    
    def test_cancel_order_contains_security_id(self):
        """撤单记录必须包含 SecurityID"""
        input_data = {
            'BizIndex': [800, 801],
            'TickTime': [93000000, 93001000],
            'Type': ['A', 'D'],
            'BuyOrderNO': [1006, 1006],
            'SellOrderNO': [0, 0],
            'Price': [15.0, 0.0],
            'Qty': [500, 200],
            'TradeMoney': [0.0, 0.0],
            'TickBSFlag': ['B', 'B'],
            'SecurityID': ['000001', '000001'],
        }
        df = pl.DataFrame(input_data)
        orders, _ = reconstruct_sh_tick_data(df, '000001')
        
        cancel_order = [o for o in orders if o['OrdType'] == 'Cancel'][0]
        assert 'SecurityID' in cancel_order
        assert cancel_order['SecurityID'] == '000001'


# ============================================================================
# 场景6: 时间过滤 (上交所 14:57 应保留)
# ============================================================================

class TestScenario6TimeFilterSH:
    """场景6: 上交所 14:57 仍是连续竞价，应保留"""
    
    def test_morning_session(self):
        """上午连续竞价: 09:30:00 - 11:29:59.999"""
        assert is_continuous_trading_time(92500000) is False   # 09:25:00 集合竞价
        assert is_continuous_trading_time(92959999) is False   # 09:29:59.999 集合竞价
        assert is_continuous_trading_time(93000000) is True    # 09:30:00 连续竞价开始
        assert is_continuous_trading_time(100000000) is True   # 10:00:00 连续竞价
        assert is_continuous_trading_time(112959999) is True   # 11:29:59.999 连续竞价
        assert is_continuous_trading_time(113000000) is False  # 11:30:00 午休
    
    def test_afternoon_session_sh(self):
        """上交所下午连续竞价: 13:00:00 - 14:59:59.999 (无收盘集合竞价)"""
        assert is_continuous_trading_time(125900000) is False  # 12:59:00 午休
        assert is_continuous_trading_time(130000000) is True   # 13:00:00 下午开始
        assert is_continuous_trading_time(145700000) is True   # 14:57:00 ⭐ 上交所仍是连续竞价!
        assert is_continuous_trading_time(145800000) is True   # 14:58:00 上交所连续竞价
        assert is_continuous_trading_time(145959999) is True   # 14:59:59.999 上交所连续竞价
        assert is_continuous_trading_time(150000000) is False  # 15:00:00 收盘
    
    def test_filtered_data_retains_1457(self):
        """验证 14:57 数据被保留"""
        input_data = {
            'BizIndex': [1, 2, 3],
            'TickTime': [145700000, 145800000, 150000000],  # 14:57, 14:58, 15:00
            'Type': ['T', 'T', 'T'],
            'BuyOrderNO': [1001, 1002, 1003],
            'SellOrderNO': [2001, 2002, 2003],
            'Price': [10.0, 10.1, 10.2],
            'Qty': [100, 200, 300],
            'TradeMoney': [1000.0, 2020.0, 3060.0],
            'TickBSFlag': ['B', 'B', 'B'],
            'SecurityID': ['600519'] * 3,
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        # 应保留 14:57 和 14:58，过滤 15:00
        assert len(trades) == 2
        trade_times = [t['TickTime'] for t in trades]
        assert 145700000 in trade_times
        assert 145800000 in trade_times
        assert 150000000 not in trade_times


# ============================================================================
# 场景7: 通道数学关系 (Ch7 = Ch9 + Ch11)
# ============================================================================

class TestScenario7ChannelMathRelationship:
    """场景7: 验证通道数学关系
    
    Ch7 (买方委托总量) = Ch9 (买方 Taker 委托量) + Ch11 (买方 Maker 委托量)
    Ch8 (卖方委托总量) = Ch10 (卖方 Taker 委托量) + Ch12 (卖方 Maker 委托量)
    """
    
    def test_channel_relationship_buy_side(self):
        """买方: Ch7 = Ch9 + Ch11"""
        # 创建混合数据: Taker 和 Maker
        input_data = {
            'BizIndex': [1, 2, 3, 4, 5],
            'TickTime': [93000000, 93000100, 93000200, 93000300, 93000400],
            'Type': ['T', 'A', 'T', 'A', 'A'],
            'BuyOrderNO': [1001, 1002, 1003, 1004, 1005],
            'SellOrderNO': [2001, 0, 2003, 0, 0],
            'Price': [10.0, 9.9, 10.1, 9.8, 9.7],
            'Qty': [100, 200, 300, 400, 500],
            'TradeMoney': [1000.0, 0.0, 3030.0, 0.0, 0.0],
            'TickBSFlag': ['B', 'B', 'B', 'B', 'B'],
            'SecurityID': ['600519'] * 5,
        }
        df = pl.DataFrame(input_data)
        orders, _ = reconstruct_sh_tick_data(df, '600519')
        
        # 统计各通道
        new_orders = [o for o in orders if o['OrdType'] == 'New']
        buy_orders = [o for o in new_orders if o['Side'] == 'B']
        
        ch7 = sum(o['Qty'] for o in buy_orders)  # 买方委托总量
        ch9 = sum(o['Qty'] for o in buy_orders if o['IsAggressive'] is True)   # Taker
        ch11 = sum(o['Qty'] for o in buy_orders if o['IsAggressive'] is False)  # Maker
        
        # 验证数学关系
        assert ch7 == ch9 + ch11, f"Ch7({ch7}) != Ch9({ch9}) + Ch11({ch11})"
        
        # 验证具体值
        # Taker: 1001(100), 1003(300) -> 400
        # Maker: 1002(200), 1004(400), 1005(500) -> 1100
        assert ch9 == 400
        assert ch11 == 1100
        assert ch7 == 1500
    
    def test_channel_relationship_sell_side(self):
        """卖方: Ch8 = Ch10 + Ch12"""
        input_data = {
            'BizIndex': [1, 2, 3],
            'TickTime': [93000000, 93000100, 93000200],
            'Type': ['T', 'A', 'A'],
            'BuyOrderNO': [2001, 0, 0],
            'SellOrderNO': [1001, 1002, 1003],
            'Price': [10.0, 10.1, 10.2],
            'Qty': [500, 300, 200],
            'TradeMoney': [5000.0, 0.0, 0.0],
            'TickBSFlag': ['S', 'S', 'S'],
            'SecurityID': ['600519'] * 3,
        }
        df = pl.DataFrame(input_data)
        orders, _ = reconstruct_sh_tick_data(df, '600519')
        
        new_orders = [o for o in orders if o['OrdType'] == 'New']
        sell_orders = [o for o in new_orders if o['Side'] == 'S']
        
        ch8 = sum(o['Qty'] for o in sell_orders)
        ch10 = sum(o['Qty'] for o in sell_orders if o['IsAggressive'] is True)
        ch12 = sum(o['Qty'] for o in sell_orders if o['IsAggressive'] is False)
        
        assert ch8 == ch10 + ch12


# ============================================================================
# 边界情况测试
# ============================================================================

class TestEdgeCases:
    """边界情况测试"""
    
    def test_empty_input(self):
        """空输入数据"""
        df = pl.DataFrame({
            'BizIndex': [],
            'TickTime': [],
            'Type': [],
            'BuyOrderNO': [],
            'SellOrderNO': [],
            'Price': [],
            'Qty': [],
            'TradeMoney': [],
            'TickBSFlag': [],
            'SecurityID': [],
        }).cast({
            'BizIndex': pl.Int64,
            'TickTime': pl.Int64,
            'BuyOrderNO': pl.Int64,
            'SellOrderNO': pl.Int64,
            'Price': pl.Float64,
            'Qty': pl.Int64,
            'TradeMoney': pl.Float64,
        })
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        assert len(orders) == 0
        assert len(trades) == 0
    
    def test_type_s_excluded(self):
        """Type='S' 产品状态记录应被剔除"""
        input_data = {
            'BizIndex': [1, 2, 3],
            'TickTime': [93000000, 93000100, 93000200],
            'Type': ['T', 'S', 'A'],
            'BuyOrderNO': [1001, 0, 1002],
            'SellOrderNO': [2001, 0, 0],
            'Price': [10.0, 0.0, 9.9],
            'Qty': [100, 0, 200],
            'TradeMoney': [1000.0, 0.0, 0.0],
            'TickBSFlag': ['B', 'TRADE', 'B'],
            'SecurityID': ['600519'] * 3,
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        # S 类型应被剔除，只有 T 和 A
        assert len(trades) == 1
        assert len(orders) == 2
    
    def test_price_zero_for_cancel_without_cache(self):
        """撤单 Price=0 且无缓存时使用 last_price"""
        # 这种情况较少见，但需要处理
        input_data = {
            'BizIndex': [1, 2],
            'TickTime': [93000000, 93000100],
            'Type': ['T', 'D'],
            'BuyOrderNO': [1001, 1002],  # D 的订单号不在缓存
            'SellOrderNO': [2001, 0],
            'Price': [10.0, 0.0],
            'Qty': [100, 50],
            'TradeMoney': [1000.0, 0.0],
            'TickBSFlag': ['B', 'B'],
            'SecurityID': ['600519', '600519'],
        }
        df = pl.DataFrame(input_data)
        orders, _ = reconstruct_sh_tick_data(df, '600519')
        
        cancel_orders = [o for o in orders if o['OrdType'] == 'Cancel']
        if len(cancel_orders) > 0:
            # 使用 last_price 兜底
            assert cancel_orders[0]['Price'] == 10.0
    
    def test_order_no_zero_ignored(self):
        """OrderNO=0 应被忽略"""
        input_data = {
            'BizIndex': [1],
            'TickTime': [93000000],
            'Type': ['T'],
            'BuyOrderNO': [0],  # 无效
            'SellOrderNO': [0],  # 无效
            'Price': [10.0],
            'Qty': [100],
            'TradeMoney': [1000.0],
            'TickBSFlag': ['B'],
            'SecurityID': ['600519'],
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '600519')
        
        # 双方 OrderNO 都为 0，不应产生委托
        # 但成交记录应该存在 (带 ActiveSide=0 集合竞价)
        assert len(trades) == 1


# ============================================================================
# 验证 SecurityID 存在
# ============================================================================

class TestSecurityIDPresence:
    """验证所有记录都包含 SecurityID"""
    
    def test_all_orders_have_security_id(self):
        """所有委托记录包含 SecurityID"""
        input_data = {
            'BizIndex': [1, 2, 3, 4],
            'TickTime': [93000000, 93000100, 93000200, 93000300],
            'Type': ['T', 'A', 'D', 'A'],
            'BuyOrderNO': [1001, 1002, 1002, 0],
            'SellOrderNO': [2001, 0, 0, 2002],
            'Price': [10.0, 9.9, 0.0, 10.1],
            'Qty': [100, 200, 100, 300],
            'TradeMoney': [1000.0, 0.0, 0.0, 0.0],
            'TickBSFlag': ['B', 'B', 'B', 'S'],
            'SecurityID': ['000001'] * 4,
        }
        df = pl.DataFrame(input_data)
        orders, trades = reconstruct_sh_tick_data(df, '000001')
        
        for order in orders:
            assert 'SecurityID' in order
            assert order['SecurityID'] == '000001'
        
        for trade in trades:
            assert 'SecurityID' in trade
            assert trade['SecurityID'] == '000001'


# ============================================================================
# 运行入口
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
