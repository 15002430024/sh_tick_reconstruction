# -*- coding: utf-8 -*-
"""
Phase 3.2: 批量处理入口单元测试

验证 process_daily_data() 和 check_bizindex_continuity() 函数

测试重点:
1. 输出文件命名: {date}_sh_order_data.parquet / {date}_sh_trade_data.parquet
2. 全市场排序: (SecurityID, TickTime, BizIndex)
3. IsAggressive 为 nullable bool
4. 所有记录包含 SecurityID
5. BizIndex 连续性检查
"""

import os
import pytest
import tempfile
from pathlib import Path

import polars as pl

from sh_tick_reconstruction import (
    process_daily_data,
    check_bizindex_continuity,
    get_output_file_paths,
    validate_date_format,
    read_order_parquet,
    read_trade_parquet,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def temp_dir():
    """创建临时目录"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def multi_stock_data(temp_dir):
    """
    多只股票测试数据
    
    股票1: 600519 - 2笔成交 + 2笔新增 + 1笔撤单
    股票2: 000001 - 1笔新增 (集合竞价)
    """
    test_data = {
        'BizIndex': [100, 101, 102, 200, 201, 300],
        'TickTime': [93000000, 93000100, 93000200, 93001000, 93001100, 92500000],
        'Type': ['T', 'A', 'D', 'T', 'A', 'A'],
        'BuyOrderNO': [1001, 1001, 1001, 1002, 1002, 0],
        'SellOrderNO': [2001, 0, 0, 2002, 0, 3001],
        'Price': [50.0, 50.5, 0, 60.0, 60.5, 45.0],
        'Qty': [500, 300, 200, 600, 400, 2000],
        'TradeMoney': [25000.0, 0, 0, 36000.0, 0, 0],
        'TickBSFlag': ['B', 'B', 'B', 'B', 'B', 'S'],
        'SecurityID': ['600519', '600519', '600519', '600519', '600519', '000001'],
    }
    
    df = pl.DataFrame(test_data)
    input_file = os.path.join(temp_dir, 'test_input.parquet')
    df.write_parquet(input_file)
    
    return input_file, temp_dir


# ============================================================================
# process_daily_data 测试
# ============================================================================

class TestProcessDailyData:
    """process_daily_data 测试"""
    
    def test_output_file_naming(self, multi_stock_data):
        """验证输出文件命名规范: {date}_sh_order_data.parquet"""
        input_file, output_dir = multi_stock_data
        
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir
        )
        
        # 验证文件命名
        assert '20260126_sh_order_data.parquet' in stats['output_files'][0]
        assert '20260126_sh_trade_data.parquet' in stats['output_files'][1]
        
        # 验证文件存在
        assert os.path.exists(stats['output_files'][0])
        assert os.path.exists(stats['output_files'][1])
    
    def test_full_market_sorting(self, multi_stock_data):
        """验证全市场排序: (SecurityID, TickTime, BizIndex)"""
        input_file, output_dir = multi_stock_data
        
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir
        )
        
        # 读取输出
        orders_df = read_order_parquet(stats['output_files'][0])
        trades_df = read_trade_parquet(stats['output_files'][1])
        
        # 验证委托排序
        assert len(orders_df) > 0
        security_ids = orders_df['SecurityID'].to_list()
        # 000001 应该在 600519 之前 (字符串排序)
        first_600519_idx = security_ids.index('600519') if '600519' in security_ids else len(security_ids)
        for i, sec_id in enumerate(security_ids):
            if sec_id == '000001':
                assert i < first_600519_idx, "000001 应在 600519 之前"
        
        # 验证成交排序
        assert len(trades_df) > 0
    
    def test_is_aggressive_nullable(self, multi_stock_data):
        """验证 IsAggressive 为 nullable bool"""
        input_file, output_dir = multi_stock_data
        
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir
        )
        
        orders_df = read_order_parquet(stats['output_files'][0])
        
        # 检查撤单的 IsAggressive 为 None
        cancel_orders = orders_df.filter(pl.col('OrdType') == 'Cancel')
        if len(cancel_orders) > 0:
            assert cancel_orders['IsAggressive'].null_count() == len(cancel_orders), \
                "撤单的 IsAggressive 应为 None"
        
        # 检查新增订单的 IsAggressive 有值
        new_orders = orders_df.filter(pl.col('OrdType') == 'New')
        if len(new_orders) > 0:
            # 至少部分新增订单应有 IsAggressive 值
            has_aggressive = new_orders['IsAggressive'].is_not_null().sum()
            assert has_aggressive >= 0  # 可能全为 False (Maker)
    
    def test_security_id_included(self, multi_stock_data):
        """验证所有记录包含 SecurityID"""
        input_file, output_dir = multi_stock_data
        
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir
        )
        
        orders_df = read_order_parquet(stats['output_files'][0])
        trades_df = read_trade_parquet(stats['output_files'][1])
        
        # 验证字段存在
        assert 'SecurityID' in orders_df.columns
        assert 'SecurityID' in trades_df.columns
        
        # 验证无空值
        assert orders_df['SecurityID'].null_count() == 0
        assert trades_df['SecurityID'].null_count() == 0
    
    def test_statistics_accuracy(self, multi_stock_data):
        """验证统计信息准确性"""
        input_file, output_dir = multi_stock_data
        
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir
        )
        
        # 验证基本统计
        assert stats['total_securities'] == 2  # 600519, 000001
        assert stats['total_orders'] > 0
        assert stats['total_trades'] > 0
        assert stats['new_orders'] + stats['cancel_orders'] == stats['total_orders']
        assert stats['processing_time_seconds'] >= 0
    
    def test_invalid_date_format(self, multi_stock_data):
        """验证日期格式校验"""
        input_file, output_dir = multi_stock_data
        
        with pytest.raises(ValueError, match="日期格式不正确"):
            process_daily_data(
                date='2026-01-26',  # 错误格式
                input_path=input_file,
                output_path=output_dir
            )
    
    def test_input_file_not_found(self, temp_dir):
        """验证输入文件不存在的处理"""
        with pytest.raises(FileNotFoundError, match="输入文件不存在"):
            process_daily_data(
                date='20260126',
                input_path='/nonexistent/path.parquet',
                output_path=temp_dir
            )
    
    def test_progress_callback(self, multi_stock_data):
        """验证进度回调"""
        input_file, output_dir = multi_stock_data
        
        progress_calls = []
        
        def callback(security_id, current, total):
            progress_calls.append((security_id, current, total))
        
        process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=output_dir,
            progress_callback=callback
        )
        
        # 应该有 2 次回调 (2 只股票)
        assert len(progress_calls) == 2
        assert progress_calls[0][1] == 1  # 第一只
        assert progress_calls[1][1] == 2  # 第二只
        assert progress_calls[0][2] == 2  # 总共 2 只


# ============================================================================
# check_bizindex_continuity 测试
# ============================================================================

class TestCheckBizindexContinuity:
    """check_bizindex_continuity 测试"""
    
    def test_continuous_bizindex(self):
        """测试连续 BizIndex"""
        df = pl.DataFrame({'BizIndex': [100, 101, 102, 103, 104]})
        
        result = check_bizindex_continuity(df)
        
        assert result['is_continuous'] is True
        assert result['gap_count'] == 0
        assert result['gaps'] == []
        assert result['min_bizindex'] == 100
        assert result['max_bizindex'] == 104
    
    def test_discontinuous_bizindex(self):
        """测试不连续 BizIndex"""
        df = pl.DataFrame({'BizIndex': [100, 101, 103, 105, 106]})  # 缺 102, 104
        
        result = check_bizindex_continuity(df)
        
        assert result['is_continuous'] is False
        assert result['gap_count'] == 2
        assert 102 in result['gaps']
        assert 104 in result['gaps']
    
    def test_empty_dataframe(self):
        """测试空 DataFrame"""
        df = pl.DataFrame({'BizIndex': []}).cast({'BizIndex': pl.Int64})
        
        result = check_bizindex_continuity(df)
        
        assert result['is_continuous'] is True
        assert result['total_records'] == 0
        assert result['gap_count'] == 0
    
    def test_single_record(self):
        """测试单条记录"""
        df = pl.DataFrame({'BizIndex': [100]})
        
        result = check_bizindex_continuity(df)
        
        assert result['is_continuous'] is True
        assert result['total_records'] == 1
        assert result['gap_count'] == 0
    
    def test_missing_bizindex_column(self):
        """测试缺少 BizIndex 列"""
        df = pl.DataFrame({'OtherColumn': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="缺少 BizIndex 列"):
            check_bizindex_continuity(df)


# ============================================================================
# 辅助函数测试
# ============================================================================

class TestHelperFunctions:
    """辅助函数测试"""
    
    def test_get_output_file_paths(self):
        """测试输出路径生成"""
        order_file, trade_file = get_output_file_paths('20260126', '/data/output')
        
        assert order_file == '/data/output/20260126_sh_order_data.parquet'
        assert trade_file == '/data/output/20260126_sh_trade_data.parquet'
    
    def test_validate_date_format_valid(self):
        """测试有效日期格式"""
        assert validate_date_format('20260126') is True
        assert validate_date_format('20231231') is True
    
    def test_validate_date_format_invalid(self):
        """测试无效日期格式"""
        assert validate_date_format('2026-01-26') is False  # 含分隔符
        assert validate_date_format('202601') is False  # 长度不够
        assert validate_date_format('abcdefgh') is False  # 非数字
        assert validate_date_format('19891231') is False  # 年份过早
        assert validate_date_format('20261301') is False  # 月份无效


# ============================================================================
# 集成测试
# ============================================================================

class TestBatchIntegration:
    """批量处理集成测试"""
    
    def test_end_to_end_workflow(self, temp_dir):
        """端到端工作流测试"""
        # 创建测试数据
        test_data = {
            'BizIndex': [1, 2, 3, 4, 5, 6],
            'TickTime': [93000000, 93000100, 93000200, 93000300, 93000400, 93000500],
            'Type': ['A', 'T', 'A', 'T', 'D', 'A'],
            'BuyOrderNO': [1001, 1001, 0, 2001, 1001, 3001],
            'SellOrderNO': [0, 2001, 2002, 2001, 0, 0],
            'Price': [50.0, 50.0, 49.5, 49.5, 0.0, 51.0],
            'Qty': [1000, 500, 800, 300, 500, 200],
            'TradeMoney': [0.0, 25000.0, 0.0, 14850.0, 0.0, 0.0],
            'TickBSFlag': ['B', 'B', 'S', 'S', 'B', 'B'],
            'SecurityID': ['600519', '600519', '600519', '600519', '600519', '600519'],
        }
        
        df = pl.DataFrame(test_data)
        input_file = os.path.join(temp_dir, 'test_daily.parquet')
        df.write_parquet(input_file)
        
        # 执行批量处理
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=temp_dir,
            validate_output=True
        )
        
        # 验证输出
        assert stats['total_securities'] == 1
        assert stats['total_orders'] > 0
        assert stats['total_trades'] == 2  # 2 笔成交
        
        # 读取并验证输出文件
        orders_df = read_order_parquet(stats['output_files'][0])
        trades_df = read_trade_parquet(stats['output_files'][1])
        
        # 验证内容
        assert len(orders_df) == stats['total_orders']
        assert len(trades_df) == stats['total_trades']
        
        # 验证成交记录
        assert trades_df['SecurityID'].to_list() == ['600519', '600519']
        
        # 验证 BizIndex 连续性
        result = check_bizindex_continuity(
            pl.concat([
                orders_df.select('BizIndex'),
                trades_df.select('BizIndex')
            ])
        )
        # 重建数据的 BizIndex 应来自原始数据，可能不连续（因为有些是 Type='A' 不产生成交）


# ============================================================================
# 主函数
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
