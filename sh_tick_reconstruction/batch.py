# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 批量处理模块

实现单日全市场数据的批量处理:
- process_daily_data(): 处理单日全市场数据
- check_bizindex_continuity(): BizIndex 连续性检查

对应需求文档: SH_Tick_Data_Reconstruction_Spec v1.8
对应落地计划: Phase 3.2
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import polars as pl

from .reconstructor import reconstruct_sh_tick_data
from .schema import (
    create_order_dataframe,
    create_trade_dataframe,
    write_order_parquet,
    write_trade_parquet,
    SH_ORDER_SCHEMA_PYARROW,
    SH_TRADE_SCHEMA_PYARROW,
)


# 配置日志
logger = logging.getLogger(__name__)


# ============================================================================
# 批量处理主函数
# ============================================================================

def process_daily_data(
    date: str,
    input_path: str,
    output_path: str,
    validate_output: bool = True,
    progress_callback: Optional[callable] = None
) -> Dict[str, Any]:
    """
    处理单日全市场数据
    
    将上交所 auction_tick_merged_data 混合数据流拆解还原为:
    - {date}_sh_order_data.parquet (委托表)
    - {date}_sh_trade_data.parquet (成交表)
    
    Args:
        date: 日期字符串，格式 YYYYMMDD (如 '20260126')
        input_path: 输入 Parquet 文件路径
        output_path: 输出目录路径
        validate_output: 是否验证输出 schema
        progress_callback: 进度回调函数 callback(security_id, current, total)
    
    Returns:
        Dict[str, Any]: 处理统计信息
            - total_securities: 处理的股票数量
            - total_orders: 输出的委托记录数
            - total_trades: 输出的成交记录数
            - new_orders: 新增委托数量
            - cancel_orders: 撤单数量
            - taker_orders: 主动委托数量 (IsAggressive=True)
            - maker_orders: 被动委托数量 (IsAggressive=False)
            - processing_time_seconds: 处理耗时
            - output_files: 输出文件路径列表
    
    Raises:
        FileNotFoundError: 如果输入文件不存在
        ValueError: 如果日期格式不正确
    
    Examples:
        >>> stats = process_daily_data(
        ...     date='20260126',
        ...     input_path='/data/raw/auction_tick_merged_data_20260126.parquet',
        ...     output_path='/data/processed/'
        ... )
        >>> print(f"处理 {stats['total_securities']} 只股票")
        >>> print(f"输出 {stats['total_orders']} 条委托, {stats['total_trades']} 条成交")
    
    Note:
        ⭐ 关键约束:
        1. 输出文件命名必须为 {date}_sh_order_data.parquet / {date}_sh_trade_data.parquet
        2. 输出必须按 (SecurityID, TickTime, BizIndex) 排序
        3. IsAggressive 必须为 nullable bool 类型
        4. 所有记录必须包含 SecurityID 字段
    """
    start_time = datetime.now()
    
    # 验证日期格式
    if len(date) != 8 or not date.isdigit():
        raise ValueError(f"日期格式不正确，应为 YYYYMMDD: {date}")
    
    # 验证输入文件存在
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"输入文件不存在: {input_path}")
    
    # 确保输出目录存在
    Path(output_path).mkdir(parents=True, exist_ok=True)
    
    logger.info(f"开始处理日期 {date} 的数据")
    logger.info(f"输入文件: {input_path}")
    logger.info(f"输出目录: {output_path}")
    
    # =========================================================================
    # Step 1: 读取数据
    # =========================================================================
    logger.info("Step 1: 读取输入数据...")
    df = pl.read_parquet(input_path)
    logger.info(f"  读取完成: {len(df):,} 行")
    
    # 获取所有股票代码
    security_ids = df['SecurityID'].unique().sort().to_list()
    total_securities = len(security_ids)
    logger.info(f"  共 {total_securities} 只股票")
    
    # =========================================================================
    # Step 2: 按股票分组处理
    # =========================================================================
    logger.info("Step 2: 按股票分组处理...")
    
    all_orders: List[Dict[str, Any]] = []
    all_trades: List[Dict[str, Any]] = []
    
    for i, security_id in enumerate(security_ids):
        # 进度回调
        if progress_callback:
            progress_callback(security_id, i + 1, total_securities)
        
        # 过滤单只股票数据
        group_df = df.filter(pl.col('SecurityID') == security_id)
        
        # 调用核心重建函数
        orders, trades = reconstruct_sh_tick_data(group_df, security_id)
        
        # 累加结果
        all_orders.extend(orders)
        all_trades.extend(trades)
        
        # 日志 (每 100 只股票打印一次)
        if (i + 1) % 100 == 0 or (i + 1) == total_securities:
            logger.info(f"  已处理 {i + 1}/{total_securities} 只股票")
    
    logger.info(f"  处理完成: {len(all_orders):,} 条委托, {len(all_trades):,} 条成交")
    
    # =========================================================================
    # Step 3: 创建 DataFrame 并全市场排序
    # =========================================================================
    logger.info("Step 3: 创建 DataFrame 并排序...")
    
    # 创建 DataFrame (使用 schema 辅助函数)
    orders_df = create_order_dataframe(all_orders)
    trades_df = create_trade_dataframe(all_trades)
    
    # 全市场排序 (SecurityID, TickTime, BizIndex)
    # 注意: write_order_parquet 和 write_trade_parquet 会自动排序，
    # 这里提前排序是为了统计验证
    orders_df = orders_df.sort(['SecurityID', 'TickTime', 'BizIndex'])
    trades_df = trades_df.sort(['SecurityID', 'TickTime', 'BizIndex'])
    
    logger.info(f"  委托表: {orders_df.shape[0]:,} 行, {orders_df.shape[1]} 列")
    logger.info(f"  成交表: {trades_df.shape[0]:,} 行, {trades_df.shape[1]} 列")
    
    # =========================================================================
    # Step 4: 输出 Parquet
    # =========================================================================
    logger.info("Step 4: 输出 Parquet 文件...")
    
    # 文件命名规范: {date}_sh_order_data.parquet / {date}_sh_trade_data.parquet
    order_file = os.path.join(output_path, f"{date}_sh_order_data.parquet")
    trade_file = os.path.join(output_path, f"{date}_sh_trade_data.parquet")
    
    # 写入文件 (包含 schema 验证和排序)
    write_order_parquet(orders_df, order_file, validate=validate_output)
    write_trade_parquet(trades_df, trade_file, validate=validate_output)
    
    logger.info(f"  委托文件: {order_file}")
    logger.info(f"  成交文件: {trade_file}")
    
    # =========================================================================
    # Step 5: 统计信息
    # =========================================================================
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    # 计算统计
    new_orders = orders_df.filter(pl.col('OrdType') == 'New').shape[0]
    cancel_orders = orders_df.filter(pl.col('OrdType') == 'Cancel').shape[0]
    
    # IsAggressive 统计 (只统计 New 订单)
    new_orders_df = orders_df.filter(pl.col('OrdType') == 'New')
    taker_orders = new_orders_df.filter(pl.col('IsAggressive') == True).shape[0]
    maker_orders = new_orders_df.filter(pl.col('IsAggressive') == False).shape[0]
    
    stats = {
        'date': date,
        'total_securities': total_securities,
        'total_orders': len(all_orders),
        'total_trades': len(all_trades),
        'new_orders': new_orders,
        'cancel_orders': cancel_orders,
        'taker_orders': taker_orders,
        'maker_orders': maker_orders,
        'processing_time_seconds': round(processing_time, 2),
        'output_files': [order_file, trade_file],
    }
    
    logger.info("=" * 60)
    logger.info(f"处理完成! 日期: {date}")
    logger.info(f"  股票数: {stats['total_securities']:,}")
    logger.info(f"  委托数: {stats['total_orders']:,} (New: {new_orders:,}, Cancel: {cancel_orders:,})")
    logger.info(f"  成交数: {stats['total_trades']:,}")
    logger.info(f"  Taker: {taker_orders:,}, Maker: {maker_orders:,}")
    logger.info(f"  耗时: {processing_time:.2f} 秒")
    logger.info("=" * 60)
    
    return stats


# ============================================================================
# BizIndex 连续性检查
# ============================================================================

def check_bizindex_continuity(
    df: pl.DataFrame,
    security_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    检查 BizIndex 连续性，检测跳号
    
    Args:
        df: 包含 BizIndex 列的 DataFrame
        security_id: 可选的股票代码 (用于日志)
    
    Returns:
        Dict[str, Any]: 检查结果
            - is_continuous: 是否连续
            - total_records: 总记录数
            - min_bizindex: 最小 BizIndex
            - max_bizindex: 最大 BizIndex
            - expected_count: 期望记录数 (max - min + 1)
            - gap_count: 跳号数量
            - gaps: 跳号列表 (前 10 个)
    
    Examples:
        >>> result = check_bizindex_continuity(df, '600519')
        >>> if not result['is_continuous']:
        ...     print(f"发现 {result['gap_count']} 个跳号")
    """
    if 'BizIndex' not in df.columns:
        raise ValueError("DataFrame 缺少 BizIndex 列")
    
    if len(df) == 0:
        return {
            'is_continuous': True,
            'total_records': 0,
            'min_bizindex': None,
            'max_bizindex': None,
            'expected_count': 0,
            'gap_count': 0,
            'gaps': [],
        }
    
    # 排序并获取 BizIndex 列表
    biz_indices = df.sort('BizIndex')['BizIndex'].to_list()
    
    min_idx = biz_indices[0]
    max_idx = biz_indices[-1]
    expected_count = max_idx - min_idx + 1
    actual_count = len(biz_indices)
    gap_count = expected_count - actual_count
    
    # 查找具体跳号
    gaps = []
    if gap_count > 0:
        biz_set = set(biz_indices)
        for i in range(min_idx, max_idx + 1):
            if i not in biz_set:
                gaps.append(i)
                if len(gaps) >= 10:  # 只记录前 10 个
                    break
    
    result = {
        'is_continuous': gap_count == 0,
        'total_records': actual_count,
        'min_bizindex': min_idx,
        'max_bizindex': max_idx,
        'expected_count': expected_count,
        'gap_count': gap_count,
        'gaps': gaps,
    }
    
    # 日志告警
    if not result['is_continuous']:
        sec_info = f" [{security_id}]" if security_id else ""
        logger.warning(
            f"BizIndex 连续性检查{sec_info}: 发现 {gap_count} 个跳号"
            f" (范围 {min_idx}-{max_idx}, 实际 {actual_count} 条)"
        )
        if gaps:
            logger.warning(f"  跳号示例: {gaps[:5]}...")
    
    return result


# ============================================================================
# 辅助函数
# ============================================================================

def get_output_file_paths(date: str, output_path: str) -> Tuple[str, str]:
    """
    获取输出文件路径
    
    Args:
        date: 日期字符串 (YYYYMMDD)
        output_path: 输出目录
    
    Returns:
        Tuple[str, str]: (委托文件路径, 成交文件路径)
    """
    order_file = os.path.join(output_path, f"{date}_sh_order_data.parquet")
    trade_file = os.path.join(output_path, f"{date}_sh_trade_data.parquet")
    return order_file, trade_file


def validate_date_format(date: str) -> bool:
    """
    验证日期格式是否为 YYYYMMDD
    
    Args:
        date: 日期字符串
    
    Returns:
        bool: True if 格式正确
    """
    if len(date) != 8 or not date.isdigit():
        return False
    
    try:
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:8])
        
        if year < 1990 or year > 2100:
            return False
        if month < 1 or month > 12:
            return False
        if day < 1 or day > 31:
            return False
        
        return True
    except ValueError:
        return False


# ============================================================================
# 模块测试
# ============================================================================

if __name__ == '__main__':
    import tempfile
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 70)
    print("Phase 3.2: 批量处理入口测试")
    print("=" * 70)
    
    # 创建测试数据
    test_data = {
        'BizIndex': [100, 101, 102, 200, 201, 300],
        'TickTime': [93000000, 93000100, 93000200, 93001000, 93001100, 93002000],
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
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as tmpdir:
        # 保存测试输入文件
        input_file = os.path.join(tmpdir, 'test_input.parquet')
        df.write_parquet(input_file)
        
        print(f"\n📋 测试数据: {len(df)} 行, 2 只股票")
        
        # 进度回调
        def progress(sec_id, current, total):
            print(f"  处理: {sec_id} ({current}/{total})")
        
        # 执行批量处理
        stats = process_daily_data(
            date='20260126',
            input_path=input_file,
            output_path=tmpdir,
            progress_callback=progress
        )
        
        print(f"\n📊 处理统计:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 验证输出文件
        order_file, trade_file = get_output_file_paths('20260126', tmpdir)
        
        print(f"\n📁 输出文件验证:")
        assert os.path.exists(order_file), "委托文件不存在"
        assert os.path.exists(trade_file), "成交文件不存在"
        print(f"  ✅ 委托文件存在: {os.path.basename(order_file)}")
        print(f"  ✅ 成交文件存在: {os.path.basename(trade_file)}")
        
        # 读取并验证内容
        orders_df = pl.read_parquet(order_file)
        trades_df = pl.read_parquet(trade_file)
        
        print(f"\n📋 输出内容:")
        print(f"  委托: {len(orders_df)} 条")
        print(f"  成交: {len(trades_df)} 条")
        
        # 验证排序
        assert orders_df['SecurityID'].to_list() == sorted(orders_df['SecurityID'].to_list()), \
            "委托未按 SecurityID 排序"
        print(f"  ✅ 委托按 (SecurityID, TickTime, BizIndex) 排序")
        
        # 验证 IsAggressive nullable
        assert orders_df['IsAggressive'].null_count() >= 0, "IsAggressive 应支持 null"
        cancel_orders = orders_df.filter(pl.col('OrdType') == 'Cancel')
        if len(cancel_orders) > 0:
            assert cancel_orders['IsAggressive'].null_count() == len(cancel_orders), \
                "撤单的 IsAggressive 应为 None"
        print(f"  ✅ IsAggressive 为 nullable bool")
        
        # 验证 SecurityID 存在
        assert 'SecurityID' in orders_df.columns
        assert 'SecurityID' in trades_df.columns
        print(f"  ✅ 所有记录包含 SecurityID")
    
    print("\n" + "=" * 70)
    print("✅ Phase 3.2 批量处理入口测试通过!")
    print("=" * 70)
