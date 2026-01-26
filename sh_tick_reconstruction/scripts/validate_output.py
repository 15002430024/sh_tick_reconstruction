# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 输出验证脚本

验证内容:
1. Schema 验证 (字段类型、nullable bool)
2. 排序验证 (SecurityID, TickTime, BizIndex)
3. 通道数学关系验证 (Ch7=Ch9+Ch11, Ch8=Ch10+Ch12)
4. SecurityID 存在性验证
5. IsAggressive 分布统计

用法:
    python -m sh_tick_reconstruction.scripts.validate_output --date 20260126 --path /path/to/output

对应需求文档: SH_Tick_Data_Reconstruction_Spec v1.8
对应落地计划: Phase 4
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List

import polars as pl

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sh_tick_reconstruction import (
    validate_order_schema,
    validate_trade_schema,
    read_order_parquet,
    read_trade_parquet,
)


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_sorting(df: pl.DataFrame, columns: List[str], name: str) -> bool:
    """验证 DataFrame 是否按指定列排序"""
    if df.height == 0:
        logger.warning(f"{name}: 数据为空，跳过排序验证")
        return True
    
    sorted_df = df.sort(columns)
    is_sorted = df.equals(sorted_df)
    
    if is_sorted:
        logger.info(f"✅ {name}: 排序正确 ({', '.join(columns)})")
    else:
        logger.error(f"❌ {name}: 排序错误，应按 ({', '.join(columns)}) 排序")
    
    return is_sorted


def verify_channel_math(orders_df: pl.DataFrame) -> bool:
    """
    验证通道数学关系: Ch7 = Ch9 + Ch11, Ch8 = Ch10 + Ch12
    
    通道映射:
    - Ch7: 全部买单 (Side='B', OrdType='New')
    - Ch8: 全部卖单 (Side='S', OrdType='New')
    - Ch9: 主动买入委托 (Side='B', IsAggressive=True)
    - Ch10: 主动卖出委托 (Side='S', IsAggressive=True)
    - Ch11: 非主动买入委托 (Side='B', IsAggressive=False)
    - Ch12: 非主动卖出委托 (Side='S', IsAggressive=False)
    """
    if orders_df.height == 0:
        logger.warning("委托表为空，跳过通道数学验证")
        return True
    
    # 只统计 New 类型的委托
    new_orders = orders_df.filter(pl.col('OrdType') == 'New')
    
    # 按 Side 和 IsAggressive 分组统计
    ch7 = new_orders.filter(pl.col('Side') == 'B').height  # 全部买单
    ch8 = new_orders.filter(pl.col('Side') == 'S').height  # 全部卖单
    
    ch9 = new_orders.filter(
        (pl.col('Side') == 'B') & (pl.col('IsAggressive') == True)
    ).height  # 主动买
    
    ch10 = new_orders.filter(
        (pl.col('Side') == 'S') & (pl.col('IsAggressive') == True)
    ).height  # 主动卖
    
    ch11 = new_orders.filter(
        (pl.col('Side') == 'B') & (pl.col('IsAggressive') == False)
    ).height  # 非主动买
    
    ch12 = new_orders.filter(
        (pl.col('Side') == 'S') & (pl.col('IsAggressive') == False)
    ).height  # 非主动卖
    
    # 验证数学关系
    buy_match = (ch7 == ch9 + ch11)
    sell_match = (ch8 == ch10 + ch12)
    
    logger.info(f"通道统计:")
    logger.info(f"  Ch7 (全部买单): {ch7}")
    logger.info(f"  Ch8 (全部卖单): {ch8}")
    logger.info(f"  Ch9 (主动买): {ch9}")
    logger.info(f"  Ch10 (主动卖): {ch10}")
    logger.info(f"  Ch11 (非主动买): {ch11}")
    logger.info(f"  Ch12 (非主动卖): {ch12}")
    
    if buy_match:
        logger.info(f"✅ Ch7 = Ch9 + Ch11: {ch7} = {ch9} + {ch11}")
    else:
        logger.error(f"❌ Ch7 ≠ Ch9 + Ch11: {ch7} ≠ {ch9} + {ch11}")
    
    if sell_match:
        logger.info(f"✅ Ch8 = Ch10 + Ch12: {ch8} = {ch10} + {ch12}")
    else:
        logger.error(f"❌ Ch8 ≠ Ch10 + Ch12: {ch8} ≠ {ch10} + {ch12}")
    
    return buy_match and sell_match


def verify_security_id(df: pl.DataFrame, name: str) -> bool:
    """验证 SecurityID 字段存在且非空"""
    if 'SecurityID' not in df.columns:
        logger.error(f"❌ {name}: 缺少 SecurityID 字段")
        return False
    
    null_count = df.filter(pl.col('SecurityID').is_null()).height
    if null_count > 0:
        logger.error(f"❌ {name}: SecurityID 存在 {null_count} 个空值")
        return False
    
    logger.info(f"✅ {name}: SecurityID 字段验证通过")
    return True


def verify_is_aggressive(orders_df: pl.DataFrame) -> bool:
    """验证 IsAggressive 字段的分布和类型"""
    if orders_df.height == 0:
        logger.warning("委托表为空，跳过 IsAggressive 验证")
        return True
    
    # 统计分布
    new_orders = orders_df.filter(pl.col('OrdType') == 'New')
    cancel_orders = orders_df.filter(pl.col('OrdType') == 'Cancel')
    
    true_count = new_orders.filter(pl.col('IsAggressive') == True).height
    false_count = new_orders.filter(pl.col('IsAggressive') == False).height
    none_count = orders_df.filter(pl.col('IsAggressive').is_null()).height
    
    logger.info(f"IsAggressive 分布:")
    logger.info(f"  True (Taker): {true_count}")
    logger.info(f"  False (Maker): {false_count}")
    logger.info(f"  None (撤单): {none_count}")
    
    # 验证撤单的 IsAggressive 应该是 None
    cancel_not_none = cancel_orders.filter(pl.col('IsAggressive').is_not_null()).height
    if cancel_not_none > 0:
        logger.error(f"❌ 撤单记录中有 {cancel_not_none} 条 IsAggressive 不是 None")
        return False
    
    # 验证 New 订单的 IsAggressive 不应该是 None
    new_is_none = new_orders.filter(pl.col('IsAggressive').is_null()).height
    if new_is_none > 0:
        logger.error(f"❌ New 订单中有 {new_is_none} 条 IsAggressive 是 None")
        return False
    
    logger.info(f"✅ IsAggressive 字段验证通过")
    return True


def validate_output(date: str, path: str) -> Dict[str, Any]:
    """
    验证输出数据
    
    Args:
        date: 日期字符串 YYYYMMDD
        path: 输出目录路径
    
    Returns:
        验证结果字典
    """
    results = {
        'date': date,
        'path': path,
        'passed': True,
        'errors': [],
        'warnings': []
    }
    
    output_path = Path(path)
    order_file = output_path / f"{date}_sh_order_data.parquet"
    trade_file = output_path / f"{date}_sh_trade_data.parquet"
    
    # 验证文件存在
    if not order_file.exists():
        results['errors'].append(f"委托文件不存在: {order_file}")
        results['passed'] = False
    
    if not trade_file.exists():
        results['errors'].append(f"成交文件不存在: {trade_file}")
        results['passed'] = False
    
    if not results['passed']:
        return results
    
    # 读取数据
    logger.info(f"读取委托数据: {order_file}")
    orders_df = read_order_parquet(str(order_file))
    
    logger.info(f"读取成交数据: {trade_file}")
    trades_df = read_trade_parquet(str(trade_file))
    
    logger.info(f"委托记录数: {orders_df.height}")
    logger.info(f"成交记录数: {trades_df.height}")
    
    # 1. Schema 验证
    logger.info("\n--- Schema 验证 ---")
    if not validate_order_schema(orders_df):
        results['errors'].append("委托表 Schema 验证失败")
        results['passed'] = False
    else:
        logger.info("✅ 委托表 Schema 验证通过")
    
    if not validate_trade_schema(trades_df):
        results['errors'].append("成交表 Schema 验证失败")
        results['passed'] = False
    else:
        logger.info("✅ 成交表 Schema 验证通过")
    
    # 2. 排序验证
    logger.info("\n--- 排序验证 ---")
    if not verify_sorting(orders_df, ['SecurityID', 'TickTime', 'BizIndex'], '委托表'):
        results['errors'].append("委托表排序错误")
        results['passed'] = False
    
    if not verify_sorting(trades_df, ['SecurityID', 'TickTime', 'BizIndex'], '成交表'):
        results['errors'].append("成交表排序错误")
        results['passed'] = False
    
    # 3. SecurityID 验证
    logger.info("\n--- SecurityID 验证 ---")
    if not verify_security_id(orders_df, '委托表'):
        results['errors'].append("委托表 SecurityID 验证失败")
        results['passed'] = False
    
    if not verify_security_id(trades_df, '成交表'):
        results['errors'].append("成交表 SecurityID 验证失败")
        results['passed'] = False
    
    # 4. IsAggressive 验证
    logger.info("\n--- IsAggressive 验证 ---")
    if not verify_is_aggressive(orders_df):
        results['errors'].append("IsAggressive 验证失败")
        results['passed'] = False
    
    # 5. 通道数学关系验证
    logger.info("\n--- 通道数学关系验证 ---")
    if not verify_channel_math(orders_df):
        results['errors'].append("通道数学关系验证失败")
        results['passed'] = False
    
    return results


def main():
    """主入口函数"""
    parser = argparse.ArgumentParser(
        description='上交所逐笔数据拆解 - 输出验证脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python -m sh_tick_reconstruction.scripts.validate_output \\
        --date 20260126 \\
        --path /data/processed/

验证内容:
    1. Schema 验证 (字段类型、nullable bool)
    2. 排序验证 (SecurityID, TickTime, BizIndex)
    3. 通道数学关系验证 (Ch7=Ch9+Ch11, Ch8=Ch10+Ch12)
    4. SecurityID 存在性验证
    5. IsAggressive 分布统计
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        required=True,
        help='日期，格式 YYYYMMDD'
    )
    parser.add_argument(
        '--path', '-p',
        required=True,
        help='输出目录路径'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("上交所逐笔数据拆解 - 输出验证")
    logger.info("=" * 60)
    
    results = validate_output(args.date, args.path)
    
    logger.info("\n" + "=" * 60)
    if results['passed']:
        logger.info("✅ 所有验证通过!")
        sys.exit(0)
    else:
        logger.error("❌ 验证失败!")
        for error in results['errors']:
            logger.error(f"  - {error}")
        sys.exit(1)


if __name__ == '__main__':
    main()
