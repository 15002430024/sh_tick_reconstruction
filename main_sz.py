#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
深交所数据重构 - 简化入口

功能：按 SecurityID 排序重构 Parquet 文件，提升单股票读取性能 37 倍

快速运行:
    python main_sz.py                                      # 使用默认配置
    python main_sz.py --date 20251031                     # 单个日期
    python main_sz.py --start-date 20251030 --end-date 20251031  # 日期范围

性能对比:
    重构前: ~1.12秒/只股票（全文件扫描）
    重构后: ~0.03秒/只股票（Row Group 跳过）

注意:
    处理后的数据输出到 output/ 目录，与上交所数据在同一位置
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from sz_data_reconstructor import reconstruct_sz_parquet


# ============================================================================
# 默认配置 (修改这里)
# ============================================================================
CONFIG = {
    # 数据路径
    'input_dir': '/Users/shiyunshuo/Desktop/pythonproject/中邮基金/华泰金工vivit/通联逐笔数据',
    'output_dir': '/Users/shiyunshuo/Desktop/pythonproject/中邮基金/华泰金工vivit/output',
    
    # 输入文件命名模式
    'input_trade_pattern': '{date}_sz_trade_data.parquet',
    'input_order_pattern': '{date}_sz_order_data.parquet',
    
    # 输出文件命名模式（保持与输入一致，方便 l2_image_builder 读取）
    'output_trade_pattern': '{date}_sz_trade_data.parquet',
    'output_order_pattern': '{date}_sz_order_data.parquet',
    
    # 默认日期 (YYYYMMDD)
    'default_date': '20251031',
    
    # 默认日期区间（用于批量处理）
    'start_date': '20251030',
    'end_date': '20251031',
    
    # 重构参数
    'row_group_size': 100_000,      # Row Group 大小
    'compression': 'zstd',          # 压缩算法 (zstd 或 lz4)
    'file_type': 'both',            # 重构类型 (trade/order/both)
    
    # 断点续传：是否跳过已重构的文件
    'skip_existing': True,
}


# ============================================================================
# 辅助函数
# ============================================================================
def generate_date_range(start_date: str, end_date: str) -> list:
    """生成日期范围列表"""
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    if start > end:
        raise ValueError(f"开始日期 {start_date} 晚于结束日期 {end_date}")
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    return dates


def check_if_reconstructed(date: str, output_dir: Path, file_type: str) -> bool:
    """检查输出目录中是否已存在重构后的文件"""
    if file_type in ('trade', 'both'):
        trade_file = output_dir / CONFIG['output_trade_pattern'].format(date=date)
        if not trade_file.exists():
            return False
    
    if file_type in ('order', 'both'):
        order_file = output_dir / CONFIG['output_order_pattern'].format(date=date)
        if not order_file.exists():
            return False
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='深交所数据重构 (性能优化)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 使用默认日期
    python main_sz.py
    
    # 指定单个日期
    python main_sz.py --date 20251031
    
    # 批量处理多个日期
    python main_sz.py --dates 20251030 20251031
    
    # 日期范围（包含起止日期）
    python main_sz.py --start-date 20251030 --end-date 20251031
    
    # 强制重新处理（覆盖已有文件）
    python main_sz.py --date 20251031 --force
    
    # 仅重构成交数据
    python main_sz.py --date 20251031 --file-type trade
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        help=f'单个日期 YYYYMMDD (默认: {CONFIG["default_date"]})'
    )
    parser.add_argument(
        '--dates',
        nargs='+',
        help='批量处理多个日期 (如: --dates 20251030 20251031)'
    )
    parser.add_argument(
        '--start-date',
        default=CONFIG.get('start_date'),
        help=f'开始日期 YYYYMMDD (默认: {CONFIG.get("start_date", "无")}，与 --end-date 配合使用)'
    )
    parser.add_argument(
        '--end-date',
        default=CONFIG.get('end_date'),
        help=f'结束日期 YYYYMMDD (默认: {CONFIG.get("end_date", "无")}，与 --start-date 配合使用)'
    )
    parser.add_argument(
        '--input-dir',
        default=CONFIG['input_dir'],
        help=f'输入目录 (默认: {CONFIG["input_dir"]})'
    )
    parser.add_argument(
        '--output-dir',
        default=CONFIG['output_dir'],
        help=f'输出目录 (默认: {CONFIG["output_dir"]})'
    )
    parser.add_argument(
        '--file-type',
        choices=['trade', 'order', 'both'],
        default=CONFIG['file_type'],
        help='重构类型 (默认: both)'
    )
    parser.add_argument(
        '--row-group-size',
        type=int,
        default=CONFIG['row_group_size'],
        help=f'Row Group 大小 (默认: {CONFIG["row_group_size"]})'
    )
    parser.add_argument(
        '--compression',
        choices=['zstd', 'lz4', 'snappy'],
        default=CONFIG['compression'],
        help=f'压缩算法 (默认: {CONFIG["compression"]})'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='强制重新处理，覆盖已存在的输出文件'
    )
    
    args = parser.parse_args()
    
    # 确定要处理的日期列表
    if args.start_date and args.end_date:
        try:
            dates = generate_date_range(args.start_date, args.end_date)
        except ValueError as e:
            print(f"❌ 日期范围错误: {e}")
            sys.exit(1)
    elif args.start_date or args.end_date:
        print("❌ 错误: --start-date 和 --end-date 必须同时使用")
        sys.exit(1)
    elif args.dates:
        dates = args.dates
    elif args.date:
        dates = [args.date]
    else:
        dates = [CONFIG['default_date']]
    
    # 确保路径存在
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    
    if not input_dir.exists():
        print(f"❌ 错误: 输入目录不存在: {input_dir}")
        sys.exit(1)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 批量处理
    print("=" * 70)
    print(f"🔧 深交所数据重构（性能优化）")
    print(f"   输入目录: {input_dir}")
    print(f"   输出目录: {output_dir}")
    print(f"   处理日期: {dates}")
    print(f"   重构类型: {args.file_type}")
    print(f"   Row Group: {args.row_group_size:,}")
    print(f"   压缩算法: {args.compression}")
    print("=" * 70)
    print()
    
    success_count = 0
    skipped_count = 0
    failed_dates = []
    
    for idx, date in enumerate(dates, 1):
        print(f"\n{'=' * 70}")
        print(f"🔄 处理日期: {date} ({idx}/{len(dates)})")
        print(f"{'=' * 70}")
        
        # 构造输入文件路径
        trade_input = input_dir / CONFIG['input_trade_pattern'].format(date=date)
        order_input = input_dir / CONFIG['input_order_pattern'].format(date=date)
        
        # 检查输入文件是否存在
        if args.file_type in ('trade', 'both') and not trade_input.exists():
            print(f"⚠️  跳过: 成交数据文件不存在 - {trade_input}")
            failed_dates.append((date, '成交文件不存在'))
            continue
        
        if args.file_type in ('order', 'both') and not order_input.exists():
            print(f"⚠️  跳过: 委托数据文件不存在 - {order_input}")
            failed_dates.append((date, '委托文件不存在'))
            continue
        
        # 断点续传：检查输出目录中是否已有重构后的文件
        if not args.force and CONFIG['skip_existing']:
            if check_if_reconstructed(date, output_dir, args.file_type):
                print(f"✓ 跳过: 输出文件已存在 (使用 --force 强制重新处理)")
                skipped_count += 1
                continue
        
        try:
            # 调用重构函数
            print(f"  读取、排序、写入中...", flush=True)
            stats = reconstruct_sz_parquet(
                date=date,
                input_dir=str(input_dir),
                output_dir=str(output_dir),
                file_type=args.file_type,
                row_group_size=args.row_group_size,
                compression=args.compression,
                overwrite=args.force,
            )
            
            print(f"\n✅ 成功重构 {date}")
            if stats.get('trade_rows'):
                print(f"   成交: {stats['trade_rows']:,} 行, "
                      f"{stats['trade_size_before']/1e6:.1f}MB → "
                      f"{stats['trade_size_after']/1e6:.1f}MB "
                      f"(压缩 {stats['trade_size_before']/stats['trade_size_after']:.2f}x)")
            if stats.get('order_rows'):
                print(f"   委托: {stats['order_rows']:,} 行, "
                      f"{stats['order_size_before']/1e6:.1f}MB → "
                      f"{stats['order_size_after']/1e6:.1f}MB "
                      f"(压缩 {stats['order_size_before']/stats['order_size_after']:.2f}x)")
            
            success_count += 1
            
        except Exception as e:
            print(f"\n❌ 处理失败 {date}: {e}")
            failed_dates.append((date, str(e)))
            import traceback
            traceback.print_exc()
    
    # 最终汇总
    print(f"\n{'=' * 70}")
    print(f"📈 处理汇总")
    print(f"   总日期数: {len(dates)}")
    print(f"   成功: {success_count}")
    print(f"   跳过: {skipped_count}")
    print(f"   失败: {len(failed_dates)}")
    
    if failed_dates:
        print(f"\n失败列表:")
        for date, reason in failed_dates:
            print(f"   - {date}: {reason}")
    
    print(f"{'=' * 70}")
    
    # 返回退出码
    sys.exit(0 if len(failed_dates) == 0 else 1)


if __name__ == '__main__':
    main()
