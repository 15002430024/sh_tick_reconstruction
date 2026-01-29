#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 简化入口

快速运行:
    python main.py                                    # 使用默认配置
    python main.py --date 20251101                   # 只改日期
    python main.py --dates 20251030 20251031        # 批量处理多个日期
    python main.py --start-date 20251030 --end-date 20251031  # 日期范围

配置说明:
    修改下面的 CONFIG 字典即可调整默认路径
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from sh_tick_reconstruction import process_daily_data


# ============================================================================
# 默认配置 (修改这里)
# ============================================================================
CONFIG = {
    # 数据路径
    'input_dir': '/Users/shiyunshuo/Desktop/pythonproject/中邮基金/华泰金工vivit/通联逐笔数据',
    'output_dir': '/Users/shiyunshuo/Desktop/pythonproject/中邮基金/华泰金工vivit/output',
    
    # 输入文件命名模式
    'input_pattern': '{date}_sh_tick_data.parquet',
    
    # 输出文件命名模式（用于断点续传检查）
    'output_order_pattern': '{date}_sh_order_data.parquet',
    'output_trade_pattern': '{date}_sh_trade_data.parquet',
    
    # 默认日期 (YYYYMMDD)
    'default_date': '20251031',
    
    # 默认日期区间（用于批量处理）
    'start_date': '20251030',
    'end_date': '20251031',
    
    # 是否验证输出
    'validate_output': True,
    
    # 断点续传：是否跳过已存在的输出文件
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


def main():
    parser = argparse.ArgumentParser(
        description='上交所逐笔数据拆解 (简化版)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 使用默认日期
    python main.py
    
    # 指定单个日期
    python main.py --date 20251101
    
    # 批量处理多个日期
    python main.py --dates 20251030 20251031 20251101
    
    # 日期范围（包含起止日期）
    python main.py --start-date 20251030 --end-date 20251031
    
    # 强制重新处理（覆盖已有文件）
    python main.py --start-date 20251030 --end-date 20251031 --force
    
    # 覆盖输入/输出路径
    python main.py --input-dir /path/to/input --output-dir /path/to/output
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
        '--no-validate',
        action='store_true',
        help='跳过输出验证'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='强制重新处理，覆盖已存在的输出文件'
    )
    
    args = parser.parse_args()
    
    # 确定要处理的日期列表
    if args.start_date and args.end_date:
        # 日期范围模式
        try:
            dates = generate_date_range(args.start_date, args.end_date)
        except ValueError as e:
            print(f"❌ 日期范围错误: {e}")
            sys.exit(1)
    elif args.start_date or args.end_date:
        print("❌ 错误: --start-date 和 --end-date 必须同时使用")
        sys.exit(1)
    elif args.dates:
        # 批量日期模式
        dates = args.dates
    elif args.date:
        # 单个日期模式
        dates = [args.date]
    else:
        # 默认日期
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
    print(f"📊 上交所逐笔数据拆解")
    print(f"   输入目录: {input_dir}")
    print(f"   输出目录: {output_dir}")
    print(f"   处理日期: {dates}")
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
        input_file = input_dir / CONFIG['input_pattern'].format(date=date)
        
        if not input_file.exists():
            print(f"⚠️  跳过: 输入文件不存在 - {input_file}")
            failed_dates.append((date, '文件不存在'))
            continue
        
        # 断点续传：检查输出文件是否已存在
        if not args.force and CONFIG['skip_existing']:
            order_file = output_dir / CONFIG['output_order_pattern'].format(date=date)
            trade_file = output_dir / CONFIG['output_trade_pattern'].format(date=date)
            
            if order_file.exists() and trade_file.exists():
                print(f"✓ 跳过: 输出文件已存在 (使用 --force 强制重新处理)")
                print(f"   委托文件: {order_file.name}")
                print(f"   成交文件: {trade_file.name}")
                skipped_count += 1
                continue
        
        try:
            # 进度回调函数
            def progress_callback(security_id, current, total):
                if current % 10 == 0 or current == total:
                    print(f"  处理进度: {current}/{total} ({current*100//total}%) - {security_id}", flush=True)
            
            # 调用核心处理函数
            stats = process_daily_data(
                date=date,
                input_path=str(input_file),
                output_path=str(output_dir),
                validate_output=not args.no_validate,
                progress_callback=progress_callback
            )
            
            print(f"\n✅ 成功处理 {date}")
            print(f"   股票数: {stats['total_securities']:,}")
            print(f"   委托数: {stats['total_orders']:,}")
            print(f"   成交数: {stats['total_trades']:,}")
            print(f"   Taker: {stats['taker_orders']:,}, Maker: {stats['maker_orders']:,}")
            print(f"   耗时: {stats['processing_time_seconds']:.2f} 秒")
            
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