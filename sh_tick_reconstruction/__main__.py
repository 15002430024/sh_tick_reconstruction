# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 模块入口

用法:
    python -m sh_tick_reconstruction --date 20260126 --input /path/to/input.parquet --output /path/to/output/

或者使用子命令:
    python -m sh_tick_reconstruction run --date 20260126 --input ... --output ...
    python -m sh_tick_reconstruction validate --date 20260126 --path ...
"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        prog='sh_tick_reconstruction',
        description='上交所逐笔数据拆解还原工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 处理单日数据
    python -m sh_tick_reconstruction run \\
        --date 20260126 \\
        --input /data/raw/auction_tick_merged_data.parquet \\
        --output /data/processed/

    # 验证输出
    python -m sh_tick_reconstruction validate \\
        --date 20260126 \\
        --path /data/processed/

输出文件:
    {output}/{date}_sh_order_data.parquet  # 委托表
    {output}/{date}_sh_trade_data.parquet  # 成交表
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # run 子命令
    run_parser = subparsers.add_parser('run', help='处理单日数据')
    run_parser.add_argument('--date', '-d', required=True, help='日期 YYYYMMDD')
    run_parser.add_argument('--input', '-i', required=True, help='输入 Parquet 文件路径')
    run_parser.add_argument('--output', '-o', required=True, help='输出目录路径')
    run_parser.add_argument('--no-validate', action='store_true', help='跳过输出验证')
    
    # validate 子命令
    validate_parser = subparsers.add_parser('validate', help='验证输出数据')
    validate_parser.add_argument('--date', '-d', required=True, help='日期 YYYYMMDD')
    validate_parser.add_argument('--path', '-p', required=True, help='数据目录路径')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(0)
    
    if args.command == 'run':
        from .scripts.run_daily import main as run_main
        # 重新构造 sys.argv 给 run_daily
        sys.argv = [
            'run_daily',
            '--date', args.date,
            '--input', args.input,
            '--output', args.output,
        ]
        if args.no_validate:
            sys.argv.append('--no-validate')
        run_main()
        
    elif args.command == 'validate':
        from .scripts.validate_output import main as validate_main
        sys.argv = [
            'validate_output',
            '--date', args.date,
            '--path', args.path,
        ]
        validate_main()


if __name__ == '__main__':
    main()
