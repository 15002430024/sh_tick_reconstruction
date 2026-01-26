# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 每日批量处理脚本

用法:
    python -m sh_tick_reconstruction.scripts.run_daily --date 20260126 --input /path/to/input --output /path/to/output

对应需求文档: SH_Tick_Data_Reconstruction_Spec v1.8
对应落地计划: Phase 3.2
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sh_tick_reconstruction import process_daily_data


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'sh_tick_reconstruction_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)


def main():
    """主入口函数"""
    parser = argparse.ArgumentParser(
        description='上交所逐笔数据拆解 - 每日批量处理脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    # 处理单日数据
    python -m sh_tick_reconstruction.scripts.run_daily \\
        --date 20260126 \\
        --input /data/raw/auction_tick_merged_data_20260126.parquet \\
        --output /data/processed/

    # 带验证的处理
    python -m sh_tick_reconstruction.scripts.run_daily \\
        --date 20260126 \\
        --input /data/raw/auction_tick_merged_data_20260126.parquet \\
        --output /data/processed/ \\
        --validate

输出文件:
    {output_path}/{date}_sh_order_data.parquet  # 委托表
    {output_path}/{date}_sh_trade_data.parquet  # 成交表
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        required=True,
        help='日期，格式 YYYYMMDD (如 20260126)'
    )
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='输入 Parquet 文件路径'
    )
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='输出目录路径'
    )
    parser.add_argument(
        '--validate', '-v',
        action='store_true',
        default=True,
        help='是否验证输出 schema (默认: True)'
    )
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='跳过输出验证'
    )
    
    args = parser.parse_args()
    
    # 验证日期格式
    if len(args.date) != 8 or not args.date.isdigit():
        logger.error(f"日期格式不正确，应为 YYYYMMDD: {args.date}")
        sys.exit(1)
    
    # 验证输入文件存在
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"输入文件不存在: {args.input}")
        sys.exit(1)
    
    # 创建输出目录
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 确定是否验证
    validate = args.validate and not args.no_validate
    
    logger.info("=" * 60)
    logger.info("上交所逐笔数据拆解 - 每日批量处理")
    logger.info("=" * 60)
    logger.info(f"日期: {args.date}")
    logger.info(f"输入: {args.input}")
    logger.info(f"输出: {args.output}")
    logger.info(f"验证: {validate}")
    logger.info("=" * 60)
    
    try:
        # 执行处理
        stats = process_daily_data(
            date=args.date,
            input_path=str(input_path),
            output_path=str(output_path),
            validate_output=validate
        )
        
        # 输出统计信息
        logger.info("=" * 60)
        logger.info("处理完成!")
        logger.info("=" * 60)
        logger.info(f"处理股票数: {stats.get('total_securities', 'N/A')}")
        logger.info(f"委托记录数: {stats.get('total_orders', 'N/A')}")
        logger.info(f"成交记录数: {stats.get('total_trades', 'N/A')}")
        logger.info(f"新增委托数: {stats.get('new_orders', 'N/A')}")
        logger.info(f"撤单数量: {stats.get('cancel_orders', 'N/A')}")
        logger.info(f"主动委托 (Taker): {stats.get('taker_orders', 'N/A')}")
        logger.info(f"被动委托 (Maker): {stats.get('maker_orders', 'N/A')}")
        logger.info(f"处理耗时: {stats.get('processing_time_seconds', 'N/A'):.2f} 秒")
        logger.info("=" * 60)
        
        # 输出文件路径
        if 'output_files' in stats:
            logger.info("输出文件:")
            for f in stats['output_files']:
                logger.info(f"  - {f}")
        
        sys.exit(0)
        
    except FileNotFoundError as e:
        logger.error(f"文件不存在: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"参数错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"处理失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
