# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解还原模块

将上交所 auction_tick_merged_data 混合数据流拆解还原为:
- derived_sh_orders (落盘: {date}_sh_order_data.parquet)
- derived_sh_trades (落盘: {date}_sh_trade_data.parquet)

Version: 1.0.0
Date: 2026-01-26
"""

from .models import OrderContext
from .schema import (
    # PyArrow Schemas
    SH_ORDER_SCHEMA_PYARROW,
    SH_TRADE_SCHEMA_PYARROW,
    # Polars Schemas
    SH_ORDER_SCHEMA_POLARS,
    SH_TRADE_SCHEMA_POLARS,
    # 字段列表
    SH_ORDER_COLUMNS,
    SH_TRADE_COLUMNS,
    # 验证函数
    validate_order_schema,
    validate_trade_schema,
    # DataFrame 创建
    create_order_dataframe,
    create_trade_dataframe,
    # Parquet 读写
    write_order_parquet,
    write_trade_parquet,
    read_order_parquet,
    read_trade_parquet,
)
from .time_filter import (
    # 核心函数
    is_continuous_trading_time,
    get_trading_session,
    # 辅助函数
    parse_tick_time,
    format_tick_time,
    # 时间常量
    MORNING_START,
    MORNING_END,
    AFTERNOON_START,
    AFTERNOON_END,
)
from .reconstructor import (
    # 主函数 (Phase 1.4)
    reconstruct_sh_tick_data,
    # 处理函数 (Phase 2 占位)
    process_trade,
    process_add_order,
    process_delete_order,
    settle_orders,
    # 辅助函数
    validate_input_df,
    get_processing_stats,
)
from .batch import (
    # 批量处理 (Phase 3.2)
    process_daily_data,
    check_bizindex_continuity,
    # 辅助函数
    get_output_file_paths,
    validate_date_format,
)

__all__ = [
    # Models
    'OrderContext',
    # PyArrow Schemas
    'SH_ORDER_SCHEMA_PYARROW',
    'SH_TRADE_SCHEMA_PYARROW',
    # Polars Schemas
    'SH_ORDER_SCHEMA_POLARS',
    'SH_TRADE_SCHEMA_POLARS',
    # 字段列表
    'SH_ORDER_COLUMNS',
    'SH_TRADE_COLUMNS',
    # 验证函数
    'validate_order_schema',
    'validate_trade_schema',
    # DataFrame 创建
    'create_order_dataframe',
    'create_trade_dataframe',
    # Parquet 读写
    'write_order_parquet',
    'write_trade_parquet',
    'read_order_parquet',
    'read_trade_parquet',
    # 时间过滤 (Phase 1.3)
    'is_continuous_trading_time',
    'get_trading_session',
    'parse_tick_time',
    'format_tick_time',
    'MORNING_START',
    'MORNING_END',
    'AFTERNOON_START',
    'AFTERNOON_END',
    # 主函数 (Phase 1.4)
    'reconstruct_sh_tick_data',
    # 处理函数 (Phase 2)
    'process_trade',
    'process_add_order',
    'process_delete_order',
    'settle_orders',
    # 辅助函数
    'validate_input_df',
    'get_processing_stats',
    # 批量处理 (Phase 3.2)
    'process_daily_data',
    'check_bizindex_continuity',
    'get_output_file_paths',
    'validate_date_format',
]
__version__ = '1.0.0'

