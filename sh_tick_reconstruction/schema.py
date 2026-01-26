# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 输出 Schema 定义

定义 sh_order_data 和 sh_trade_data 的输出 Schema，
支持 Polars 和 PyArrow 格式。

对应需求文档: SH_Tick_Data_Reconstruction_Spec v1.8 Section 3
"""

from typing import List, Dict, Any, Optional
import polars as pl
import pyarrow as pa


# ============================================================================
# sh_order_data Schema (还原后的委托表)
# ============================================================================

# PyArrow Schema - 用于 Parquet 写入
SH_ORDER_SCHEMA_PYARROW = pa.schema([
    pa.field('SecurityID', pa.string(), nullable=False),      # 证券代码（全市场输出必需）
    pa.field('BizIndex', pa.int64(), nullable=False),         # 首次出现的逐笔序号
    pa.field('TickTime', pa.int64(), nullable=False),         # 委托时间 (HHMMSSmmm)
    pa.field('OrdID', pa.int64(), nullable=False),            # 委托单号
    pa.field('OrdType', pa.string(), nullable=False),         # 'New' 或 'Cancel'
    pa.field('Side', pa.string(), nullable=False),            # 'B' 或 'S'
    pa.field('Price', pa.float64(), nullable=False),          # 委托价格
    pa.field('Qty', pa.int64(), nullable=False),              # 原始委托量
    pa.field('IsAggressive', pa.bool_(), nullable=True),      # ⭐ Nullable Boolean
])

# Polars Schema - 用于 DataFrame 操作
SH_ORDER_SCHEMA_POLARS = {
    'SecurityID': pl.Utf8,
    'BizIndex': pl.Int64,
    'TickTime': pl.Int64,
    'OrdID': pl.Int64,
    'OrdType': pl.Utf8,
    'Side': pl.Utf8,
    'Price': pl.Float64,
    'Qty': pl.Int64,
    'IsAggressive': pl.Boolean,  # Polars Boolean 默认支持 null
}

# 字段列表 - 用于列选择和排序
SH_ORDER_COLUMNS = [
    'SecurityID', 'BizIndex', 'TickTime', 'OrdID', 
    'OrdType', 'Side', 'Price', 'Qty', 'IsAggressive'
]


# ============================================================================
# sh_trade_data Schema (标准化成交表)
# ============================================================================

# PyArrow Schema - 用于 Parquet 写入
SH_TRADE_SCHEMA_PYARROW = pa.schema([
    pa.field('SecurityID', pa.string(), nullable=False),      # 证券代码
    pa.field('BizIndex', pa.int64(), nullable=False),         # 逐笔序号
    pa.field('TickTime', pa.int64(), nullable=False),         # 成交时间 (HHMMSSmmm)
    pa.field('BidOrdID', pa.int64(), nullable=False),         # 买单号
    pa.field('AskOrdID', pa.int64(), nullable=False),         # 卖单号
    pa.field('Price', pa.float64(), nullable=False),          # 成交价
    pa.field('Qty', pa.int64(), nullable=False),              # 成交量
    pa.field('TradeMoney', pa.float64(), nullable=False),     # 成交金额
    pa.field('ActiveSide', pa.int8(), nullable=False),        # 1=主动买, 2=主动卖, 0=集合竞价
])

# Polars Schema - 用于 DataFrame 操作
SH_TRADE_SCHEMA_POLARS = {
    'SecurityID': pl.Utf8,
    'BizIndex': pl.Int64,
    'TickTime': pl.Int64,
    'BidOrdID': pl.Int64,
    'AskOrdID': pl.Int64,
    'Price': pl.Float64,
    'Qty': pl.Int64,
    'TradeMoney': pl.Float64,
    'ActiveSide': pl.Int8,
}

# 字段列表 - 用于列选择和排序
SH_TRADE_COLUMNS = [
    'SecurityID', 'BizIndex', 'TickTime', 'BidOrdID', 
    'AskOrdID', 'Price', 'Qty', 'TradeMoney', 'ActiveSide'
]


# ============================================================================
# Schema 验证函数
# ============================================================================

def validate_order_schema(df: pl.DataFrame) -> bool:
    """
    验证委托 DataFrame 是否符合 sh_order_data Schema
    
    Args:
        df: 待验证的 Polars DataFrame
    
    Returns:
        True if schema 匹配
    
    Raises:
        ValueError: 如果 schema 不匹配，附带详细错误信息
    
    Examples:
        >>> df = pl.DataFrame({
        ...     'SecurityID': ['600519'],
        ...     'BizIndex': [12345],
        ...     'TickTime': [93000540],
        ...     'OrdID': [1001],
        ...     'OrdType': ['New'],
        ...     'Side': ['B'],
        ...     'Price': [1800.0],
        ...     'Qty': [100],
        ...     'IsAggressive': [True],
        ... })
        >>> validate_order_schema(df)
        True
    """
    errors = []
    
    # 检查必需字段是否存在
    for col in SH_ORDER_COLUMNS:
        if col not in df.columns:
            errors.append(f"缺少必需字段: {col}")
    
    if errors:
        raise ValueError(f"委托表 Schema 验证失败:\n" + "\n".join(errors))
    
    # 检查字段类型
    for col, expected_dtype in SH_ORDER_SCHEMA_POLARS.items():
        if col in df.columns:
            actual_dtype = df[col].dtype
            # Polars 类型兼容性检查
            if not _is_dtype_compatible(actual_dtype, expected_dtype):
                errors.append(
                    f"字段 {col} 类型不匹配: 期望 {expected_dtype}, 实际 {actual_dtype}"
                )
    
    # 检查 IsAggressive 是否支持 null
    if 'IsAggressive' in df.columns:
        # Polars Boolean 类型默认支持 null，此处验证通过
        pass
    
    if errors:
        raise ValueError(f"委托表 Schema 验证失败:\n" + "\n".join(errors))
    
    return True


def validate_trade_schema(df: pl.DataFrame) -> bool:
    """
    验证成交 DataFrame 是否符合 sh_trade_data Schema
    
    Args:
        df: 待验证的 Polars DataFrame
    
    Returns:
        True if schema 匹配
    
    Raises:
        ValueError: 如果 schema 不匹配，附带详细错误信息
    
    Examples:
        >>> df = pl.DataFrame({
        ...     'SecurityID': ['600519'],
        ...     'BizIndex': [12345],
        ...     'TickTime': [93000540],
        ...     'BidOrdID': [1001],
        ...     'AskOrdID': [2001],
        ...     'Price': [1800.0],
        ...     'Qty': [100],
        ...     'TradeMoney': [180000.0],
        ...     'ActiveSide': [1],
        ... })
        >>> validate_trade_schema(df)
        True
    """
    errors = []
    
    # 检查必需字段是否存在
    for col in SH_TRADE_COLUMNS:
        if col not in df.columns:
            errors.append(f"缺少必需字段: {col}")
    
    if errors:
        raise ValueError(f"成交表 Schema 验证失败:\n" + "\n".join(errors))
    
    # 检查字段类型
    for col, expected_dtype in SH_TRADE_SCHEMA_POLARS.items():
        if col in df.columns:
            actual_dtype = df[col].dtype
            if not _is_dtype_compatible(actual_dtype, expected_dtype):
                errors.append(
                    f"字段 {col} 类型不匹配: 期望 {expected_dtype}, 实际 {actual_dtype}"
                )
    
    # 检查 ActiveSide 值范围
    if 'ActiveSide' in df.columns and len(df) > 0:
        invalid_values = df.filter(
            ~pl.col('ActiveSide').is_in([0, 1, 2])
        )
        if len(invalid_values) > 0:
            errors.append(
                f"ActiveSide 存在非法值 (应为 0/1/2): "
                f"发现 {invalid_values['ActiveSide'].unique().to_list()}"
            )
    
    if errors:
        raise ValueError(f"成交表 Schema 验证失败:\n" + "\n".join(errors))
    
    return True


def _is_dtype_compatible(actual: pl.DataType, expected: pl.DataType) -> bool:
    """
    检查两个 Polars 数据类型是否兼容
    
    Args:
        actual: 实际数据类型
        expected: 期望数据类型
    
    Returns:
        True if 兼容
    """
    # 直接匹配
    if actual == expected:
        return True
    
    # 整数类型兼容性 (Int32 可以兼容 Int64 等)
    int_types = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
    if actual in int_types and expected in int_types:
        return True
    
    # 浮点类型兼容性
    float_types = {pl.Float32, pl.Float64}
    if actual in float_types and expected in float_types:
        return True
    
    # 字符串类型兼容性
    string_types = {pl.Utf8, pl.String}
    if actual in string_types and expected in string_types:
        return True
    
    return False


# ============================================================================
# DataFrame 创建辅助函数
# ============================================================================

def create_order_dataframe(records: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    从记录列表创建符合 Schema 的委托 DataFrame
    
    Args:
        records: 委托记录字典列表
    
    Returns:
        符合 sh_order_data schema 的 Polars DataFrame
    
    Examples:
        >>> records = [
        ...     {'SecurityID': '600519', 'BizIndex': 12345, 'TickTime': 93000540,
        ...      'OrdID': 1001, 'OrdType': 'New', 'Side': 'B', 
        ...      'Price': 1800.0, 'Qty': 100, 'IsAggressive': True},
        ... ]
        >>> df = create_order_dataframe(records)
        >>> df.shape
        (1, 9)
    """
    if not records:
        # 返回空 DataFrame，但带有正确的 schema
        return pl.DataFrame(schema=SH_ORDER_SCHEMA_POLARS)
    
    df = pl.DataFrame(records)
    
    # 确保列顺序和类型
    df = df.select([
        pl.col('SecurityID').cast(pl.Utf8),
        pl.col('BizIndex').cast(pl.Int64),
        pl.col('TickTime').cast(pl.Int64),
        pl.col('OrdID').cast(pl.Int64),
        pl.col('OrdType').cast(pl.Utf8),
        pl.col('Side').cast(pl.Utf8),
        pl.col('Price').cast(pl.Float64),
        pl.col('Qty').cast(pl.Int64),
        pl.col('IsAggressive').cast(pl.Boolean),  # 支持 null
    ])
    
    return df


def create_trade_dataframe(records: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    从记录列表创建符合 Schema 的成交 DataFrame
    
    Args:
        records: 成交记录字典列表
    
    Returns:
        符合 sh_trade_data schema 的 Polars DataFrame
    
    Examples:
        >>> records = [
        ...     {'SecurityID': '600519', 'BizIndex': 12345, 'TickTime': 93000540,
        ...      'BidOrdID': 1001, 'AskOrdID': 2001, 'Price': 1800.0,
        ...      'Qty': 100, 'TradeMoney': 180000.0, 'ActiveSide': 1},
        ... ]
        >>> df = create_trade_dataframe(records)
        >>> df.shape
        (1, 9)
    """
    if not records:
        # 返回空 DataFrame，但带有正确的 schema
        return pl.DataFrame(schema=SH_TRADE_SCHEMA_POLARS)
    
    df = pl.DataFrame(records)
    
    # 确保列顺序和类型
    df = df.select([
        pl.col('SecurityID').cast(pl.Utf8),
        pl.col('BizIndex').cast(pl.Int64),
        pl.col('TickTime').cast(pl.Int64),
        pl.col('BidOrdID').cast(pl.Int64),
        pl.col('AskOrdID').cast(pl.Int64),
        pl.col('Price').cast(pl.Float64),
        pl.col('Qty').cast(pl.Int64),
        pl.col('TradeMoney').cast(pl.Float64),
        pl.col('ActiveSide').cast(pl.Int8),
    ])
    
    return df


# ============================================================================
# Parquet 读写辅助函数
# ============================================================================

def write_order_parquet(
    df: pl.DataFrame, 
    path: str, 
    validate: bool = True
) -> None:
    """
    将委托 DataFrame 写入 Parquet 文件
    
    Args:
        df: 委托 DataFrame
        path: 输出文件路径
        validate: 是否在写入前验证 schema
    
    Raises:
        ValueError: 如果 validate=True 且 schema 不匹配
    
    Examples:
        >>> df = create_order_dataframe([...])
        >>> write_order_parquet(df, '/raw_data/20260126_sh_order_data.parquet')
    """
    if validate:
        validate_order_schema(df)
    
    # 确保按 (SecurityID, TickTime, BizIndex) 排序
    df = df.sort(['SecurityID', 'TickTime', 'BizIndex'])
    
    # 转换为 PyArrow Table 后写入，确保使用正确的 schema
    arrow_table = df.to_arrow()
    # 使用指定 schema 确保 IsAggressive 为 nullable bool
    arrow_table = arrow_table.cast(SH_ORDER_SCHEMA_PYARROW)
    import pyarrow.parquet as pq
    pq.write_table(arrow_table, path)


def write_trade_parquet(
    df: pl.DataFrame, 
    path: str, 
    validate: bool = True
) -> None:
    """
    将成交 DataFrame 写入 Parquet 文件
    
    Args:
        df: 成交 DataFrame
        path: 输出文件路径
        validate: 是否在写入前验证 schema
    
    Raises:
        ValueError: 如果 validate=True 且 schema 不匹配
    
    Examples:
        >>> df = create_trade_dataframe([...])
        >>> write_trade_parquet(df, '/raw_data/20260126_sh_trade_data.parquet')
    """
    if validate:
        validate_trade_schema(df)
    
    # 确保按 (SecurityID, TickTime, BizIndex) 排序
    df = df.sort(['SecurityID', 'TickTime', 'BizIndex'])
    
    # 转换为 PyArrow Table 后写入，确保使用正确的 schema
    arrow_table = df.to_arrow()
    arrow_table = arrow_table.cast(SH_TRADE_SCHEMA_PYARROW)
    import pyarrow.parquet as pq
    pq.write_table(arrow_table, path)


def read_order_parquet(path: str, validate: bool = True) -> pl.DataFrame:
    """
    从 Parquet 文件读取委托 DataFrame
    
    Args:
        path: 输入文件路径
        validate: 是否在读取后验证 schema
    
    Returns:
        委托 DataFrame
    
    Raises:
        ValueError: 如果 validate=True 且 schema 不匹配
    
    Examples:
        >>> df = read_order_parquet('/raw_data/20260126_sh_order_data.parquet')
        >>> df.shape
        (10000, 9)
    """
    df = pl.read_parquet(path)
    
    if validate:
        validate_order_schema(df)
    
    return df


def read_trade_parquet(path: str, validate: bool = True) -> pl.DataFrame:
    """
    从 Parquet 文件读取成交 DataFrame
    
    Args:
        path: 输入文件路径
        validate: 是否在读取后验证 schema
    
    Returns:
        成交 DataFrame
    
    Raises:
        ValueError: 如果 validate=True 且 schema 不匹配
    
    Examples:
        >>> df = read_trade_parquet('/raw_data/20260126_sh_trade_data.parquet')
        >>> df.shape
        (50000, 9)
    """
    df = pl.read_parquet(path)
    
    if validate:
        validate_trade_schema(df)
    
    return df


# ============================================================================
# 模块导出
# ============================================================================

__all__ = [
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
]
