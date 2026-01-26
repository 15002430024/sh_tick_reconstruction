# -*- coding: utf-8 -*-
"""
上交所连续竞价时段过滤模块

判断给定时间是否属于上交所连续竞价时段，用于剔除集合竞价和休市数据。

时间规则（上交所特有）：
- 09:15 - 09:25  开盘集合竞价  ❌ 剔除
- 09:25 - 09:30  静默期        ❌ 剔除  
- 09:30 - 11:30  连续竞价(上午) ✅ 保留
- 11:30 - 13:00  午间休市      ❌ 剔除
- 13:00 - 15:00  连续竞价(下午) ✅ 保留

🔴 关键差异：
- 上交所下午连续竞价到 15:00（无收盘集合竞价）
- 深交所下午连续竞价到 14:57（有收盘集合竞价 14:57-15:00）

Version: 1.0.0
Date: 2026-01-26
"""

from typing import Tuple

# ============================================================================
# 时间常量定义 (HHMMSSmmm 格式)
# ============================================================================

# 上午连续竞价时段
MORNING_START = 93000000   # 09:30:00.000
MORNING_END = 113000000    # 11:30:00.000 (不含)

# 下午连续竞价时段
AFTERNOON_START = 130000000  # 13:00:00.000
AFTERNOON_END = 150000000    # 15:00:00.000 (不含)

# 开盘集合竞价（用于参考，非本模块处理范围）
OPEN_AUCTION_START = 91500000   # 09:15:00.000
OPEN_AUCTION_END = 92500000     # 09:25:00.000


# ============================================================================
# 核心函数
# ============================================================================

def is_continuous_trading_time(tick_time: int) -> bool:
    """
    判断是否为上交所连续竞价时段
    
    上交所连续竞价时段：
    - 上午：09:30:00.000 <= tick_time < 11:30:00.000
    - 下午：13:00:00.000 <= tick_time < 15:00:00.000
    
    Args:
        tick_time: HHMMSSmmm 格式的时间，如 93000540 表示 09:30:00.540
    
    Returns:
        bool: True 表示在连续竞价时段内，False 表示不在
    
    Note:
        ⚠️ 上交所无收盘集合竞价，下午连续竞价延续到 15:00
        与深交所的区别：深交所 14:57-15:00 是收盘集合竞价
    
    Examples:
        >>> is_continuous_trading_time(93000000)   # 9:30 开始
        True
        >>> is_continuous_trading_time(92500000)   # 9:25 开盘集合竞价
        False
        >>> is_continuous_trading_time(145700000)  # 14:57 ⭐ 上交所仍是连续竞价
        True
        >>> is_continuous_trading_time(150000000)  # 15:00 收盘
        False
        >>> is_continuous_trading_time(130000000)  # 13:00 下午开始
        True
    """
    # 上午连续竞价: [09:30, 11:30)
    if MORNING_START <= tick_time < MORNING_END:
        return True
    
    # 下午连续竞价: [13:00, 15:00)
    if AFTERNOON_START <= tick_time < AFTERNOON_END:
        return True
    
    return False


def get_trading_session(tick_time: int) -> str:
    """
    获取当前时间对应的交易时段名称
    
    Args:
        tick_time: HHMMSSmmm 格式的时间
    
    Returns:
        str: 时段名称
            - 'morning_auction': 开盘集合竞价 (09:15-09:25)
            - 'silent_period': 静默期 (09:25-09:30)
            - 'morning_continuous': 上午连续竞价 (09:30-11:30)
            - 'lunch_break': 午间休市 (11:30-13:00)
            - 'afternoon_continuous': 下午连续竞价 (13:00-15:00)
            - 'closed': 已收盘或非交易时段
    
    Examples:
        >>> get_trading_session(92000000)
        'morning_auction'
        >>> get_trading_session(100000000)
        'morning_continuous'
    """
    if tick_time < OPEN_AUCTION_START:
        return 'closed'
    elif tick_time < OPEN_AUCTION_END:
        return 'morning_auction'
    elif tick_time < MORNING_START:
        return 'silent_period'
    elif tick_time < MORNING_END:
        return 'morning_continuous'
    elif tick_time < AFTERNOON_START:
        return 'lunch_break'
    elif tick_time < AFTERNOON_END:
        return 'afternoon_continuous'
    else:
        return 'closed'


def parse_tick_time(tick_time: int) -> Tuple[int, int, int, int]:
    """
    解析 HHMMSSmmm 格式的时间
    
    Args:
        tick_time: HHMMSSmmm 格式的时间，如 93000540
    
    Returns:
        Tuple[int, int, int, int]: (hour, minute, second, millisecond)
    
    Examples:
        >>> parse_tick_time(93000540)
        (9, 30, 0, 540)
        >>> parse_tick_time(145959999)
        (14, 59, 59, 999)
    """
    millisecond = tick_time % 1000
    tick_time //= 1000
    second = tick_time % 100
    tick_time //= 100
    minute = tick_time % 100
    hour = tick_time // 100
    
    return (hour, minute, second, millisecond)


def format_tick_time(tick_time: int) -> str:
    """
    将 HHMMSSmmm 格式转换为可读字符串
    
    Args:
        tick_time: HHMMSSmmm 格式的时间
    
    Returns:
        str: 格式化后的时间字符串 "HH:MM:SS.mmm"
    
    Examples:
        >>> format_tick_time(93000540)
        '09:30:00.540'
        >>> format_tick_time(145700000)
        '14:57:00.000'
    """
    hour, minute, second, ms = parse_tick_time(tick_time)
    return f"{hour:02d}:{minute:02d}:{second:02d}.{ms:03d}"


# ============================================================================
# 模块测试
# ============================================================================

if __name__ == '__main__':
    # 验收测试用例
    test_cases = [
        (93000000, True, "9:30 连续竞价开始"),
        (92500000, False, "9:25 开盘集合竞价"),
        (145700000, True, "14:57 ⭐ 上交所仍是连续竞价"),
        (150000000, False, "15:00 收盘"),
        (130000000, True, "13:00 下午开始"),
        (113000000, False, "11:30 午间休市开始"),
        (112959999, True, "11:29:59.999 上午最后一刻"),
        (145959999, True, "14:59:59.999 下午最后一刻"),
        (91500000, False, "9:15 集合竞价开始"),
        (92959999, False, "9:29:59.999 静默期最后一刻"),
    ]
    
    print("=" * 60)
    print("上交所连续竞价时段判断测试")
    print("=" * 60)
    
    all_passed = True
    for tick_time, expected, description in test_cases:
        result = is_continuous_trading_time(tick_time)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_passed = False
        print(f"{status} {format_tick_time(tick_time)} -> {result:5} (期望: {expected:5}) | {description}")
    
    print("=" * 60)
    if all_passed:
        print("✅ 所有测试通过！")
    else:
        print("❌ 部分测试失败！")
