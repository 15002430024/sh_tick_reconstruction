# -*- coding: utf-8 -*-
"""
上交所逐笔数据拆解 - 数据模型定义

包含:
- OrderContext: 单个订单的上下文信息缓存类

对应需求文档: SH_Tick_Data_Reconstruction_Spec v1.8 Section 5.1
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OrderContext:
    """
    单个订单的上下文信息缓存类
    
    用于在处理上交所逐笔数据时，缓存每个订单的累计状态，
    支持多条成交记录聚合为一条委托记录。
    
    Attributes:
        order_no (int): 订单号
        side (str): 买卖方向，'B' (买) 或 'S' (卖)
        first_time (int): 该订单首次出现的时间 (TickTime, HHMMSSmmm格式)
        first_biz_index (int): 该订单首次出现的逐笔序号 (BizIndex)，用于排序和审计
        trade_qty (int): 累计成交量，默认为0
        resting_qty (int): 挂单量 (Type='A'记录的数量)，默认为0
        trade_price (float): 最新成交价，默认为0.0
        resting_price (float): 挂单价 (Type='A'记录的价格)，默认为0.0
        is_aggressive (bool): 入场进攻性标识，默认为False
            - True: Taker (消耗流动性) - 该订单首次出现为Type='T'
            - False: Maker (提供流动性) - 该订单首次出现为Type='A'
        has_resting (bool): 是否有挂单记录 (Type='A')，默认为False
    
    核心业务规则:
        1. is_aggressive 只看订单"出生方式"（首次出现的Type），不看后续经历
        2. 原始委托量 = trade_qty + resting_qty
        3. 委托价格优先使用 resting_price，其次使用 trade_price
    
    Examples:
        >>> # 创建一个主动买入订单（首次出现为成交）
        >>> ctx = OrderContext(
        ...     order_no=1001,
        ...     side='B',
        ...     first_time=93000540,
        ...     first_biz_index=12345,
        ...     is_aggressive=True  # 首次为Type='T'
        ... )
        >>> ctx.add_trade_qty(500)
        >>> ctx.trade_price = 10.5
        >>> 
        >>> # 后续部分转挂单
        >>> ctx.add_resting_qty(300)
        >>> ctx.resting_price = 10.5
        >>> ctx.has_resting = True
        >>> 
        >>> # 获取原始委托量
        >>> ctx.get_total_qty()
        800
        >>> 
        >>> # 获取委托价格（优先挂单价）
        >>> ctx.get_price()
        10.5
    """
    
    # 必填字段
    order_no: int           # 订单号
    side: str               # 'B' 或 'S'
    first_time: int         # 首次出现时间 (TickTime)
    first_biz_index: int    # 首次出现的 BizIndex
    
    # 可选字段（带默认值）
    trade_qty: int = 0          # 累计成交量
    resting_qty: int = 0        # 挂单量
    trade_price: float = 0.0    # 最新成交价
    resting_price: float = 0.0  # 挂单价
    is_aggressive: bool = False # 入场进攻性（默认 False = Maker）
    has_resting: bool = False   # 是否有挂单记录
    
    def add_trade_qty(self, qty: int) -> None:
        """
        累加成交量
        
        Args:
            qty: 本次成交数量（股）
        
        Raises:
            ValueError: 如果 qty 为负数
        
        Examples:
            >>> ctx = OrderContext(order_no=1, side='B', first_time=93000000, first_biz_index=1)
            >>> ctx.add_trade_qty(100)
            >>> ctx.add_trade_qty(200)
            >>> ctx.trade_qty
            300
        """
        if qty < 0:
            raise ValueError(f"成交量不能为负数: {qty}")
        self.trade_qty += qty
    
    def add_resting_qty(self, qty: int) -> None:
        """
        累加挂单量
        
        通常一个订单只会有一条 Type='A' 记录，
        但为了健壮性，支持累加。
        
        Args:
            qty: 挂单数量（股）
        
        Raises:
            ValueError: 如果 qty 为负数
        
        Examples:
            >>> ctx = OrderContext(order_no=1, side='B', first_time=93000000, first_biz_index=1)
            >>> ctx.add_resting_qty(500)
            >>> ctx.resting_qty
            500
        """
        if qty < 0:
            raise ValueError(f"挂单量不能为负数: {qty}")
        self.resting_qty += qty
    
    def get_price(self) -> float:
        """
        获取委托价格，优先使用挂单价
        
        价格优先级:
        1. resting_price (挂单价，更准确反映委托意图)
        2. trade_price (成交价，作为兜底)
        
        Returns:
            委托价格。如果两个价格都为0，返回0.0
        
        Examples:
            >>> ctx = OrderContext(order_no=1, side='B', first_time=93000000, first_biz_index=1)
            >>> ctx.trade_price = 10.0
            >>> ctx.get_price()  # 只有成交价
            10.0
            >>> ctx.resting_price = 10.5
            >>> ctx.get_price()  # 优先使用挂单价
            10.5
        """
        return self.resting_price if self.resting_price > 0 else self.trade_price
    
    def get_total_qty(self) -> int:
        """
        获取原始委托总量
        
        原始委托量 = 累计成交量 + 挂单量
        
        这个值代表投资者最初提交的委托规模，
        即"母单"的真实大小。
        
        Returns:
            原始委托总量（股）
        
        Examples:
            >>> ctx = OrderContext(order_no=1, side='B', first_time=93000000, first_biz_index=1)
            >>> ctx.add_trade_qty(300)
            >>> ctx.add_resting_qty(700)
            >>> ctx.get_total_qty()
            1000
        """
        return self.trade_qty + self.resting_qty
    
    def __repr__(self) -> str:
        """
        返回可读的字符串表示
        
        Returns:
            包含关键信息的字符串
        """
        return (
            f"OrderContext("
            f"order_no={self.order_no}, "
            f"side='{self.side}', "
            f"is_aggressive={self.is_aggressive}, "
            f"trade_qty={self.trade_qty}, "
            f"resting_qty={self.resting_qty}, "
            f"total_qty={self.get_total_qty()}, "
            f"price={self.get_price():.3f}"
            f")"
        )
