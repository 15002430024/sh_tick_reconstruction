# 上交所逐笔数据拆解项目开发进度追踪

> 本文档由 AI Agent 自动维护，记录项目开发进度和实现细节

## 📋 项目概述

- **项目名称**: 上交所逐笔数据拆解还原 (SH Tick Data Reconstruction)
- **创建日期**: 2026-01-26
- **最后更新**: 2026-01-26 (Phase 4.1 单元测试套件完成)
- **当前状态**: 开发中
- **目标**: 将上交所 `auction_tick_merged_data` 混合数据流拆解还原为独立的委托表和成交表
- **对应需求文档**: SH_Tick_Data_Reconstruction_Spec v1.8
- **落地计划**: SH_Tick_Reconstruction_Plan v1.1

---

## 📊 输出产物

| 逻辑表名 | 落盘文件名 | 对标深交所 |
|----------|-----------|-----------|
| `derived_sh_orders` | `{date}_sh_order_data.parquet` | `sz_order_data` |
| `derived_sh_trades` | `{date}_sh_trade_data.parquet` | `sz_trade_data` |

---

## ✅ 已实现功能

### Phase 1: 数据结构与基础框架

| 任务ID | 功能 | 状态 | 实现日期 | 说明 |
|--------|------|------|----------|------|
| 1.1 | OrderContext 数据类 | ✅ 完成 | 2026-01-26 | 含 has_resting 字段，完整文档 |
| 1.2 | 输出 Schema 定义 | ✅ 完成 | 2026-01-26 | PyArrow/Polars Schema，Parquet 读写 |
| 1.3 | 时间过滤函数 | ✅ 完成 | 2026-01-26 | is_continuous_trading_time() |
| 1.4 | 主函数框架 | ✅ 完成 | 2026-01-26 | reconstruct_sh_tick_data() |

### Phase 2: 核心处理逻辑

| 任务ID | 功能 | 状态 | 实现日期 | 说明 |
|--------|------|------|----------|------|
| 2.1 | process_trade() | ✅ 完成 | 2026-01-26 | 处理 Type='T' 成交记录 |
| 2.2 | process_add_order() | ✅ 完成 | 2026-01-26 | 处理 Type='A' 挂单记录 |
| 2.3 | process_delete_order() | ✅ 完成 | 2026-01-26 | 处理 Type='D' 撤单记录 |
| 2.4 | settle_orders() | ✅ 完成 | 2026-01-26 | 批次结算，输出聚合委托 |

### Phase 3: 排序与批量处理

| 任务ID | 功能 | 状态 | 实现日期 | 说明 |
|--------|------|------|----------|------|
| 3.1 | 完整主函数 | ✅ 完成 | 2026-01-26 | 端到端可运行，(TickTime, BizIndex) 双重排序 |
| 3.2 | 批量处理入口 | ✅ 完成 | 2026-01-26 | process_daily_data(), 17项测试通过 |
| 3.3 | 全市场排序输出 | ✅ 完成 | 2026-01-26 | (SecurityID, TickTime, BizIndex) |
| 3.4 | BizIndex 连续性检查 | ✅ 完成 | 2026-01-26 | check_bizindex_continuity() |

### Phase 4: 测试与验证

| 任务ID | 功能 | 状态 | 实现日期 | 说明 |
|--------|------|------|----------|------|
| 4.1 | 单元测试 | ✅ 完成 | 2026-01-26 | 7个核心场景, 20项测试通过 |
| 4.2 | 通道数学关系校验 | ✅ 完成 | 2026-01-26 | Ch7=Ch9+Ch11 等验证 |
| 4.3 | 真实数据验证 | ⏳ 待开发 | - | 抽样 10 只股票 |
| 4.4 | 边界情况处理 | ✅ 完成 | 2026-01-26 | Price=0, OrderNO=0, 空输入 |

### Phase 5: 与图像构建对接

| 任务ID | 功能 | 状态 | 实现日期 | 说明 |
|--------|------|------|----------|------|
| 5.1 | 接入 trades 通道 | ⏳ 待开发 | - | Ch0-6 |
| 5.2 | 接入 orders 通道 | ⏳ 待开发 | - | Ch7-14 |
| 5.3 | 沪深统一化验证 | ⏳ 待开发 | - | ActiveSide 格式一致 |

---

## 🔌 接口定义

### 数据模型 (models.py)

```python
@dataclass
class OrderContext:
    """
    单个订单的上下文信息缓存类
    
    用于在处理上交所逐笔数据时，缓存每个订单的累计状态，
    支持多条成交记录聚合为一条委托记录。
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
    
    # 辅助方法
    def add_trade_qty(self, qty: int) -> None: ...
    def add_resting_qty(self, qty: int) -> None: ...
    def get_price(self) -> float: ...
    def get_total_qty(self) -> int: ...
```

### 输出 Schema (schema.py)

```python
# PyArrow Schema - 用于 Parquet 写入
SH_ORDER_SCHEMA_PYARROW = pa.schema([
    pa.field('SecurityID', pa.string(), nullable=False),
    pa.field('BizIndex', pa.int64(), nullable=False),
    pa.field('TickTime', pa.int64(), nullable=False),
    pa.field('OrdID', pa.int64(), nullable=False),
    pa.field('OrdType', pa.string(), nullable=False),
    pa.field('Side', pa.string(), nullable=False),
    pa.field('Price', pa.float64(), nullable=False),
    pa.field('Qty', pa.int64(), nullable=False),
    pa.field('IsAggressive', pa.bool_(), nullable=True),  # ⭐ Nullable Boolean
])

SH_TRADE_SCHEMA_PYARROW = pa.schema([
    pa.field('SecurityID', pa.string(), nullable=False),
    pa.field('BizIndex', pa.int64(), nullable=False),
    pa.field('TickTime', pa.int64(), nullable=False),
    pa.field('BidOrdID', pa.int64(), nullable=False),
    pa.field('AskOrdID', pa.int64(), nullable=False),
    pa.field('Price', pa.float64(), nullable=False),
    pa.field('Qty', pa.int64(), nullable=False),
    pa.field('TradeMoney', pa.float64(), nullable=False),
    pa.field('ActiveSide', pa.int8(), nullable=False),  # 1=主动买, 2=主动卖, 0=集合竞价
])

# 验证函数
def validate_order_schema(df: pl.DataFrame) -> bool: ...
def validate_trade_schema(df: pl.DataFrame) -> bool: ...

# DataFrame 创建
def create_order_dataframe(records: List[Dict]) -> pl.DataFrame: ...
def create_trade_dataframe(records: List[Dict]) -> pl.DataFrame: ...

# Parquet 读写
def write_order_parquet(df: pl.DataFrame, path: str, validate: bool = True) -> None: ...
def write_trade_parquet(df: pl.DataFrame, path: str, validate: bool = True) -> None: ...
def read_order_parquet(path: str, validate: bool = True) -> pl.DataFrame: ...
def read_trade_parquet(path: str, validate: bool = True) -> pl.DataFrame: ...
```

### 时间过滤 (time_filter.py)

```python
# 时间常量 (HHMMSSmmm 格式)
MORNING_START = 93000000     # 09:30:00.000 上午开始
MORNING_END = 113000000      # 11:30:00.000 上午结束
AFTERNOON_START = 130000000  # 13:00:00.000 下午开始
AFTERNOON_END = 150000000    # 15:00:00.000 下午结束（上交所无收盘集合竞价）

# 核心函数
def is_continuous_trading_time(tick_time: int) -> bool:
    """
    判断是否为上交所连续竞价时段
    
    Args:
        tick_time: HHMMSSmmm 格式的时间，如 93000540
    
    Returns:
        bool: True=连续竞价时段, False=非连续竞价时段
    
    Note:
        上交所无收盘集合竞价，下午连续竞价延续到 15:00
        深交所 14:57-15:00 是收盘集合竞价（本函数不适用）
    """

def get_trading_session(tick_time: int) -> str:
    """
    获取当前时间对应的交易时段名称
    
    Returns:
        str: 'morning_auction' | 'silent_period' | 'morning_continuous' |
             'lunch_break' | 'afternoon_continuous' | 'closed'
    """

# 辅助函数
def parse_tick_time(tick_time: int) -> Tuple[int, int, int, int]: ...
def format_tick_time(tick_time: int) -> str: ...
```

### 主函数 (reconstructor.py)

```python
def reconstruct_sh_tick_data(
    df: pl.DataFrame,
    security_id: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    上交所逐笔数据拆解还原主函数
    
    Args:
        df: 单只股票当日的 auction_tick_merged_data (Polars DataFrame)
        security_id: 证券代码 (如 '600519')
    
    Returns:
        Tuple[List[dict], List[dict]]:
            - order_list: 还原后的委托列表 (含 SecurityID)
            - trade_list: 标准化成交列表 (含 SecurityID)
    
    Processing Flow:
        1. 预处理: 剔除 Type='S', 时间过滤, 排序
        2. 初始化: order_map, order_list, trade_list, last_price
        3. 逐行处理: Type='T'/'A'/'D' 分发
        4. 批次结算: settle_orders()
        5. 排序输出: (TickTime, BizIndex)
    """

# 核心处理函数 (Phase 2)
def process_trade(row, order_map, trade_list, security_id) -> None: ...  # ✅ 已实现
def process_add_order(row, order_map) -> None: ...  # ✅ 已实现
def process_delete_order(row, order_map, order_list, last_price, security_id) -> None: ...  # ✅ 已实现
def settle_orders(order_map, order_list, security_id) -> None: ...  # ✅ 已实现

# 辅助函数
def validate_input_df(df: pl.DataFrame) -> bool: ...
def get_processing_stats(...) -> Dict[str, Any]: ...
```

### 批量处理 (batch.py) ✅ Phase 3.2

```python
def process_daily_data(
    date: str,              # 日期 YYYYMMDD
    input_path: str,        # 输入 Parquet 路径
    output_path: str,       # 输出目录
    validate_output: bool = True,
    progress_callback: callable = None
) -> Dict[str, Any]:
    """
    处理单日全市场数据
    
    输出文件:
        - {date}_sh_order_data.parquet (委托表)
        - {date}_sh_trade_data.parquet (成交表)
    
    Returns:
        {
            'total_securities': int,     # 处理的股票数量
            'total_orders': int,         # 委托记录数
            'total_trades': int,         # 成交记录数
            'new_orders': int,           # 新增委托
            'cancel_orders': int,        # 撤单
            'taker_orders': int,         # Taker 数量
            'maker_orders': int,         # Maker 数量
            'processing_time_seconds': float,
            'output_files': List[str]
        }
    
    关键约束:
        1. 输出按 (SecurityID, TickTime, BizIndex) 排序
        2. IsAggressive 为 nullable bool
        3. 所有记录包含 SecurityID
    """

def check_bizindex_continuity(df: pl.DataFrame, security_id: str = None) -> Dict[str, Any]:
    """BizIndex 连续性检查，检测跳号"""

def get_output_file_paths(date: str, output_path: str) -> Tuple[str, str]:
    """获取输出文件路径"""

def validate_date_format(date: str) -> bool:
    """验证日期格式 YYYYMMDD"""
```

---

## 🔗 依赖关系

### 模块依赖图

```
sh_tick_reconstruction/
  ├── __init__.py          # 模块入口
  ├── models.py            # OrderContext 数据类 ✅
  ├── schema.py            # 输出 Schema 定义 ✅
  ├── time_filter.py       # 时间过滤函数 ✅
  ├── reconstructor.py     # 主函数框架 ✅
  ├── batch.py             # 批量处理入口 ✅ Phase 3.2
  └── tests/               # 单元测试
      ├── test_batch.py    # 17 个测试 ✅
      └── test_integration.py  # 9 个测试 ✅
```

### 外部依赖

| 包名 | 版本 | 用途 |
|------|------|------|
| polars | >=0.19.0 | 数据处理 |
| pyarrow | >=10.0.0 | Parquet 读写 |
| pandas | >=1.5.0 | 兼容性支持 |

---

## ⚠️ 注意事项

### 核心业务规则

1. **IsAggressive 只看"出生方式"**
   - True = 首次出现为 Type='T' (Taker)
   - False = 首次出现为 Type='A' (Maker)
   - None = 撤单记录（使用 Nullable Bool）

2. **只还原主动方，不处理被动方**
   - 被动方已有 Type='A' 记录
   - 如果错误还原被动方，会导致通道11/12像素值虚高一倍

3. **排序必须使用 (TickTime, BizIndex) 双重排序**
   - 同毫秒内可能有多笔委托

4. **上交所时间规则**
   - 上午连续竞价: 09:30-11:30
   - 下午连续竞价: 13:00-15:00 (无收盘集合竞价)
   - 注意：深交所下午到 14:57

### 已知限制

1. **OrderNO 跨日不唯一**: 需要按日期独立处理
2. **撤单 Price=0**: 需要从缓存或 last_price 回溯

---

## 📜 变更日志

### [2026-01-26] - Phase 3.1 完整主函数完成

**完成:**
- `reconstructor.py`: reconstruct_sh_tick_data() 主函数端到端可运行
  - 功能: 整合 Phase 1-2 所有组件，实现完整的数据拆解还原
  - 处理流程:
    1. 预处理: 剔除 Type='S', 时间过滤 (09:30-11:30, 13:00-15:00), 双重排序
    2. 逐行处理: Type='T'→process_trade, 'A'→process_add_order, 'D'→process_delete_order
    3. 批次结算: settle_orders()
    4. 输出排序: (TickTime, BizIndex)

- `test_integration.py`: 完整集成测试 (9 个测试场景)
  - 测试 1: 即时全部成交 (只有T，无A)
  - 测试 2: 部分成交后转挂单 (先T后A)
  - 测试 3: 纯挂单 (只有A)
  - 测试 4: 撤单价格回溯 (A后D，D的Price=0)
  - 测试 5: 时间过滤 (上交所规则: 09:30-11:30, 13:00-15:00)
  - 测试 6: Type='S' 产品状态记录剔除
  - 测试 7: 综合场景 - 多订单多类型
  - 测试 8: 排序正确性 (TickTime, BizIndex)
  - 测试 9: 所有记录包含 SecurityID

**符合验收标准:**
- ✅ 端到端可运行
- ✅ 排序逻辑正确 (TickTime, BizIndex)
- ✅ 所有输出记录包含 SecurityID
- ✅ 时间过滤正确 (上交所: 09:30-11:30, 13:00-15:00)
- ✅ Type='S' 产品状态记录被剔除
- ✅ IsAggressive 判定正确 (True/False/None)
- ✅ 所有测试通过

---

### [2026-01-26] - Phase 2.4 settle_orders 完成

**新增:**
- `reconstructor.py`: settle_orders() 函数完整实现
  - 功能: 批次结算所有缓存的委托
  - 核心逻辑:
    1. 遍历 order_map 中所有订单
    2. 计算原始委托量: Qty = trade_qty + resting_qty
    3. 确定委托价格: 优先 resting_price，否则用 trade_price
    4. 输出 OrdType='New' 的委托记录到 order_list
    5. 跳过零数量订单 (total_qty <= 0)

- `test_settle_orders.py`: 完整单元测试 (7 个测试场景)
  - 测试 1: 即时全部成交 (只有 trade_qty)
  - 测试 2: 部分成交后转挂单 (trade_qty + resting_qty)
  - 测试 3: 纯挂单 (只有 resting_qty)
  - 测试 4: 多订单批量结算
  - 测试 5: IsAggressive 正确传递 (True/False，不是 None)
  - 测试 6: 验证所有必需字段存在
  - 测试 7: 跳过零数量订单

**关键规则:**
- Qty = trade_qty + resting_qty (原始委托量)
- Price = resting_price if > 0 else trade_price (优先挂单价)
- BizIndex 使用首次出现的 (first_biz_index)
- IsAggressive 直接使用缓存值 (True=Taker, False=Maker)

**符合验收标准:**
- ✅ Qty = trade_qty + resting_qty
- ✅ BizIndex 是首次出现的
- ✅ IsAggressive 正确传递 (True/False，不是 None)
- ✅ SecurityID 正确填充
- ✅ 所有测试通过

---

### [2026-01-26] - Phase 2.3 process_delete_order 完成

**新增:**
- `reconstructor.py`: process_delete_order() 函数完整实现
  - 功能: 处理 Type='D' 撤单记录
  - 核心逻辑:
    1. 根据 TickBSFlag 确定订单号和方向
    2. 分级兜底策略获取撤单价格:
       - Level 0: 数据源自带的 Price (如果 > 0)
       - Level 1: 查本地缓存 order_map.get_price()
       - Level 2: 最终兜底用 last_price
    3. 输出撤单记录到 order_list

- `test_process_delete_order.py`: 完整单元测试
  - 测试 1: Level 0 - 数据源自带价格
  - 测试 2: Level 1 - 从缓存回溯价格
  - 测试 3: Level 2 - 兜底用 last_price
  - 测试 4: 部分成交后撤单
  - 测试 5: Price=0 vs Price=None 区别处理
  - 测试 6: 验证所有必需字段

**关键约束:**
- BizIndex 是撤单记录自身的（不是原委托的）
- IsAggressive 必须是 None（撤单不适用）
- 必须包含 SecurityID

**符合验收标准:**
- ✅ 价格回溯逻辑正确（三级兜底）
- ✅ IsAggressive = None
- ✅ SecurityID 字段存在
- ✅ 所有测试通过

---

### [2026-01-26] - Phase 2.2 process_add_order 完成

**新增:**
- `reconstructor.py`: process_add_order() 函数完整实现
  - 功能: 处理 Type='A' 挂单记录
  - 核心逻辑:
    1. 根据 TickBSFlag 确定订单号和方向 (B→BuyOrderNO, S→SellOrderNO)
    2. 判断订单是否已在缓存:
       - 已在缓存: 部分成交后转挂单，保持 is_aggressive=True，保持 first_biz_index
       - 不在缓存: 纯挂单，设置 is_aggressive=False（Maker）
    3. 累加 resting_qty，设置 resting_price
    4. 标记 has_resting=True

- `test_process_add_order.py`: 完整单元测试
  - 测试 1: 纯挂单 (is_aggressive=False)
  - 测试 2: 部分成交后转挂单 (先 Type='T' 后 Type='A'，保持 is_aggressive=True)
  - 测试 3: 卖单挂单
  - 测试 4: 同一订单多次挂单累加
  - 测试 5: get_total_qty() 正确性
  - 测试 6: get_price() 优先级（优先 resting_price）

**字段映射:**
- 输入: BuyOrderNO, SellOrderNO, Price, Qty, TickBSFlag
- 更新 order_map: resting_qty, resting_price, has_resting

**符合验收标准:**
- ✅ 已有缓存时保持 is_aggressive 不变
- ✅ 新订单 is_aggressive=False
- ✅ 正确累加 resting_qty
- ✅ 正确设置 has_resting=True
- ✅ 保持首次出现的 first_biz_index
- ✅ 所有测试通过

---

### [2026-01-26] - Phase 2.1 process_trade 完成

**新增:**
- `reconstructor.py`: process_trade() 函数完整实现
  - 功能: 处理 Type='T' 成交记录
  - 核心逻辑:
    1. ActiveSide 统一化映射: B→1 (主动买), S→2 (主动卖), N→0 (集合竞价)
    2. 输出成交记录到 trade_list (所有 Type='T' 都输出)
    3. 只还原主动方委托 (Taker)，被动方已有 Type='A' 记录
    4. 新订单: is_aggressive=True；已有订单: 累加 trade_qty
    5. 集合竞价 (TickBSFlag='N') 只输出成交，不还原委托

- `test_process_trade.py`: 完整单元测试
  - 测试 1: 主动买入 (TickBSFlag='B')
  - 测试 2: 主动卖出 (TickBSFlag='S')
  - 测试 3: 集合竞价 (TickBSFlag='N')
  - 测试 4: 同一订单多次成交累加
  - 测试 5: TradeMoney 为空时自动计算

**字段映射:**
- 输入: BuyOrderNO, SellOrderNO, Price, Qty, TradeMoney, TickBSFlag
- 输出 trade_list: SecurityID, BizIndex, TickTime, BidOrdID, AskOrdID, Price, Qty, TradeMoney, ActiveSide

**符合验收标准:**
- ✅ ActiveSide 映射正确: B→1, S→2, N→0
- ✅ 只还原主动方 (被动方已有 Type='A')
- ✅ 首次出现的 Taker 设置 is_aggressive=True
- ✅ 同一订单多次成交正确累加 trade_qty
- ✅ 集合竞价输出成交但不还原委托
- ✅ 所有测试通过

---

### [2026-01-26] - Phase 1.4 主函数框架完成

**新增:**
- `reconstructor.py`: 主函数框架模块
  - `reconstruct_sh_tick_data(df, security_id)`: 主函数入口
  - `process_trade()`: 成交处理 (Phase 2 占位)
  - `process_add_order()`: 挂单处理 (Phase 2 占位)
  - `process_delete_order()`: 撤单处理 (Phase 2 占位)
  - `settle_orders()`: 批次结算 (Phase 2 占位)
  - `validate_input_df()`: 输入验证
  - `get_processing_stats()`: 统计信息

**处理流程:**
1. 预处理: 剔除 Type='S', 时间过滤 (09:30-11:30, 13:00-15:00), 双重排序
2. 初始化: order_map, order_list, trade_list, last_price
3. 逐行处理: Type='T'/'A'/'D' 分发
4. 批次结算: settle_orders(order_map, order_list, security_id)
5. 排序输出: (TickTime, BizIndex)

**符合验收标准:**
- ✅ 函数签名: `reconstruct_sh_tick_data(df, security_id) -> Tuple[List, List]`
- ✅ settle_orders 接收 security_id 参数
- ✅ 完整类型注解和文档
- ✅ 占位函数已定义

---

### [2026-01-26] - Phase 3.2 批量处理入口完成

**新增:**
- `batch.py`: 批量处理入口模块
  - `process_daily_data()`: 处理单日全市场数据
    - 输出: `{date}_sh_order_data.parquet`, `{date}_sh_trade_data.parquet`
    - 自动按 (SecurityID, TickTime, BizIndex) 排序
    - 返回完整的处理统计信息
  - `check_bizindex_continuity()`: BizIndex 连续性检查
    - 检测跳号并返回详细信息
  - `get_output_file_paths()`: 获取输出文件路径
  - `validate_date_format()`: 日期格式验证

- `tests/test_batch.py`: 17 个单元测试
  - TestProcessDailyData: 8 个测试
    - 输出文件命名规范
    - 全市场排序 (SecurityID, TickTime, BizIndex)
    - IsAggressive nullable bool
    - SecurityID 字段存在
    - 统计信息准确性
    - 日期格式校验
    - 输入文件不存在处理
    - 进度回调
  - TestCheckBizindexContinuity: 5 个测试
    - 连续/不连续 BizIndex
    - 空 DataFrame / 单条记录
    - 缺少列异常
  - TestHelperFunctions: 3 个测试
    - 路径生成
    - 日期格式验证
  - TestBatchIntegration: 1 个端到端测试

**符合验收标准:**
- ✅ 输出文件命名: `{date}_sh_order_data.parquet` / `{date}_sh_trade_data.parquet`
- ✅ 全市场排序: (SecurityID, TickTime, BizIndex)
- ✅ IsAggressive 为 nullable bool
- ✅ 所有记录包含 SecurityID
- ✅ BizIndex 连续性检查功能
- ✅ 17 个测试全部通过

---

### [2026-01-26] - Phase 4.1 单元测试套件完成

**新增:**
- `tests/test_scenarios.py`: 7 个核心场景测试套件 (20 项测试)
  - TestScenario1ImmediateFullExecution: 即时全部成交 (2 项)
    - 买方主动 Taker
    - 卖方主动 Taker
  - TestScenario2PartialExecutionThenResting: 部分成交后转挂单 (2 项)
    - 单笔成交后剩余挂单
    - 多笔成交后剩余挂单
  - TestScenario3PureRestingOrder: 纯挂单 (2 项)
    - 买方 Maker
    - 卖方 Maker
  - TestScenario4PassiveOrderLaterExecuted: 被动单后续成交 (1 项)
    - 被动挂单被另一个主动单吃掉
  - TestScenario5CancelPriceBackfill: 撤单价格回溯 (3 项)
    - 撤单 Price=0 从缓存回溯
    - 撤单 BizIndex 是自身的
    - 撤单包含 SecurityID
  - TestScenario6TimeFilterSH: 时间过滤 (3 项)
    - 上午连续竞价
    - 下午连续竞价 (14:57 应保留!)
    - 数据过滤验证
  - TestScenario7ChannelMathRelationship: 通道数学关系 (2 项)
    - 买方: Ch7 = Ch9 + Ch11
    - 卖方: Ch8 = Ch10 + Ch12
  - TestEdgeCases: 边界情况 (4 项)
    - 空输入数据
    - Type='S' 剔除
    - Price=0 且无缓存
    - OrderNO=0 忽略
  - TestSecurityIDPresence: SecurityID 验证 (1 项)

**符合验收标准:**
- ✅ 所有 20 个测试通过
- ✅ 覆盖 7 个核心场景 + 边界情况
- ✅ 验证 SecurityID 字段存在
- ✅ 验证时间过滤正确 (上交所 14:57 应保留)
- ✅ 验证通道数学关系 (Ch7 = Ch9 + Ch11)

---

### [2026-01-26] - Phase 1.3 时间过滤函数完成

**新增:**
- `time_filter.py`: 上交所连续竞价时段过滤模块
  - `is_continuous_trading_time(tick_time)`: 核心过滤函数
  - `get_trading_session(tick_time)`: 获取时段名称
  - `parse_tick_time(tick_time)`: 解析 HHMMSSmmm 格式
  - `format_tick_time(tick_time)`: 格式化为可读字符串
  - 时间常量: MORNING_START/END, AFTERNOON_START/END

**符合验收标准:**
- ✅ is_continuous_trading_time(93000000) → True   (9:30 连续竞价开始)
- ✅ is_continuous_trading_time(92500000) → False  (9:25 开盘集合竞价)
- ✅ is_continuous_trading_time(145700000) → True  (14:57 上交所仍是连续竞价)
- ✅ is_continuous_trading_time(150000000) → False (15:00 收盘)
- ✅ is_continuous_trading_time(130000000) → True  (13:00 下午开始)

**沪深差异说明:**
- 上交所: 下午连续竞价 13:00-15:00 (无收盘集合竞价)
- 深交所: 下午连续竞价 13:00-14:57 (14:57-15:00 为收盘集合竞价)

---

### [2026-01-26] - Phase 1.2 输出 Schema 定义完成

**新增:**
- `schema.py`: 输出 Schema 定义模块
  - `SH_ORDER_SCHEMA_PYARROW`: 委托表 PyArrow Schema
  - `SH_TRADE_SCHEMA_PYARROW`: 成交表 PyArrow Schema
  - `SH_ORDER_SCHEMA_POLARS`: 委托表 Polars Schema
  - `SH_TRADE_SCHEMA_POLARS`: 成交表 Polars Schema
  - `validate_order_schema()`: 委托表 Schema 验证
  - `validate_trade_schema()`: 成交表 Schema 验证
  - `create_order_dataframe()`: 从记录列表创建委托 DataFrame
  - `create_trade_dataframe()`: 从记录列表创建成交 DataFrame
  - `write_order_parquet()`: 写入委托 Parquet
  - `write_trade_parquet()`: 写入成交 Parquet
  - `read_order_parquet()`: 读取委托 Parquet
  - `read_trade_parquet()`: 读取成交 Parquet

**符合验收标准:**
- ✅ PyArrow: `pa.field('IsAggressive', pa.bool_(), nullable=True)`
- ✅ 能正确写入和读取 Parquet
- ✅ SecurityID 字段存在且类型正确
- ✅ null 值在 Parquet 读写后保持

---

### [2026-01-26] - Phase 1.1 OrderContext 完成

**新增:**
- `models.py`: OrderContext 数据类
  - 包含所有必需字段（与需求文档 Section 5.1 完全一致）
  - 实现 4 个辅助方法
  - 完整的文档字符串和类型注解
  - PEP 8 规范
- `__init__.py`: 模块入口文件

**符合验收标准:**
- ✅ 代码可直接运行
- ✅ 包含完整文档字符串
- ✅ 字段与需求文档完全一致
- ✅ 符合 PEP 8 规范

---

### [2026-01-26] - 文件结构对齐 Plan 文档

**修改:**
- 移动测试文件到 `tests/` 目录:
  - `test_integration.py` → `tests/test_integration.py`
  - `test_settle_orders.py` → `tests/test_settle_orders.py`

**新增:**
- `scripts/` 目录及脚本:
  - `scripts/__init__.py`: 脚本模块入口
  - `scripts/run_daily.py`: 每日批量处理脚本（支持命令行参数）
  - `scripts/validate_output.py`: 输出验证脚本（Schema、排序、通道数学关系）

**更新 Plan 文档:**
- 修正测试场景数量: 6个 → 7个（与 Prompt 4.1 对齐）
- 更新文件交付清单以反映实际代码结构
- 标记 Phase 4 已完成的产出清单项

**当前文件结构:**
```
sh_tick_reconstruction/
├── __init__.py                # 模块入口
├── models.py                  # OrderContext 数据类
├── reconstructor.py           # 核心处理函数
├── schema.py                  # Schema 定义
├── time_filter.py             # 时间过滤
├── batch.py                   # 批量处理
├── agent.md                   # 进度追踪
├── tests/
│   ├── test_scenarios.py      # 7大场景测试
│   ├── test_batch.py          # 批量处理测试
│   ├── test_integration.py    # 集成测试
│   └── test_settle_orders.py  # settle_orders 测试
└── scripts/
    ├── run_daily.py           # 每日处理脚本
    └── validate_output.py     # 输出验证脚本
```

---

*文档结束*

