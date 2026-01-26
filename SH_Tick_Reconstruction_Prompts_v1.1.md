# 上交所逐笔数据拆解工程 - AI 提示词手册

> **配套文档**: SH_Tick_Data_Reconstruction_Spec v1.8 + Plan v1.1  
> **使用方式**: 将本提示词 + 数据字典 + 需求文档 + Plan 一起喂给 AI

---

## 📖 使用指南

### 喂给 AI 的文档组合

每次与 AI 对话时，按以下顺序提供文档：

```
1. L2_data_dictionary.md                    # 数据字典（完整字段定义）
2. SH_Tick_Data_Reconstruction_Spec_v1.8.md # 需求文档（业务规则）
3. SH_Tick_Reconstruction_Plan_v1.1.md      # 落地计划（进度跟踪）
4. 当前阶段提示词（从本文档复制）
```

### 提示词结构说明

每个提示词包含：
- **角色设定**: AI 的专业背景
- **背景信息**: 项目上下文（已在需求文档中，此处简化引用）
- **任务目标**: 本阶段具体任务
- **输入/输出规格**: 明确的数据格式
- **约束条件**: 必须遵守的规则
- **验收标准**: 如何判断完成

---

## 🚀 Phase 1 提示词

### Prompt 1.1: OrderContext 数据结构

```markdown
## 角色
你是一个高频交易系统的后端开发专家，精通 Python、Pandas/Polars 和金融市场微观结构。

## 任务
基于需求文档 Section 5（OrderMap缓存设计），实现 `OrderContext` 数据类。

## 要求
1. 使用 Python dataclass，包含类型注解
2. 必须包含以下字段（与需求文档完全一致）：
   - order_no: int           # 订单号
   - side: str               # 'B' 或 'S'
   - first_time: int         # 首次出现时间 (TickTime)
   - first_biz_index: int    # 首次出现的 BizIndex
   - trade_qty: int = 0      # 累计成交量
   - resting_qty: int = 0    # 挂单量
   - trade_price: float = 0  # 最新成交价
   - resting_price: float = 0  # 挂单价
   - is_aggressive: bool = False  # ⭐ 入场进攻性（默认 False）
   - has_resting: bool = False    # ⭐ 是否有挂单记录

3. 实现辅助方法：
   - add_trade_qty(qty: int)  # 累加成交量
   - add_resting_qty(qty: int)  # 累加挂单量
   - get_price() -> float  # 获取委托价格（优先 resting_price）
   - get_total_qty() -> int  # 获取原始委托总量

## 验收标准
- 代码可直接运行
- 包含完整文档字符串
- 字段与需求文档 Section 5.1 完全一致
- 符合 PEP 8 规范
```

---

### Prompt 1.2: 输出 Schema 定义

```markdown
## 角色
你是一个数据工程师，精通 Polars/PyArrow 和 Parquet 格式。

## 任务
定义 sh_order_data 和 sh_trade_data 的输出 Schema。

## sh_order_data Schema（注意：必须包含 SecurityID）
| 字段 | 类型 | 说明 |
|------|------|------|
| SecurityID | str | ⭐ 证券代码（全市场输出必需） |
| BizIndex | int | 首次出现的逐笔序号 |
| TickTime | int | 委托时间 |
| OrdID | int | 委托单号 |
| OrdType | str | 'New' 或 'Cancel' |
| Side | str | 'B' 或 'S' |
| Price | float | 委托价格 |
| Qty | int | 原始委托量 |
| IsAggressive | bool (nullable) | True/False/None |

## sh_trade_data Schema
| 字段 | 类型 | 说明 |
|------|------|------|
| SecurityID | str | 证券代码 |
| BizIndex | int | 逐笔序号 |
| TickTime | int | 成交时间 |
| BidOrdID | int | 买单号 |
| AskOrdID | int | 卖单号 |
| Price | float | 成交价 |
| Qty | int | 成交量 |
| TradeMoney | float | 成交金额 |
| ActiveSide | int | 1=主动买, 2=主动卖, 0=集合竞价 |

## 要求
1. 使用 Polars 或 PyArrow 定义 Schema
2. IsAggressive 必须是 nullable boolean
3. 两张表都必须包含 SecurityID 字段
4. 提供 Schema 验证函数

## 验收标准
- PyArrow: `pa.field('IsAggressive', pa.bool_(), nullable=True)`
- 能正确写入和读取 Parquet
- SecurityID 字段存在且类型正确
```

---

### Prompt 1.3: 时间过滤函数

```markdown
## 角色
你是一个金融数据处理专家。

## 任务
实现 `is_continuous_trading_time()` 函数，判断是否为**上交所**连续竞价时段。

## 时间规则（⚠️ 注意：上交所无收盘集合竞价）

| 时间段 | 阶段 | 处理 |
|--------|------|------|
| 09:15 - 09:25 | 开盘集合竞价 | ❌ 剔除 |
| 09:25 - 09:30 | 静默期 | ❌ 剔除 |
| 09:30 - 11:30 | 连续竞价(上午) | ✅ 保留 |
| 11:30 - 13:00 | 午间休市 | ❌ 剔除 |
| 13:00 - 15:00 | 连续竞价(下午) | ✅ 保留 |

**🔴 关键差异**：
- 上交所下午连续竞价到 **15:00**（无收盘集合竞价）
- 深交所下午连续竞价到 14:57（有收盘集合竞价 14:57-15:00）

## 函数签名
```python
def is_continuous_trading_time(tick_time: int) -> bool:
    """
    判断是否为上交所连续竞价时段
    
    Args:
        tick_time: HHMMSSmmm 格式的时间，如 93000540
    
    Returns:
        True if 连续竞价时段
    
    Note:
        上交所无收盘集合竞价，下午连续竞价延续到 15:00
    """
```

## 验收标准
- is_continuous_trading_time(93000000) → True   # 9:30
- is_continuous_trading_time(92500000) → False  # 9:25 开盘集合竞价
- is_continuous_trading_time(145700000) → True  # 14:57 ⭐ 上交所仍是连续竞价
- is_continuous_trading_time(150000000) → False # 15:00 收盘
- is_continuous_trading_time(130000000) → True  # 13:00
```

---

### Prompt 1.4: 主函数框架

```markdown
## 角色
你是一个系统架构师。

## 任务
搭建 `reconstruct_sh_tick_data()` 主函数框架（空壳）。

## 函数签名
```python
def reconstruct_sh_tick_data(
    df: pl.DataFrame, 
    security_id: str
) -> Tuple[List[dict], List[dict]]:
    """
    上交所逐笔数据拆解还原主函数
    
    Args:
        df: 单只股票当日的 auction_tick_merged_data
        security_id: 证券代码
    
    Returns:
        order_list: 还原后的委托列表（每条记录含 SecurityID）
        trade_list: 标准化成交列表（每条记录含 SecurityID）
    """
```

## 处理流程（伪代码）
```
1. 预处理
   - 剔除 Type='S'
   - 时间过滤（上交所：09:30-11:30, 13:00-15:00）
   - 按 (TickTime, BizIndex) 排序

2. 初始化
   - order_map = {}
   - order_list = []
   - trade_list = []
   - last_price = 0

3. 逐行处理
   - Type='T' → process_trade()
   - Type='A' → process_add_order()
   - Type='D' → process_delete_order()

4. 批次结算
   - settle_orders(order_map, order_list, security_id)

5. 排序输出
   - 按 (TickTime, BizIndex) 排序
```

## 要求
- 暂时用 pass 或 placeholder 代替未实现的函数
- 包含完整的类型注解和文档
- 确保 security_id 传递给 settle_orders
```

---

## 🔧 Phase 2 提示词

### Prompt 2.1: process_trade 函数

```markdown
## 角色
你是一个高频交易系统开发专家。

## 任务
实现 `process_trade()` 函数，处理 Type='T' 的成交记录。

## 函数签名
```python
def process_trade(
    row: dict, 
    order_map: Dict[int, OrderContext], 
    trade_list: List[dict]
) -> None:
```

## 核心逻辑

### 1. ActiveSide 统一化
```python
if row['TickBSFlag'] == 'B':
    active_side = 1  # 主动买入
    active_order_no = row['BuyOrderNO']
    side = 'B'
elif row['TickBSFlag'] == 'S':
    active_side = 2  # 主动卖出
    active_order_no = row['SellOrderNO']
    side = 'S'
else:  # 'N' 集合竞价
    active_side = 0
    return  # 跳过集合竞价
```

### 2. 输出到成交表（⭐ 包含 SecurityID）
```python
trade_list.append({
    'SecurityID': row['SecurityID'],  # ⭐ 必须包含
    'BizIndex': row['BizIndex'],
    'TickTime': row['TickTime'],
    'BidOrdID': row['BuyOrderNO'],
    'AskOrdID': row['SellOrderNO'],
    'Price': row['Price'],
    'Qty': row['Qty'],
    'TradeMoney': row['TradeMoney'],
    'ActiveSide': active_side
})
```

### 3. 只还原主动方（关键！）
- 被动方已有 Type='A' 记录，不要重复还原
- 如果 active_order_no 不在 order_map → 新建 OrderContext(is_aggressive=True)
- 如果 active_order_no 已在 order_map → 只累加 trade_qty

## 验收标准
- 只处理主动方
- ActiveSide 正确映射
- 新订单 is_aggressive=True
- trade_list 每条记录包含 SecurityID
```

---

### Prompt 2.2: process_add_order 函数

```markdown
## 角色
你是一个高频交易系统开发专家。

## 任务
实现 `process_add_order()` 函数，处理 Type='A' 的挂单记录。

## 函数签名
```python
def process_add_order(
    row: dict, 
    order_map: Dict[int, OrderContext]
) -> None:
```

## 核心逻辑

### 上交所规则
> 如果有成交，Type='T' 先到，Type='A' 后到

### 判断逻辑
```python
# 确定订单号和方向
if row['TickBSFlag'] == 'B':
    order_no = row['BuyOrderNO']
    side = 'B'
else:  # 'S'
    order_no = row['SellOrderNO']
    side = 'S'

if order_no in order_map:
    # 情况1: 已有成交记录（部分成交后转挂单）
    # ⭐ is_aggressive 保持 True（因为它先主动吃单了）
    # ⭐ first_biz_index 保持不变
    order_map[order_no].add_resting_qty(row['Qty'])
    order_map[order_no].resting_price = row['Price']
    order_map[order_no].has_resting = True  # ⭐ 设置 has_resting
else:
    # 情况2: 纯挂单，没有成交
    # ⭐ is_aggressive = False（被动等待成交）
    order_map[order_no] = OrderContext(
        order_no=order_no,
        side=side,
        first_time=row['TickTime'],
        first_biz_index=row['BizIndex'],
        is_aggressive=False  # 关键！默认值就是 False
    )
    order_map[order_no].add_resting_qty(row['Qty'])
    order_map[order_no].resting_price = row['Price']
    order_map[order_no].has_resting = True  # ⭐ 设置 has_resting
```

## 验收标准
- 已有缓存时保持 is_aggressive 不变
- 新订单 is_aggressive=False
- 正确累加 resting_qty
- 正确设置 has_resting=True
```

---

### Prompt 2.3: process_delete_order 函数

```markdown
## 角色
你是一个高频交易系统开发专家。

## 任务
实现 `process_delete_order()` 函数，处理 Type='D' 的撤单记录。

## 函数签名
```python
def process_delete_order(
    row: dict, 
    order_map: Dict[int, OrderContext], 
    order_list: List[dict], 
    last_price: float
) -> None:
```

## 核心逻辑

### 分级兜底策略（获取撤单价格）
```python
# Level 0: 数据源自带的 Price（如果 > 0）
if row['Price'] is not None and row['Price'] > 0:
    cancel_price = row['Price']

# Level 1: 查本地缓存
elif order_no in order_map:
    cancel_price = order_map[order_no].get_price()

# Level 2: 最终兜底 - 用最新成交价
else:
    cancel_price = last_price
    # 记录告警日志
```

### 输出撤单记录（⭐ 包含 SecurityID）
```python
order_list.append({
    'SecurityID': row['SecurityID'],  # ⭐ 必须包含
    'BizIndex': row['BizIndex'],  # ⭐ 撤单记录自身的 BizIndex
    'TickTime': row['TickTime'],
    'OrdID': order_no,
    'OrdType': 'Cancel',
    'Side': side,
    'Price': cancel_price,
    'Qty': row['Qty'],
    'IsAggressive': None  # ⭐ 撤单不适用，填 None
})
```

## 关键约束
- BizIndex 是撤单记录自身的（不是原委托的）
- IsAggressive 必须是 None（不是 False）
- 必须包含 SecurityID

## 验收标准
- 价格回溯逻辑正确
- IsAggressive = None
- SecurityID 字段存在
```

---

### Prompt 2.4: settle_orders 函数

```markdown
## 角色
你是一个高频交易系统开发专家。

## 任务
实现 `settle_orders()` 函数，批次结算所有缓存的委托。

## 函数签名
```python
def settle_orders(
    order_map: Dict[int, OrderContext], 
    order_list: List[dict],
    security_id: str  # ⭐ 新增参数
) -> None:
```

## 核心逻辑
```python
for order_no, ctx in order_map.items():
    # 计算原始委托量
    total_qty = ctx.trade_qty + ctx.resting_qty
    
    # 确定委托价格（优先挂单价）
    price = ctx.resting_price if ctx.resting_price > 0 else ctx.trade_price
    
    order_list.append({
        'SecurityID': security_id,  # ⭐ 添加 SecurityID
        'BizIndex': ctx.first_biz_index,  # ⭐ 首次出现的
        'TickTime': ctx.first_time,
        'OrdID': order_no,
        'OrdType': 'New',
        'Side': ctx.side,
        'Price': price,
        'Qty': total_qty,
        'IsAggressive': ctx.is_aggressive  # True 或 False
    })
```

## 验收标准
- Qty = trade_qty + resting_qty
- BizIndex 是首次出现的
- IsAggressive 正确传递（True/False，不是 None）
- SecurityID 正确填充
```

---

## 📦 Phase 3 提示词

### Prompt 3.1: 完整主函数

```markdown
## 角色
你是一个系统架构师。

## 任务
整合 Phase 1-2 的所有组件，实现完整的 `reconstruct_sh_tick_data()` 函数。

## 要求
1. 预处理流程：
   - 剔除 Type='S'
   - 时间过滤（上交所：09:30-11:30, 13:00-15:00）⭐ 注意是 15:00 不是 14:57
   - 按 (TickTime, BizIndex) 排序（⭐ 必须双重排序）

2. 逐行处理：
   - 调用 process_trade/add_order/delete_order
   - 更新 last_price

3. 批次结算：
   - 调用 settle_orders(order_map, order_list, security_id)

4. 输出排序：
   - 按 (TickTime, BizIndex) 排序

## 验收标准
- 端到端可运行
- 排序逻辑正确
- 所有输出记录包含 SecurityID
```

---

### Prompt 3.2: 批量处理入口

```markdown
## 角色
你是一个数据工程师。

## 任务
实现 `process_daily_data()` 函数，处理单日全市场数据。

## 函数签名
```python
def process_daily_data(
    date: str, 
    input_path: str, 
    output_path: str
) -> None:
```

## 处理流程
```python
1. 读取数据
   df = pl.read_parquet(input_path)

2. 按股票分组处理
   for security_id in df['SecurityID'].unique():
       group_df = df.filter(pl.col('SecurityID') == security_id)
       orders, trades = reconstruct_sh_tick_data(group_df, security_id)
       all_orders.extend(orders)
       all_trades.extend(trades)

3. 全市场排序（⭐ 关键）
   orders_df.sort(['SecurityID', 'TickTime', 'BizIndex'])
   trades_df.sort(['SecurityID', 'TickTime', 'BizIndex'])

4. 输出 Parquet（⭐ 文件命名）
   orders_df.write_parquet(f"{output_path}/{date}_sh_order_data.parquet")
   trades_df.write_parquet(f"{output_path}/{date}_sh_trade_data.parquet")
```

## 验收标准
- 输出文件命名：`{date}_sh_order_data.parquet` / `{date}_sh_trade_data.parquet`
- 物理排序正确
- IsAggressive 类型为 nullable bool
- 所有记录包含 SecurityID
```

---

## ✅ Phase 4 提示词

### Prompt 4.1: 单元测试套件

```markdown
## 角色
你是一个测试工程师。

## 任务
为上交所逐笔数据拆解系统编写 pytest 测试套件。

## 必须覆盖的 6 个场景

### 场景1: 即时全部成交
```python
def test_immediate_full_execution():
    """只有T记录，无A记录"""
    input_data = [
        {'Type': 'T', 'TickBSFlag': 'B', 'BuyOrderNO': 1001, 
         'SellOrderNO': 2001, 'Qty': 1000, 'Price': 10.0, 
         'SecurityID': '600519', ...}
    ]
    orders, trades = reconstruct_sh_tick_data(...)
    
    assert len(orders) == 1
    assert orders[0]['OrdType'] == 'New'
    assert orders[0]['IsAggressive'] == True
    assert orders[0]['Qty'] == 1000
    assert orders[0]['SecurityID'] == '600519'  # ⭐ 验证 SecurityID
```

### 场景2: 部分成交后转挂单
```python
def test_partial_execution_then_resting():
    """先T后A"""
    input_data = [
        {'Type': 'T', 'TickBSFlag': 'B', 'BuyOrderNO': 1002, 'Qty': 600, ...},
        {'Type': 'A', 'TickBSFlag': 'B', 'BuyOrderNO': 1002, 'Qty': 400, ...}
    ]
    orders, _ = reconstruct_sh_tick_data(...)
    
    assert orders[0]['IsAggressive'] == True
    assert orders[0]['Qty'] == 1000  # 600 + 400
```

### 场景3: 纯挂单
```python
def test_pure_resting_order():
    """只有A记录"""
    input_data = [
        {'Type': 'A', 'TickBSFlag': 'S', 'SellOrderNO': 2001, 'Qty': 500, ...}
    ]
    orders, _ = reconstruct_sh_tick_data(...)
    
    assert orders[0]['IsAggressive'] == False
```

### 场景4: 被动单后续成交
```python
def test_passive_order_later_executed():
    """首次A，后续作为被动方成交"""
    # 注意：被动方的成交不会产生新的委托记录
    # 只有 A 记录会产生委托
```

### 场景5: 撤单价格回溯和 IsAggressive=None
```python
def test_cancel_price_backfill():
    """撤单的 Price=0，需要从缓存回溯"""
    input_data = [
        {'Type': 'A', 'TickBSFlag': 'B', 'BuyOrderNO': 1004, 
         'Price': 10.50, 'Qty': 1000, ...},
        {'Type': 'D', 'TickBSFlag': 'B', 'BuyOrderNO': 1004, 
         'Price': 0, 'Qty': 500, ...}  # Price=0
    ]
    orders, _ = reconstruct_sh_tick_data(...)
    
    cancel_order = [o for o in orders if o['OrdType'] == 'Cancel'][0]
    assert cancel_order['Price'] == 10.50  # 从缓存回溯
    assert cancel_order['IsAggressive'] is None  # ⭐ 撤单填 None
```

### 场景6: 时间过滤（上交所 14:57 应保留）
```python
def test_time_filter_sh():
    """上交所 14:57 仍是连续竞价，应保留"""
    assert is_continuous_trading_time(145700000) == True  # 14:57
    assert is_continuous_trading_time(150000000) == False  # 15:00
```

### 场景7: 通道数学关系
```python
def test_channel_math_relationship():
    """Ch7 = Ch9 + Ch11"""
    # 统计各通道的记录数/金额，验证数学关系
```

## 验收标准
- 所有测试通过
- 覆盖边界情况
- 验证 SecurityID 字段存在
- 验证时间过滤正确（上交所 vs 深交所差异）
```

---

## 📊 辅助提示词

### 调试提示词

```markdown
## 问题描述
[描述你遇到的问题]

## 当前代码
[粘贴相关代码]

## 期望行为
[描述期望的结果]

## 实际行为
[描述实际的结果]

## 请帮我排查原因并修复
```

---

### 代码审查提示词

```markdown
## 角色
你是一个代码审查专家。

## 任务
审查以下代码，检查是否符合需求文档 v1.8 的约束。

## 检查清单
1. 排序是否用 (TickTime, BizIndex)？
2. IsAggressive 判定是否只看首次出现类型？
3. 撤单 IsAggressive 是否为 None？
4. 是否只还原主动方？
5. Parquet 输出是否正确处理 nullable bool？
6. ⭐ 所有输出记录是否包含 SecurityID？
7. ⭐ 时间过滤是否正确（上交所：13:00-15:00）？
8. ⭐ OrderContext 是否包含 has_resting 字段？

## 代码
[粘贴代码]
```

---

## 🔴 v1.8 更新要点速查

| 更新项 | v1.7 | v1.8 |
|--------|------|------|
| derived_sh_orders Schema | 无 SecurityID | ⭐ **包含 SecurityID** |
| 上交所下午连续竞价 | 13:00-14:57 | ⭐ **13:00-15:00** |
| OrderContext.has_resting | 可选 | ⭐ **必需** |
| 落盘文件命名 | 未明确 | ⭐ **{date}_sh_order_data.parquet** |

---

*提示词手册结束*
