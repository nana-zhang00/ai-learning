---
name: id-fragmentation-investigation
description: 
  用户ID割裂排查与分析。当用户提到以下情况时触发：
  - 用户ID不一致、用户割裂、ID绑定失败
  - 用户表和事件表的#account_id/#distinct_id/#user_id异常
  - 用户在不同设备/账号间数据不贯通
  - 一个用户被识别成多个#user_id
  -用户注册前后的数据，不在一个#user_id下
  -用户序列只有注册事件，找不到其他触发事件
---

# ID 割裂排查 Skill

## 交互式排查入口

**首先，向用户确认以下关键信息：**

```
使用 AskUserQuestion 询问：

2. 问题现象：
   - 用户表中同一ID对应多个#user_id
   - 事件表中同一用户行为分散在多个#user_id
   - 登录后数据未与访客数据合并
   - userset设置的属性未关联到正确用户
3. 是否知道具体的项目ID和异常用户样本
```

根据回答，选择对应排查路径：

---
## 排查路径
step1: 查询事件表，#data_source的的分布情况，
确认上报方式：仅客户端上报 / 客户端+服务端双端上报 / 仅服务端上报

step2：
### 排查路径 A：双端上报场景

**适用条件**：客户端+服务端同时上报数据

#### A1：检查双端distinct_id一致性

```sql
-- 查找同一account_id下不同distinct_id的情况
SELECT 
  "#account_id",
  "#distinct_id",
  "#data_source",
  COUNT(*) as event_count
FROM ta.v_event_{project_id}
WHERE "#account_id" IS NOT NULL AND "#account_id" != ''
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
GROUP BY "#account_id", "#distinct_id", "#data_source"
HAVING COUNT(DISTINCT "#distinct_id") > 1
ORDER BY event_count DESC
LIMIT 50
```

**判断**：
- 同一#account_id，客户端和服务端的#distinct_id不同 → **双端distinct_id不一致（割裂）**
- 服务端事件#distinct_id为空 → **服务端未携带distinct_id（割裂）**

#### A2：检查注册/登录事件

```sql
-- 查找首条带account_id的事件（检查是否携带distinct_id）
SELECT 
  "#user_id",
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#event_time",
  "#data_source"
FROM ta.v_event_{project_id}
WHERE "#account_id" = '{目标account_id}'
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
ORDER BY "#event_time"
LIMIT 10
```

**判断**：首条带#account_id事件的#distinct_id为空 → **注册未携带distinct_id**

---

### 排查路径 B：仅客户端上报场景

**适用条件**：只有客户端SDK上报数据

### B1：检查用户表绑定情况

```sql
-- 检查用户表中distinct_id/account_id空值比例
SELECT 
  COUNT(*) as total_users,
  COUNT(CASE WHEN "#distinct_id" IS NULL OR "#distinct_id" = '' THEN 1 END) as did_empty,
  COUNT(CASE WHEN "#account_id" IS NULL OR "#account_id" = '' THEN 1 END) as aid_empty,
  ROUND(COUNT(CASE WHEN "#distinct_id" IS NULL OR "#distinct_id" = '' THEN 1 END) * 100.0 / COUNT(*), 2) as did_empty_pct
FROM ta.v_user_{project_id}
```

**判断**：#distinct_id空值比例 > 10% → 需排查userset入库问题

### B2：查找割裂样本

```sql
-- 查找同一account_id对应多个user_id
SELECT "#account_id", COUNT(DISTINCT "#user_id") as user_count,
       ARRAY_AGG(DISTINCT "#user_id") as user_ids
FROM ta.v_user_{project_id}
WHERE "#account_id" IS NOT NULL AND "#account_id" != ''
GROUP BY "#account_id"
HAVING COUNT(DISTINCT "#user_id") > 1
ORDER BY user_count DESC
LIMIT 20
```

---

### 排查路径 C：userset入库问题

**适用条件**：用户反映用户属性设置异常或绑定失败

#### C1：检查userset携带distinct_id情况

```sql
-- 查找userset事件中distinct_id为空的情况
SELECT 
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#event_time",
  "#data_source"
FROM ta.v_event_{project_id}
WHERE "#event_name" LIKE '%user_set%' OR "#event_name" LIKE '%user_setOnce%'
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
  AND ("#distinct_id" IS NULL OR "#distinct_id" = '')
ORDER BY "#event_time"
LIMIT 50
```

**判断**：userset事件#distinct_id为空 → **userset未携带distinct_id**

#### C2：检查入库顺序（Kafka）

```sql
SELECT 
  "#data_source",
  JSON_EXTRACT_SCALAR(str_json, '$._xxxxxtype') AS "#type",
  JSON_EXTRACT_SCALAR(str_json, '$._xxxxxaccount_id') AS "#account_id",
  JSON_EXTRACT_SCALAR(str_json, '$._xxxxxdistinct_id') AS "#distinct_id",
  "#server_time"
FROM kafka.ta.ta-data
WHERE _timestamp BETWEEN TIMESTAMP '{开始时间}' AND TIMESTAMP '{结束时间}'
  AND JSON_EXTRACT_SCALAR(str_json, '$._xxxxxaccount_id') = '{目标account_id}'
  AND JSON_EXTRACT_SCALAR(str_json, '$._xxxxxtype') IN ('user_set', 'user_setOnce', 'track')
ORDER BY "#server_time"
LIMIT 100
```

**判断**：userset入库时间早于track，且userset无#distinct_id → **入库顺序问题**

---

### 排查路径 E：已知ID查割裂

**适用条件**：用户提供#account_id或#distinct_id，需要判断是否存在ID割裂

**核心原理**：
- TE用户表中，#user_id与#account_id、#user_id与#distinct_id都是**一一对应**关系，用户表中不会出现"一个ID对应多个#user_id"的情况
- **割裂的本质**：account_id和distinct_id在入库时未能正确绑定（绑定时机不对），导致分别生成了不同的#user_id
- **割裂的表现**：**事件表中**同一个#account_id（或#distinct_id）的事件分散在多个不同的#user_id下

---

#### E1：检查事件表中user_id分布（判断是否割裂）

**目的**：查看该ID的事件是否分散在多个#user_id下，这是割裂的直接表现

```sql
-- 已知account_id，查询其事件分布到多少个user_id
SELECT 
  "#user_id",
  "#distinct_id",
  COUNT(*) as event_count,
  MIN("#event_time") as first_event,
  MAX("#event_time") as last_event
FROM ta.v_event_{project_id}
WHERE "#account_id" = '{目标account_id}'
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
GROUP BY "#user_id", "#distinct_id"
ORDER BY first_event

-- 已知distinct_id，查询其事件分布到多少个user_id
SELECT 
  "#user_id",
  "#account_id",
  COUNT(*) as event_count,
  MIN("#event_time") as first_event,
  MAX("#event_time") as last_event
FROM ta.v_event_{project_id}
WHERE "#distinct_id" = '{目标distinct_id}'
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
GROUP BY "#user_id", "#account_id"
ORDER BY first_event
```

**判断规则**：

| 结果 | 判断 |
|----------|------|
| 只有1个#user_id | **正常**：该ID所有事件归属同一用户，无割裂 |
| 有多个#user_id | **割裂**：该ID的事件分散在多个用户下，需执行E2分析原因 |

---

#### E2：分析事件序列定位割裂原因

**目的**：当E1发现多个#user_id时，分析事件序列找出割裂发生的时机和原因

```sql
-- 查询完整事件序列
SELECT 
  "#event_time",
  "#user_id",
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#data_source"
FROM ta.v_event_{project_id}
WHERE "#account_id" = '{目标account_id}'  -- 或 "#distinct_id" = '{目标distinct_id}'
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
ORDER BY "#event_time"
LIMIT 100
```

**对照典型割裂场景判断原因**：

| 场景 | 事件序列特征 | 原因说明 |
|------|--------------|----------|
| 新设备注册未携带distinct_id | (null,d1,u1) → (a1,null,u2) | 注册时#account_id未带#distinct_id，系统新建u2 |
| userset先入库无distinct_id | track(d1,u1) → userset(a1,null,u2) → track(a1,d1,u2) | userset入库时#distinct_id为空，新建u2 |
| 双端distinct_id不一致 | (null,d1,u1) → (a1,d2,u2) | 客户端用d1，服务端用d2，分别创建u1和u2 |

**正常场景对照**：

| 场景 | 事件序列特征 | 说明 |
|------|--------------|------|
| 新设备正常注册 | (null,d1,u1) → (a1,d1,u1) | 注册时携带了d1，共用u1 |
| 新机老账号 | (a1,d1,u1) → (null,d2,u2) → (a1,d2,u1) | 老账号a1在新设备d2登录，d2绑定到a1的u1 |
| 老机新账号 | (a1,d1,u1) → (a2,d1,u2) | d1已绑定a1，新账号a2无法再绑定d1，新建u2（正常） |

---

#### E3：检查用户表绑定关系（辅助确认）

**目的**：查看割裂后各user_id的最终绑定状态

```sql
SELECT "#user_id", "#account_id", "#distinct_id"
FROM ta.v_user_{project_id}
WHERE "#user_id" IN ({E1查出的多个user_id})
```

**说明**：用户表反映最终绑定关系。割裂场景中：
- 注册未带distinct_id：u2的#distinct_id为空
- 双端不一致：u1和u2分别绑定了不同的distinct_id

---

#### 执行流程

```
已知account_id或distinct_id
    │
    ▼
E1：查事件表user_id分布
    │
    ├─ 只有1个user_id → 正常，无割裂
    │
    └─ 有多个user_id → 割裂！执行E2
         │
         ▼
    E2：分析事件序列，对照典型割裂场景
         │
         ├─ (null,d1,u1) → (a1,null,u2) → 注册未携带distinct_id
         ├─ userset(a1,null)早于track → userset入库顺序问题
         └─ d1→d2变化 → 双端distinct_id不一致
         │
         ▼
    E3（可选）：查用户表确认绑定状态
```

---

### 排查路径 D：已知具体异常用户

**适用条件**：用户提供了具体的#user_id/#account_id/#distinct_id，需要分析完整行为序列

直接查询该用户的行为序列：

```sql
SELECT 
  "#user_id",
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#event_time",
  "#data_source"
FROM ta.v_event_{project_id}
WHERE 
  ("#distinct_id" = '{目标distinct_id}' OR "#account_id" = '{目标account_id}')
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
ORDER BY "#event_time"
LIMIT 100
```

**对照典型场景表判断**：

| 时间序列特征 | 结论 |
|--------------|------|
| (null,d1,u1) → (a1,d1,u1) | 正常绑定 |
| (null,d1,u1) → (a1,null,u2) | **割裂**：注册未带d1 |
| (null,d1,u1) → (a1,d2,u2) | **割裂**：双端d1≠d2 |
| track(d1) → userset(a1,null) → track(a1,d1) | **割裂**：userset先入库无d1 |
| (a1,d1,u1) → (a2,d1,u2) | 正常：d1已绑a1 |
| (a1,d1,u1) → (null,d2,u2) → (a1,d2,u1) | 正常：a1绑新设备 |

---

## 核心概念速查

| 字段 | 说明 | 来源 |
|------|------|------|
| `#account_id` | 账户ID（登录态） | 客户端调用login()设置 |
| `#distinct_id` | 访客ID（未登录态） | SDK自动生成或identify()设置 |
| `#user_id` | TE系统用户唯一ID | 系统根据ID关联规则生成 |

**核心规则**：存在#account_id时，优先按#account_id关联#user_id；#account_id为空时，才按#distinct_id关联。

---

## 解决方案速查

| 问题类型 | 解决方案 |
|----------|----------|
| 双端distinct_id不一致 | 服务端上报必须携带客户端#distinct_id |
| 注册未带distinct_id | 确保login()调用时SDK已初始化 |
| userset未带distinct_id | 确保userset调用携带distinct_id |
| userset入库顺序问题 | 调整上报顺序，track先于userset |
| 历史数据修复 | 联系客户成功经理 |

---

## MCP工具调用示例

```json
{
  "projectId": {project_id},
  "modelType": "sql",
  "qp": "{\"eventView\":{},\"events\":{\"sql\":\"SELECT ... FROM ta.v_user_{project_id} ...\"}}"
}
```

---

## 排查流程图

```
开始排查
    │
    ▼
询问上报方式和已知信息 ────────────────────────┐
    │                                          │
    ├─ 已知account_id/distinct_id → 路径E：查ID割裂
    │     │
    │     ├─ E1查事件表user_id分布 → 只有1个 → 正常
    │     └─ E1查事件表user_id分布 → 多个 → 割裂！
    │            │
    │            ▼
    │       E2分析事件序列 → 定位割裂原因
    │
    ├─ 双端上报 → 路径A：检查distinct_id一致性
    │              │
    │              ├─ 不一致 → 服务端需携带客户端distinct_id
    │              └─ 为空 → 服务端未携带distinct_id
    │
    ├─ 仅客户端 → 路径B：检查用户表绑定
    │              │
    │              ├─ 空值比例高 → 路径C：检查userset
    │              └─ 查找割裂样本 → 分析行为序列
    │
    └─ 已知异常用户 → 路径D：直接查行为序列
    │
    ▼
对照典型场景表 → 给出解决方案
```

---

## 执行排查时的交互流程

1. **确认场景**：使用AskUserQuestion询问上报方式和问题现象
2. **选择路径**：根据回答选择A/B/C/D路径
3. **执行SQL**：调用mcp__te-mcp-analysis__query_adhoc执行对应SQL
4. **解读结果**：对照典型场景表判断割裂类型
5. **给出方案**：从解决方案速查表给出对应建议
