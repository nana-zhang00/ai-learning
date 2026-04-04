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
1.目前排查目的是什么？                                          
选项A：目前未发现异常，想整体排查是否存在ID割裂问题         
选项B：已经发现数据异常，需要排查具体问题
  B1.继续提问，目前遇到的问题现象：
   - 用户表中部分#user_id/#account_id，没有绑定#distinct_id
   - 事件表中同一用户行为分散在多个#user_id
   - 登录后数据未与访客数据归属到同一个用户
    - 更多，请详细描述
  B2. 是否知道具体的异常用户的#account_id和#distinct_id,请提供
```

根据回答，选择对应排查路径：

---
## 排查路径 A
step1: 查询user表，检查用户表绑定情况

```sql
-- 检查用户表中distinct_id/account_id空值比例
SELECT 
  COUNT(*) as total_users,
  COUNT(CASE WHEN "#distinct_id" IS NULL OR "#distinct_id" = '' THEN 1 END) as did_empty,
  COUNT(CASE WHEN "#account_id" IS NULL OR "#account_id" = '' THEN 1 END) as aid_empty,
  ROUND(COUNT(CASE WHEN "#distinct_id" IS NULL OR "#distinct_id" = '' THEN 1 END) * 100.0 / COUNT(*), 2) as did_empty_pct
FROM ta.v_user_{project_id}
```

**判断**：#distinct_id空值比例 > 10% ->可能存在异常，需进一步排查。#distinct_id空值率=0，不存在ID割裂；#distinct_id空值率>0,这一步不给是否割裂的结论，均需进一步排查.



step2: 查询事件表，确认所有事件数据中#data_source的的分布情况，
确认上报方式：仅客户端上报 / 客户端+服务端双端上报 / 仅服务端上报

step3：
### 排查路径

#### A1：检查是否上报注册事件，若有则使用注册事件进行以下步骤排查，若无则使用登录事件。
检查step1中#account_id有值但#distinct_id无值的用户，注册（登录）事件中#distinct_id是否有值。
```sql
-- 查找首条带#account_id的事件（检查是否携带#distinct_id）
SELECT 
  "#user_id",
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#event_time",
  "#data_source"
FROM ta.v_event_{project_id} 
WHERE "#account_id" = '{目标account_id}'
 AND ("$part_event" LIKE '%register%' OR "$part_event" LIKE '%login%')
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
ORDER BY "#event_time"

```
**依次判断**：
-1.若注册（登录）事件的#distinct_id全都无值，
  1.1大概率会导致ID割裂，除非注册（登录）时userset的#account_id和#distinct_id一起出现并先于注册（登录）事件先入库；
  → **注册（登录）未携带distinct_id（割裂）**

-2.若注册（登录）事件，#distinct_id有值。
 2.1 但用户注册/登录后的5分钟内，#distinct_id分别在客户端和服务端事件数据中，是两个不一样的值。 同一#account_id，客户端和服务端的#distinct_id不同； 
  → **双端distinct_id不一致（割裂）**

--2.2 查询该#distinct_id在user表中已经与别的#account_id和#user_id绑定了，并且该#user_id注册时间早于该#account_id；
  → **#distinct_id已于老#account_id绑定，老设备注册新账号（正常）**

--2.3 #distinct_id在user表中没有与别的#account_id绑定,则排查注册（登录）userset时#distinct_id无值。
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
  AND JSON_EXTRACT_SCALAR(str_json, '$._xxxxxtype') IN ('user_set', 'user_setOnce')
ORDER BY "#server_time"
LIMIT 100
```
  →**判断**：userset未携带#distinct_id → **userset未携带先于注册（登录）事件入库，导致id割裂（割裂）**


---

### 排查路径 B：已知ID查割裂

**适用条件**：用户提供#account_id或#distinct_id，需要判断是否存在ID割裂

**核心原理**：
- TE用户表中，#user_id与#account_id、#user_id与#distinct_id都是**一一对应**关系，用户表中不会出现"一个ID对应多个#user_id"的情况
- **割裂的本质**：account_id和distinct_id在入库时未能正确绑定，导致分别生成了不同的#user_id
- **割裂的表现**：**事件表中**同一个用户的事件分散在多个不同的#user_id下

---

#### B1：用户仅提供了#account_id，查询该#account_id的事件数据
-注册事件/首条登录事件，#distinct_id为空。->确认割裂，注册/登录事件未上报#distinct_id导致的ID割裂
-注册事件/首条登录事件，#distinct_id有值。
 --查询#distinct_id的用户表数据，#distinct_id在user表中已经与别的#account_id绑定了。->正常。老设备注册新账号，不算割裂。


#### B2：用户仅提供了#distinct_id，查询该#distinct_id的事件数据和用户表数据

**目的**：查看该distinct_id的事件是否分散在多个#user_id下，以及确认用户表中与之绑定的#account_id和#user_id

**判断规则**：

| 结果 | 判断 |
|----------|------|
| 事件表中只有1个#user_id | **正常**：该ID所有事件归属同一用户，无割裂 |
| 事件中有多个#user_id，但是#distinct_id在用户表中与其他#account_id绑定 | **正常**：该ID的事件分散在多个用户下，但是绑定的是最早注册的#account_id |
| 事件中有多个#user_id，但是#distinct_id在用户表中未与其他#account_id绑定 | **割裂**：该ID的事件分散在多个用户下，可能是因为注册时未上报#distinct_id |

---

#### 用户同时提供了#distinct_id 和 #account_id

直接查询该用户的行为序列：

```sql
SELECT 
  "#user_id",
  "#account_id",
  "#distinct_id",
  "#event_name",
  "#event_time",
  "#device_id",
  "#data_source"
FROM ta.v_event_{project_id}
WHERE 
  ("#distinct_id" = '{目标distinct_id}' OR "#account_id" = '{目标account_id}')
  AND "$part_date" BETWEEN '{开始日期}' AND '{结束日期}'
ORDER BY "#event_time"
LIMIT 100
```
**判断规则**：

| 结果 | 判断 |
|----------|------|
| #distinct_id 和 #account_id 分别对应两个#user_id，且#account_id在用户表中与别的#distinct_id绑定 | **正常**：新设备登录老号，登录后行为归属老账号 |
| #distinct_id 和 #account_id 分别对应两个#user_id，且#account_id在用户表中未与别的#distinct_id，#distinct_id在用户表中也未与其他#account_id绑定 | **割裂**：注册时/登录时，未成功绑定#account_id和#distinct_id |
|  #distinct_id 和 #account_id 分别对应两个#user_id，#distinct_id在用户表中已与其他#account_id绑定 | **正常**：老设备注册新账号 |


---

#### b3：检查用户表绑定关系（辅助确认）

**目的**：查看割裂后各user_id的最终绑定状态

```sql
SELECT "#user_id", "#account_id", "#distinct_id"
FROM ta.v_user_{project_id}
WHERE "#user_id" IN ({E1查出的多个user_id})
```

**说明**：用户表反映最终绑定关系。割裂场景中：
- 注册未带distinct_id：u2的#distinct_id为空
- 双端不一致：u1和u2分别绑定了不同的distinct_id

----

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

## 执行排查时的交互流程

1. **确认场景**：使用AskUserQuestion询问上报方式和问题现象
2. **选择路径**：根据回答选择A/B/C/D路径
3. **执行SQL**：调用mcp__te-mcp-analysis__query_adhoc执行对应SQL
4. **解读结果**：对照典型场景表判断割裂类型
5. **给出方案**：从解决方案速查表给出对应建议
