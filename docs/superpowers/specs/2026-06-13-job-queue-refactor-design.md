# Channel Stream Regex Assigner 任务队列重构设计

**日期：** 2026-06-13  
**作者：** 风宝  
**状态：** 设计阶段

---

## 问题背景

当前插件的任务互斥机制存在以下问题：

1. **任务互斥过于严格**：使用单一全局锁，任务状态过期时间为 6 小时，如果任务卡住会长时间阻塞所有后续任务
2. **状态清理不完善**：任务完成后状态文件可能残留，仍被判断为"活跃"，导致新任务被永久拦截
3. **缺少调试信息**：用户看不到任务被拦截的具体原因，不知道是哪个任务在运行
4. **M3U 刷新后自动执行失败**：由于上述问题，M3U 刷新事件触发的自动任务经常被阻塞，无法执行

---

## 设计目标

1. **智能任务替换**：新的 M3U 刷新任务可以替换队列中旧的相同类型任务，避免积压
2. **健壮的状态管理**：任务状态清晰，自动清理过期任务，防止永久阻塞
3. **更好的用户体验**：用户能看到当前运行的任务和队列中等待的任务
4. **向后兼容**：不破坏现有 API 和前端交互逻辑

---

## 整体架构

### 核心组件

**1. 任务队列管理器**

- 维护一个轻量级任务队列，包含两个槽位：
  - `running`：当前正在执行的任务（最多 1 个）
  - `pending`：等待执行的任务（最多 1 个）
- 存储在 `job_queue.json` 文件中（与 `job_state.json` 并列）

**队列数据结构：**

```json
{
  "running": {
    "job_id": "uuid",
    "job_name": "apply_match",
    "started_at": "2026-06-13 10:00:00",
    "updated_at_ts": 1234567890.123
  },
  "pending": {
    "job_id": "uuid",
    "job_name": "auto_m3u_refresh",
    "queued_at": "2026-06-13 10:05:00",
    "delay_seconds": 180,
    "kwargs": {...}
  }
}
```

**2. 任务类型定义**

插件支持以下任务类型：
- `preview_match`：预览规则匹配
- `apply_match`：立即执行规则匹配
- `sort_existing_streams`：重排已挂流
- `scan_m3u_epg`：扫描 M3U 头部 EPG
- `auto_m3u_refresh`：M3U 刷新后自动执行（包含 EPG 扫描 + 频道挂流）

**任务替换规则：**
- `auto_m3u_refresh` 可以替换 `auto_m3u_refresh`
- `apply_match` 可以替换 `preview_match`（因为 apply 包含 preview 的效果）
- `apply_match` 可以替换 `apply_match`
- `preview_match` 可以替换 `preview_match`
- `sort_existing_streams` 可以替换 `sort_existing_streams`
- `scan_m3u_epg` 可以替换 `scan_m3u_epg`
- 其他情况不允许替换（返回"队列已满"错误）

---

## 任务调度逻辑

### 提交新任务

**流程：**

1. 获取队列锁
2. 读取当前队列状态
3. 判断队列状态：
   - **情况 A：队列为空（无 running，无 pending）**
     - 立即启动新任务，放入 `running` 槽位
   - **情况 B：有 running，无 pending**
     - 新任务放入 `pending` 槽位
   - **情况 C：有 running，有 pending**
     - 检查新任务是否可以替换 `pending` 任务（根据任务替换规则）
     - 如果可以替换：用新任务替换 `pending`，记录日志
     - 如果不可以替换：拒绝新任务，返回"队列已满，请稍后再试"
4. 写入队列状态
5. 释放队列锁
6. 如果新任务被立即启动，启动后台线程

**返回值：**
```python
{
    "status": "queued" | "blocked",
    "message": "...",
    "job_id": "uuid",
    "job": {...},
    "queue_position": "running" | "pending",
    "replaced_job_id": "uuid"  # 如果替换了旧任务
}
```

### 任务完成时的队列轮转

**流程：**

1. 任务完成（`completed` / `failed`）
2. 获取队列锁
3. 清空 `running` 槽位
4. 检查 `pending` 槽位：
   - 如果有 `pending` 任务：
     - 将其移到 `running` 槽位
     - 更新 `started_at` 时间戳
     - 写入队列状态
     - 释放队列锁
     - 启动后台线程执行该任务
   - 如果没有 `pending` 任务：
     - 队列变为空闲
     - 写入空队列状态
5. 释放队列锁

---

## 状态管理与清理

### 任务状态生命周期

```
[提交] → waiting (在 pending 队列中)
       ↓
[启动] → running (移到 running 槽位，启动后台线程)
       ↓
[完成] → completed / failed (写入最终状态，触发队列轮转)
```

### 状态文件职责

- **`job_queue.json`**（新增）：存储队列结构，只包含 `running` 和 `pending` 两个槽位
- **`job_state.json`**（保留）：存储当前 `running` 任务的详细进度信息（供前端查询）
- **`last_result.json`**（保留）：存储最近一次任务的最终结果

### 清理机制

**立即清理：**
- 任务完成（`completed` / `failed`）时，立即从队列的 `running` 槽位移除
- 队列自动轮转：`pending` → `running`

**过期清理（防御性措施）：**
- **Running 任务过期**：如果 `running` 任务超过 **30 分钟** 未更新 `updated_at_ts` → 标记为 `stale`
- **Pending 任务过期**：如果 `pending` 任务在队列中等待超过 **2 小时** → 自动删除
- **过期检测时机**：
  - 新任务提交时检测
  - 用户查询任务状态时检测
  - 检测到 `stale` 任务自动清理，记录警告日志

**心跳机制：**
- 后台任务每次调用 `progress_callback` 时自动更新 `updated_at_ts`
- 现有的 `_write_progress` 函数已经在做这个，不需要额外改动

### 过期时间常量

```python
RUNNING_JOB_TIMEOUT_SECONDS = 30 * 60  # 30 分钟
PENDING_JOB_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 小时
```

---

## 代码改动范围

### 新增函数

1. **`_read_job_queue(plugin_dir: str) -> Dict[str, Any]`**
   - 读取 `job_queue.json` 文件
   - 返回队列状态（包含 `running` 和 `pending`）

2. **`_write_job_queue(plugin_dir: str, queue: Dict[str, Any]) -> None`**
   - 写入 `job_queue.json` 文件
   - 使用原子写入（类似 `_write_json_file_atomic`）

3. **`_clean_stale_jobs(plugin_dir: str, queue: Dict[str, Any]) -> Dict[str, Any]`**
   - 检测并清理过期任务
   - 返回清理后的队列状态

4. **`_can_replace_job(new_job_name: str, old_job_name: str) -> bool`**
   - 判断新任务是否可以替换旧任务
   - 实现任务替换规则

5. **`_enqueue_job(plugin_dir: str, job_name: str, delay_seconds: int, **kwargs) -> Dict[str, Any]`**
   - 提交新任务到队列
   - 返回提交结果（包含队列位置、是否替换了旧任务）

6. **`_dequeue_and_start_next(plugin_dir: str, logger=None) -> None`**
   - 从 `pending` 取出下一个任务并启动
   - 在任务完成时调用

### 修改函数

1. **`_start_background_job(...)`**
   - 替换现有的 `_reserve_background_job` 逻辑
   - 调用 `_enqueue_job` 提交任务
   - 根据队列位置决定是否立即启动后台线程

2. **`read_active_job_state(plugin_dir: str)`**
   - 增强逻辑：同时检查 `job_queue.json` 和 `job_state.json`
   - 返回队列信息（当前运行任务 + 等待任务）

3. **后台任务 runner 函数（在 `_start_background_job` 内部）**
   - 任务完成后调用 `_dequeue_and_start_next` 触发队列轮转

### 删除函数

1. **`_reserve_background_job(...)`** → 替换为 `_enqueue_job`
2. **`_read_active_job_state_unlocked(...)`** → 合并到 `_read_job_queue`

---

## 用户可见的变化

### 任务提交响应

**原有响应：**
```json
{
  "status": "queued" | "blocked",
  "message": "任务已提交后台，任务 ID：xxx",
  "task_id": "uuid",
  "active_job": {...}
}
```

**新增字段：**
```json
{
  "status": "queued" | "blocked",
  "message": "任务已提交后台，任务 ID：xxx",
  "task_id": "uuid",
  "queue_position": "running" | "pending",  // 新增
  "replaced_job_id": "uuid",  // 新增：如果替换了旧任务
  "active_job": {...}
}
```

### 查看结果响应

**原有响应：**
```json
{
  "status": "ok",
  "message": "已有后台任务正在执行",
  "file": "...",
  "summary": {...},
  "active_job": {...}
}
```

**新增字段：**
```json
{
  "status": "ok",
  "message": "已有后台任务正在执行",
  "file": "...",
  "summary": {...},
  "active_job": {...},
  "queue": {  // 新增
    "running": {...},
    "pending": {...}
  }
}
```

---

## 错误处理

### 队列已满

**触发条件：** 有 `running` + 有 `pending`，且新任务无法替换 `pending` 任务

**返回：**
```json
{
  "status": "blocked",
  "message": "队列已满：当前正在执行【立即执行规则匹配】任务，队列中等待【重排已挂流】任务。新的【扫描 M3U EPG】任务无法提交，请稍后再试。",
  "queue": {
    "running": {"job_name": "apply_match", ...},
    "pending": {"job_name": "sort_existing_streams", ...}
  }
}
```

### 任务过期警告

**触发条件：** 检测到 `stale` 任务

**日志记录：**
```
WARNING: Job {job_id} ({job_name}) has been running for over 30 minutes without heartbeat, marked as stale and removed.
```

**用户可见消息：**
- 自动清理，不返回错误
- 在下次查询任务状态时，会看到"上一个任务已过期清理"的提示

---

## 向后兼容性

### API 兼容性

- 保持所有现有的 `action` 接口不变
- 新增字段不影响前端解析（前端可以忽略不认识的字段）
- 如果前端未升级，仍然可以正常工作（只是看不到队列信息）

### 数据文件迁移

**首次运行时：**
- 如果 `job_queue.json` 不存在，自动创建空队列
- 如果 `job_state.json` 存在且包含活跃任务，将其导入到 `job_queue.json` 的 `running` 槽位

**降级兼容：**
- 如果回退到旧版本插件，`job_queue.json` 会被忽略
- `job_state.json` 仍然存在，旧版本可以正常工作

---

## 测试场景

### 场景 1：M3U 刷新事件快速连续触发

**步骤：**
1. 手动触发 M3U 刷新（触发事件 A）
2. 延迟 1 分钟后再次触发 M3U 刷新（触发事件 B）
3. 延迟 1 分钟后再次触发 M3U 刷新（触发事件 C）

**预期结果：**
- 事件 A 立即执行
- 事件 B 放入 `pending` 队列
- 事件 C 替换 `pending` 队列中的事件 B
- 事件 A 完成后，事件 C 自动启动（事件 B 被跳过）

### 场景 2：任务卡住后自动恢复

**步骤：**
1. 启动一个任务，人为模拟卡住（停止心跳更新）
2. 等待 30 分钟
3. 提交新任务

**预期结果：**
- 新任务检测到旧任务已过期
- 自动清理旧任务
- 新任务立即启动

### 场景 3：队列已满被拒绝

**步骤：**
1. 启动"立即执行规则匹配"任务（进入 `running`）
2. 提交"重排已挂流"任务（进入 `pending`）
3. 提交"扫描 M3U EPG"任务

**预期结果：**
- 第三个任务被拒绝（无法替换"重排已挂流"）
- 返回明确的错误消息，说明队列状态

### 场景 4：Apply 任务替换 Preview 任务

**步骤：**
1. 启动"重排已挂流"任务（进入 `running`）
2. 提交"预览规则匹配"任务（进入 `pending`）
3. 提交"立即执行规则匹配"任务

**预期结果：**
- "立即执行"任务替换 `pending` 中的"预览"任务
- 返回消息说明已替换

---

## 实现优先级

### Phase 1：核心队列逻辑（必须）
- 实现队列读写函数
- 实现任务入队逻辑
- 实现队列轮转逻辑
- 实现任务替换规则

### Phase 2：状态清理（必须）
- 实现过期检测逻辑
- 实现自动清理机制
- 添加警告日志

### Phase 3：用户体验优化（推荐）
- 增强错误消息（显示队列状态）
- 查询结果中返回队列信息
- 添加调试日志

### Phase 4：数据迁移（可选）
- 从旧版本 `job_state.json` 导入活跃任务
- 添加迁移日志

---

## 风险与缓解

### 风险 1：队列文件损坏

**缓解措施：**
- 使用原子写入（`os.replace`）
- 读取失败时自动重建空队列
- 记录错误日志

### 风险 2：并发竞争条件

**缓解措施：**
- 使用文件锁（`JOB_STATE_LOCK`）保护队列读写
- 所有队列操作都在锁保护下进行

### 风险 3：任务替换逻辑错误

**缓解措施：**
- 明确定义任务替换规则表
- 增加单元测试覆盖所有替换场景
- 记录详细日志

---

## 未来扩展可能

1. **优先级队列**：为不同任务类型设置优先级，高优先级任务可以插队
2. **多槽位队列**：允许多个任务并行执行（需要考虑数据库并发冲突）
3. **任务取消**：用户手动取消队列中的 `pending` 任务
4. **任务重试**：失败任务自动重试（带指数退避）
5. **任务历史**：保留最近 N 个任务的执行记录

---

## 总结

本设计通过引入轻量级任务队列（1 running + 1 pending）和智能任务替换机制，解决了当前插件的任务互斥和状态清理问题。核心改动集中在任务调度逻辑，向后兼容现有 API，用户体验显著提升。

**关键收益：**
- ✅ M3U 刷新事件不再丢失（总会执行最新的一次）
- ✅ 任务不会永久阻塞（自动清理过期任务）
- ✅ 用户能看到队列状态（调试友好）
- ✅ 代码改动可控（基于现有架构增强）
