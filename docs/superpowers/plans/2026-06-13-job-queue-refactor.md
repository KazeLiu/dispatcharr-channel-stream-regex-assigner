# 任务队列重构实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重构插件任务调度机制，实现任务队列 + 智能替换 + 完善日志，解决 M3U 刷新后自动执行失败问题

**Architecture:** 引入双槽位任务队列（1 running + 1 pending），支持同类型任务替换，自动清理过期任务，完整的日志记录覆盖事件接收、任务提交、队列轮转全流程

**Tech Stack:** Python 3, threading, json, uuid, Django ORM

---

## Phase 1: 核心队列逻辑

### Task 1: 添加队列常量和锁

**Files:**
- Modify: `plugin.py:15-34`

- [ ] **Step 1: 添加队列相关常量**

在现有常量区域（line 15-34）后添加：

```python
JOB_QUEUE_FILE = "job_queue.json"
RUNNING_JOB_TIMEOUT_SECONDS = 30 * 60  # 30 分钟
PENDING_JOB_TIMEOUT_SECONDS = 2 * 60 * 60  # 2 小时
JOB_QUEUE_LOCK = threading.Lock()
```

- [ ] **Step 2: 验证常量添加正确**

检查 `plugin.py` 文件，确认常量已添加在 `JOB_STATE_LOCK` 之后

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 添加任务队列常量和锁"
```

---

### Task 2: 实现队列文件路径函数

**Files:**
- Modify: `plugin.py` (在 `_job_state_path` 函数之后添加)

- [ ] **Step 1: 添加队列文件路径函数**

在 `_job_state_path` 函数（line 401-402）之后添加：

```python
def _job_queue_path(plugin_dir: str) -> str:
    return os.path.join(_exports_dir(plugin_dir), JOB_QUEUE_FILE)
```

- [ ] **Step 2: 验证函数位置正确**

检查函数添加在 `_job_state_path` 之后，`_read_job_state_unlocked` 之前

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 添加队列文件路径函数"
```

---

### Task 3: 实现队列读取函数

**Files:**
- Modify: `plugin.py` (在 `_job_queue_path` 函数之后添加)

- [ ] **Step 1: 添加队列读取函数**

```python
def _read_job_queue(plugin_dir: str) -> Dict[str, Any]:
    """读取任务队列文件，返回队列状态（包含 running 和 pending）"""
    path = _job_queue_path(plugin_dir)
    if not os.path.isfile(path):
        return {"running": None, "pending": None}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {"running": None, "pending": None}
        return {
            "running": data.get("running"),
            "pending": data.get("pending"),
        }
    except (OSError, json.JSONDecodeError):
        return {"running": None, "pending": None}
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现队列读取函数"
```

---

### Task 4: 实现队列写入函数

**Files:**
- Modify: `plugin.py` (在 `_read_job_queue` 函数之后添加)

- [ ] **Step 1: 添加队列写入函数**

```python
def _write_job_queue(plugin_dir: str, queue: Dict[str, Any]) -> None:
    """写入任务队列文件，使用原子写入"""
    path = _job_queue_path(plugin_dir)
    payload = {
        "running": queue.get("running"),
        "pending": queue.get("pending"),
        "updated_at": _now_label(),
    }
    _write_json_file_atomic(path, payload, lock=JOB_QUEUE_LOCK)
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现队列写入函数"
```

---

### Task 5: 实现任务替换规则函数

**Files:**
- Modify: `plugin.py` (在 `_write_job_queue` 函数之后添加)

- [ ] **Step 1: 添加任务替换规则函数**

```python
def _can_replace_job(new_job_name: str, old_job_name: str) -> bool:
    """判断新任务是否可以替换旧任务"""
    if new_job_name == old_job_name:
        return True
    if new_job_name == "apply_match" and old_job_name == "preview_match":
        return True
    if new_job_name == "auto_m3u_refresh" and old_job_name == "auto_m3u_refresh":
        return True
    return False
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现任务替换规则函数"
```

---

### Task 6: 实现过期任务清理函数

**Files:**
- Modify: `plugin.py` (在 `_can_replace_job` 函数之后添加)

- [ ] **Step 1: 添加过期任务清理函数**

```python
def _clean_stale_jobs(plugin_dir: str, queue: Dict[str, Any], logger=None) -> Dict[str, Any]:
    """检测并清理过期任务，返回清理后的队列状态"""
    cleaned_queue = dict(queue)
    now_ts = time.time()
    
    # 清理过期的 running 任务
    running = queue.get("running")
    if running:
        updated_at_ts = float(running.get("updated_at_ts") or 0)
        if now_ts - updated_at_ts > RUNNING_JOB_TIMEOUT_SECONDS:
            if logger:
                logger.warning(
                    "%s: Stale running job detected and removed, job_id=%s, job_name=%s, "
                    "last_update=%s, stale_seconds=%d",
                    PLUGIN_KEY,
                    running.get("job_id"),
                    running.get("job_name"),
                    running.get("started_at"),
                    int(now_ts - updated_at_ts)
                )
            cleaned_queue["running"] = None
    
    # 清理过期的 pending 任务
    pending = queue.get("pending")
    if pending:
        queued_at = pending.get("queued_at")
        if queued_at:
            try:
                queued_at_ts = time.mktime(time.strptime(queued_at, "%Y-%m-%d %H:%M:%S"))
                if now_ts - queued_at_ts > PENDING_JOB_TIMEOUT_SECONDS:
                    if logger:
                        logger.warning(
                            "%s: Stale pending job detected and removed, job_id=%s, job_name=%s, "
                            "queued_at=%s, wait_seconds=%d",
                            PLUGIN_KEY,
                            pending.get("job_id"),
                            pending.get("job_name"),
                            queued_at,
                            int(now_ts - queued_at_ts)
                        )
                    cleaned_queue["pending"] = None
            except (ValueError, TypeError):
                pass
    
    return cleaned_queue
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现过期任务清理函数"
```

---

### Task 7: 实现任务入队函数

**Files:**
- Modify: `plugin.py` (在 `_clean_stale_jobs` 函数之后添加)

- [ ] **Step 1: 添加任务入队函数（Part 1 - 函数签名和初始化）**

```python
def _enqueue_job(
    plugin_dir: str,
    job_name: str,
    delay_seconds: int,
    logger=None,
    **kwargs
) -> Dict[str, Any]:
    """提交新任务到队列，返回提交结果"""
    job_id = str(uuid.uuid4())
    now_label = _now_label()
    
    with JOB_QUEUE_LOCK:
        # 读取并清理队列
        queue = _read_job_queue(plugin_dir)
        queue = _clean_stale_jobs(plugin_dir, queue, logger=logger)
        
        running = queue.get("running")
        pending = queue.get("pending")
```

- [ ] **Step 2: 添加任务入队函数（Part 2 - 队列逻辑）**

在上一步代码后继续添加：

```python
        # 情况 A：队列为空
        if not running and not pending:
            job_payload = {
                "job_id": job_id,
                "job_name": job_name,
                "started_at": now_label,
                "updated_at_ts": time.time(),
                "delay_seconds": delay_seconds,
                "kwargs": kwargs,
            }
            queue["running"] = job_payload
            _write_job_queue(plugin_dir, queue)
            
            if logger:
                logger.info(
                    "%s: Job started immediately, job_id=%s, job_name=%s",
                    PLUGIN_KEY,
                    job_id,
                    job_name
                )
            
            return {
                "status": "queued",
                "job_id": job_id,
                "queue_position": "running",
                "job": job_payload,
            }
```

- [ ] **Step 3: 添加任务入队函数（Part 3 - pending 逻辑）**

继续添加：

```python
        # 情况 B：有 running，无 pending
        if running and not pending:
            job_payload = {
                "job_id": job_id,
                "job_name": job_name,
                "queued_at": now_label,
                "delay_seconds": delay_seconds,
                "kwargs": kwargs,
            }
            queue["pending"] = job_payload
            _write_job_queue(plugin_dir, queue)
            
            if logger:
                logger.info(
                    "%s: Job queued as pending, job_id=%s, job_name=%s",
                    PLUGIN_KEY,
                    job_id,
                    job_name
                )
            
            return {
                "status": "queued",
                "job_id": job_id,
                "queue_position": "pending",
                "job": job_payload,
            }
```

- [ ] **Step 4: 添加任务入队函数（Part 4 - 替换逻辑）**

继续添加：

```python
        # 情况 C：有 running，有 pending
        if running and pending:
            # 检查是否可以替换
            if _can_replace_job(job_name, pending.get("job_name")):
                old_job_id = pending.get("job_id")
                old_job_name = pending.get("job_name")
                
                job_payload = {
                    "job_id": job_id,
                    "job_name": job_name,
                    "queued_at": now_label,
                    "delay_seconds": delay_seconds,
                    "kwargs": kwargs,
                }
                queue["pending"] = job_payload
                _write_job_queue(plugin_dir, queue)
                
                if logger:
                    logger.warning(
                        "%s: Pending job replaced, old_job_id=%s, old_job_name=%s, "
                        "new_job_id=%s, new_job_name=%s",
                        PLUGIN_KEY,
                        old_job_id,
                        old_job_name,
                        job_id,
                        job_name
                    )
                
                return {
                    "status": "queued",
                    "job_id": job_id,
                    "queue_position": "pending",
                    "job": job_payload,
                    "replaced_job_id": old_job_id,
                }
```

- [ ] **Step 5: 添加任务入队函数（Part 5 - 拒绝逻辑）**

继续添加并闭合函数：

```python
            # 无法替换，拒绝任务
            if logger:
                logger.warning(
                    "%s: Job rejected (queue full), job_name=%s, running=%s, pending=%s",
                    PLUGIN_KEY,
                    job_name,
                    running.get("job_name"),
                    pending.get("job_name")
                )
            
            return {
                "status": "blocked",
                "message": (
                    f"队列已满：当前正在执行【{_job_display_name(running.get('job_name'))}】任务，"
                    f"队列中等待【{_job_display_name(pending.get('job_name'))}】任务。"
                    f"新的【{_job_display_name(job_name)}】任务无法提交，请稍后再试。"
                ),
                "queue": {
                    "running": running,
                    "pending": pending,
                },
            }
        
        # 不应该到达这里
        return {
            "status": "error",
            "message": "Unexpected queue state",
        }
```

- [ ] **Step 6: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 7: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现任务入队函数"
```

---

### Task 8: 实现队列轮转函数

**Files:**
- Modify: `plugin.py` (在 `_enqueue_job` 函数之后添加)

- [ ] **Step 1: 添加队列轮转函数**

```python
def _dequeue_and_start_next(plugin_dir: str, completed_job_id: str, logger=None) -> None:
    """从 pending 取出下一个任务并启动"""
    with JOB_QUEUE_LOCK:
        queue = _read_job_queue(plugin_dir)
        
        # 清空 running 槽位
        queue["running"] = None
        
        pending = queue.get("pending")
        if not pending:
            # 队列变为空闲
            _write_job_queue(plugin_dir, queue)
            if logger:
                logger.info(
                    "%s: Queue is now idle, completed_job_id=%s",
                    PLUGIN_KEY,
                    completed_job_id
                )
            return
        
        # 将 pending 移到 running
        next_job_id = pending.get("job_id")
        next_job_name = pending.get("job_name")
        delay_seconds = pending.get("delay_seconds", 0)
        kwargs = pending.get("kwargs", {})
        
        running_payload = {
            "job_id": next_job_id,
            "job_name": next_job_name,
            "started_at": _now_label(),
            "updated_at_ts": time.time(),
            "delay_seconds": delay_seconds,
            "kwargs": kwargs,
        }
        queue["running"] = running_payload
        queue["pending"] = None
        _write_job_queue(plugin_dir, queue)
        
        if logger:
            logger.info(
                "%s: Queue rotation triggered, completed_job_id=%s, next_job_id=%s, next_job_name=%s",
                PLUGIN_KEY,
                completed_job_id,
                next_job_id,
                next_job_name
            )
    
    # 释放锁后启动后台线程
    _start_job_runner(plugin_dir, next_job_id, next_job_name, delay_seconds, logger, **kwargs)
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现队列轮转函数"
```

---

## Phase 2: 重构现有任务调度

### Task 9: 提取后台任务 runner 逻辑

**Files:**
- Modify: `plugin.py` (在 `_dequeue_and_start_next` 函数之后添加)

- [ ] **Step 1: 添加 _start_job_runner 函数（Part 1 - 函数签名）**

```python
def _start_job_runner(
    plugin_dir: str,
    job_id: str,
    job_name: str,
    delay_seconds: int,
    logger=None,
    **kwargs
) -> None:
    """启动后台任务线程"""
    def runner():
        try:
            from django.db import close_old_connections
            close_old_connections()
        except Exception:
            pass
        
        if delay_seconds > 0:
            time.sleep(delay_seconds)
            running_state = _job_state_payload(
                job_id,
                job_name,
                "running",
                f"{_job_display_name(job_name)}任务正在后台运行。任务 ID：{job_id}",
            )
            _write_job_state(plugin_dir, running_state)
            _write_last_result(plugin_dir, running_state)
```

- [ ] **Step 2: 添加 _start_job_runner 函数（Part 2 - 执行逻辑）**

继续添加：

```python
        try:
            progress_log_state = {"last_at": 0.0, "last_message": ""}
            
            def progress_callback(current, total, message, extra=None):
                payload = _write_progress(
                    plugin_dir,
                    job_id,
                    job_name,
                    current,
                    total,
                    message,
                    extra=extra,
                )
                _log_progress(logger, payload, progress_log_state)
            
            # 根据 job_name 调用对应的任务函数
            target_func = _get_job_target_function(job_name)
            if not target_func:
                raise ValueError(f"Unknown job name: {job_name}")
            
            result = target_func(progress_callback=progress_callback, **kwargs)
```

- [ ] **Step 3: 添加 _start_job_runner 函数（Part 3 - 完成处理）**

继续添加：

```python
            if isinstance(result, dict):
                completed = dict(result)
                completed["job_id"] = job_id
                completed["job_name"] = job_name
                completed["finished_at"] = _now_label()
                completed["status"] = completed.get("status") or "ok"
                _write_job_state(plugin_dir, completed)
                _write_last_result(plugin_dir, completed)
                if logger:
                    logger.info(
                        "%s: Job completed successfully, job_id=%s, job_name=%s, summary=%s",
                        PLUGIN_KEY,
                        job_id,
                        job_name,
                        json.dumps(completed.get("message", ""), ensure_ascii=False)
                    )
            else:
                completed = {
                    "status": "ok",
                    "message": f"{_job_display_name(job_name)}任务完成。任务 ID：{job_id}",
                    "job_id": job_id,
                    "job_name": job_name,
                    "finished_at": _now_label(),
                }
                _write_job_state(plugin_dir, completed)
                _write_last_result(plugin_dir, completed)
                if logger:
                    logger.info(
                        "%s: Job completed successfully, job_id=%s, job_name=%s",
                        PLUGIN_KEY,
                        job_id,
                        job_name
                    )
```

- [ ] **Step 4: 添加 _start_job_runner 函数（Part 4 - 错误处理和轮转）**

继续添加并闭合函数：

```python
        except Exception as exc:
            if logger:
                logger.error(
                    "%s: Job failed, job_id=%s, job_name=%s, error=%s",
                    PLUGIN_KEY,
                    job_id,
                    job_name,
                    str(exc)
                )
                logger.exception("%s background job %s failed", PLUGIN_KEY, job_id)
            try:
                failed = {
                    "status": "error",
                    "message": f"{job_name} 后台任务失败：{exc}",
                    "job_id": job_id,
                    "job_name": job_name,
                    "finished_at": _now_label(),
                }
                _write_job_state(plugin_dir, failed)
                _write_last_result(plugin_dir, failed)
            except Exception:
                if logger:
                    logger.exception("%s failed to write error report", PLUGIN_KEY)
        finally:
            # 触发队列轮转
            _dequeue_and_start_next(plugin_dir, job_id, logger=logger)
            try:
                from django.db import close_old_connections
                close_old_connections()
            except Exception:
                pass
    
    thread = threading.Thread(
        target=runner,
        name=f"{PLUGIN_KEY}-{job_name}-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
```

- [ ] **Step 5: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 6: Commit**

```bash
git add plugin.py
git commit -m "feat: 提取后台任务 runner 逻辑"
```

---

### Task 10: 实现任务目标函数映射

**Files:**
- Modify: `plugin.py` (在 `_start_job_runner` 函数之后添加)

- [ ] **Step 1: 添加任务目标函数映射**

```python
def _get_job_target_function(job_name: str):
    """根据任务名称返回对应的执行函数"""
    job_targets = {
        "preview_match": lambda **kw: run_channel_stream_regex_job(dry_run=True, **kw),
        "apply_match": lambda **kw: run_channel_stream_regex_job(dry_run=False, **kw),
        "sort_existing_streams": sort_existing_channel_streams,
        "scan_m3u_epg": import_m3u_epg_sources,
        "auto_m3u_refresh": handle_m3u_refresh_job,
    }
    return job_targets.get(job_name)
```

- [ ] **Step 2: 验证函数语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 实现任务目标函数映射"
```

---

### Task 11: 重构 _start_background_job 函数

**Files:**
- Modify: `plugin.py:213-330` (替换现有 `_start_background_job` 函数)

- [ ] **Step 1: 备份原函数逻辑**

```bash
# 查看当前函数内容
sed -n '213,330p' plugin.py > /tmp/old_start_background_job.txt
```

- [ ] **Step 2: 替换为新的 _start_background_job 函数**

找到 `def _start_background_job(` 开始的位置（line 213），替换整个函数为：

```python
def _start_background_job(
    job_name: str,
    result_dir: str,
    target,
    *,
    logger=None,
    delay_seconds: int = 0,
    **kwargs,
) -> Dict[str, Any]:
    """提交后台任务到队列"""
    job_kwargs = dict(kwargs)
    delay_seconds = max(int(delay_seconds or 0), 0)
    
    # 记录事件接收日志
    if job_name == "auto_m3u_refresh":
        payload = job_kwargs.get("payload", {})
        if logger:
            logger.info(
                "%s: M3U refresh event received, account=%s, payload=%s",
                PLUGIN_KEY,
                payload.get("account_name", "(all)"),
                json.dumps(payload, ensure_ascii=False)
            )
    
    # 提交任务到队列
    enqueue_result = _enqueue_job(
        result_dir,
        job_name,
        delay_seconds,
        logger=logger,
        **job_kwargs
    )
    
    if enqueue_result.get("status") == "blocked":
        return enqueue_result
    
    job_id = enqueue_result["job_id"]
    queue_position = enqueue_result.get("queue_position")
    
    # 写入初始状态
    job_payload = enqueue_result["job"]
    _write_last_result(result_dir, job_payload)
    
    if queue_position == "running":
        # 立即启动
        _start_job_runner(result_dir, job_id, job_name, delay_seconds, logger, **job_kwargs)
    
    return enqueue_result
```

- [ ] **Step 3: 删除旧的 _reserve_background_job 函数**

找到 `def _reserve_background_job(` 开始的函数（line 333-361），完整删除该函数

- [ ] **Step 4: 验证语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 5: Commit**

```bash
git add plugin.py
git commit -m "refactor: 重构 _start_background_job 使用队列"
```

---

### Task 12: 更新 read_active_job_state 函数

**Files:**
- Modify: `plugin.py:382-384` (修改 `read_active_job_state` 函数)

- [ ] **Step 1: 替换 read_active_job_state 函数**

找到现有的 `read_active_job_state` 函数（line 382-384），替换为：

```python
def read_active_job_state(plugin_dir: str) -> Optional[Dict[str, Any]]:
    """读取活跃任务状态，包含队列信息"""
    with JOB_STATE_LOCK:
        # 读取并清理队列
        queue = _read_job_queue(plugin_dir)
        queue = _clean_stale_jobs(plugin_dir, queue, logger=None)
        
        # 如果队列状态发生变化，写回
        original_queue = _read_job_queue(plugin_dir)
        if queue != original_queue:
            _write_job_queue(plugin_dir, queue)
        
        running = queue.get("running")
        if not running:
            return None
        
        # 检查是否过期
        updated_at_ts = float(running.get("updated_at_ts") or 0)
        if time.time() - updated_at_ts > RUNNING_JOB_TIMEOUT_SECONDS:
            return None
        
        # 返回包含队列信息的状态
        result = dict(running)
        result["queue"] = {
            "running": running,
            "pending": queue.get("pending"),
        }
        return result
```

- [ ] **Step 2: 删除旧的 _read_active_job_state_unlocked 函数**

找到 `def _read_active_job_state_unlocked(` 函数（line 387-391），完整删除

- [ ] **Step 3: 验证语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 4: Commit**

```bash
git add plugin.py
git commit -m "refactor: 更新 read_active_job_state 返回队列信息"
```

---

## Phase 3: 增强用户体验

### Task 13: 更新 latest_result 响应

**Files:**
- Modify: `plugin.py:168-192` (修改 `latest_result` action 处理)

- [ ] **Step 1: 更新 latest_result 响应包含队列信息**

找到 `if action == "latest_result":` 部分（line 168-192），替换为：

```python
        if action == "latest_result":
            try:
                result = read_latest_result(plugin_dir)
                active_job = read_active_job_state(plugin_dir)
            except Exception as exc:
                return {
                    "status": "error",
                    "message": f"读取最近结果失败：{exc}",
                }
            if active_job:
                queue_info = active_job.pop("queue", {})
                return {
                    "status": "ok",
                    "message": active_job.get("message", "已有后台任务正在执行。"),
                    "file": result.get("file") if result else None,
                    "summary": result,
                    "active_job": active_job,
                    "queue": queue_info,
                }
            if not result:
                return {"status": "ok", "message": "还没有结果报告。"}
            return {
                "status": "ok",
                "message": result.get("message", "已读取最近结果。"),
                "file": result.get("file"),
                "summary": result,
            }
```

- [ ] **Step 2: 验证语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: latest_result 响应包含队列信息"
```

---

### Task 14: 更新任务提交响应格式

**Files:**
- Modify: `plugin.py:197-210` (修改 `_format_background_job_result` 函数)

- [ ] **Step 1: 更新 _format_background_job_result 函数**

找到 `_format_background_job_result` 函数（line 197-210），替换为：

```python
def _format_background_job_result(result: Dict[str, Any], queued_prefix: str) -> Dict[str, Any]:
    if result.get("status") == "blocked":
        return {
            "status": "blocked",
            "message": result.get("message", "已有后台任务正在执行，请稍后再试。"),
            "queue": result.get("queue"),
        }
    job_id = result.get("job_id", "")
    queue_position = result.get("queue_position", "running")
    replaced_job_id = result.get("replaced_job_id")
    
    message = f"{queued_prefix}，任务 ID：{job_id}"
    if queue_position == "pending":
        message += "（队列中等待）"
    if replaced_job_id:
        message += f"（已替换旧任务 {replaced_job_id[:8]}）"
    
    return {
        "status": "queued",
        "message": message,
        "task_id": job_id,
        "queue_position": queue_position,
        "replaced_job_id": replaced_job_id,
        "job": result.get("job"),
    }
```

- [ ] **Step 2: 验证语法正确**

```bash
python -m py_compile plugin.py
```

预期：无输出表示语法正确

- [ ] **Step 3: Commit**

```bash
git add plugin.py
git commit -m "feat: 任务提交响应包含队列位置和替换信息"
```

---

## Phase 4: 测试与验证

### Task 15: 手动测试 - M3U 刷新事件连续触发

**Files:**
- Test: Manual testing via plugin UI

- [ ] **Step 1: 启动 Dispatcharr**

```bash
docker logs -f dispatcharr
```

保持日志输出窗口打开

- [ ] **Step 2: 触发第一次 M3U 刷新**

在 Dispatcharr 插件页面，点击"手动模拟"按钮（auto_m3u_refresh action）

预期日志：
```
INFO: channel_stream_regex_assigner: M3U refresh event received
INFO: channel_stream_regex_assigner: Job started immediately, job_id=xxx, job_name=auto_m3u_refresh
```

- [ ] **Step 3: 等待 1 分钟后触发第二次**

再次点击"手动模拟"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: M3U refresh event received
INFO: channel_stream_regex_assigner: Job queued as pending, job_id=yyy, job_name=auto_m3u_refresh
```

- [ ] **Step 4: 再等待 1 分钟触发第三次**

再次点击"手动模拟"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: M3U refresh event received
WARNING: channel_stream_regex_assigner: Pending job replaced, old_job_id=yyy, new_job_id=zzz
```

- [ ] **Step 5: 等待第一个任务完成**

预期日志：
```
INFO: channel_stream_regex_assigner: Job completed successfully, job_id=xxx
INFO: channel_stream_regex_assigner: Queue rotation triggered, completed_job_id=xxx, next_job_id=zzz
```

- [ ] **Step 6: 验证最终结果**

只有第一次和第三次任务被执行，第二次被替换跳过

---

### Task 16: 手动测试 - 队列已满拒绝

**Files:**
- Test: Manual testing via plugin UI

- [ ] **Step 1: 启动"立即执行规则匹配"**

在插件页面点击"立即执行"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: Job started immediately, job_id=aaa, job_name=apply_match
```

- [ ] **Step 2: 立即提交"重排已挂流"**

点击"重排已挂流"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: Job queued as pending, job_id=bbb, job_name=sort_existing_streams
```

- [ ] **Step 3: 立即提交"扫描 M3U EPG"**

点击"扫描 EPG"按钮

预期响应：
```json
{
  "status": "blocked",
  "message": "队列已满：当前正在执行【立即执行规则匹配】任务，队列中等待【重排已挂流】任务。新的【扫描 M3U EPG】任务无法提交，请稍后再试。"
}
```

预期日志：
```
WARNING: channel_stream_regex_assigner: Job rejected (queue full), job_name=scan_m3u_epg, running=apply_match, pending=sort_existing_streams
```

- [ ] **Step 4: 验证队列信息**

点击"查看结果"按钮，响应应包含：
```json
{
  "queue": {
    "running": {"job_name": "apply_match", ...},
    "pending": {"job_name": "sort_existing_streams", ...}
  }
}
```

---

### Task 17: 手动测试 - Apply 替换 Preview

**Files:**
- Test: Manual testing via plugin UI

- [ ] **Step 1: 启动"重排已挂流"**

点击"重排已挂流"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: Job started immediately, job_id=xxx, job_name=sort_existing_streams
```

- [ ] **Step 2: 提交"预览规则匹配"**

点击"预览"按钮

预期日志：
```
INFO: channel_stream_regex_assigner: Job queued as pending, job_id=yyy, job_name=preview_match
```

- [ ] **Step 3: 提交"立即执行规则匹配"**

点击"立即执行"按钮

预期日志：
```
WARNING: channel_stream_regex_assigner: Pending job replaced, old_job_id=yyy, old_job_name=preview_match, new_job_id=zzz, new_job_name=apply_match
```

- [ ] **Step 4: 验证最终执行**

等待重排完成后，应执行 apply_match（不是 preview_match）

预期日志：
```
INFO: channel_stream_regex_assigner: Queue rotation triggered, next_job_name=apply_match
```

---

### Task 18: 日志验证测试

**Files:**
- Test: Docker logs analysis

- [ ] **Step 1: 清空日志并重启**

```bash
docker-compose restart dispatcharr
```

- [ ] **Step 2: 执行一次完整流程**

触发 M3U 刷新 → 等待完成 → 再次触发 → 验证替换

- [ ] **Step 3: 检查日志完整性**

```bash
docker logs dispatcharr 2>&1 | grep "channel_stream_regex_assigner" > /tmp/plugin_logs.txt
```

验证日志包含：
- ✅ M3U refresh event received
- ✅ Job started immediately
- ✅ Job queued as pending
- ✅ Pending job replaced
- ✅ Job completed successfully
- ✅ Queue rotation triggered

- [ ] **Step 4: 验证日志可读性**

打开 `/tmp/plugin_logs.txt`，确认：
- 每条日志都有时间戳
- 包含 job_id 便于追踪
- 消息清晰描述了发生了什么

---

## Phase 5: 文档与收尾

### Task 19: 更新 README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 添加日志查询章节**

在 README 的"自动执行"章节后添加：

```markdown
## 日志查询

插件在关键操作点都会记录日志，方便排查问题。

**查看最近的任务提交和执行：**
```bash
docker logs dispatcharr 2>&1 | grep "channel_stream_regex_assigner" | tail -50
```

**查看 M3U 刷新事件：**
```bash
docker logs dispatcharr 2>&1 | grep "M3U refresh event received"
```

**查看任务被拒绝的原因：**
```bash
docker logs dispatcharr 2>&1 | grep "Job rejected"
```

**查看任务替换情况：**
```bash
docker logs dispatcharr 2>&1 | grep "Pending job replaced"
```

**查看队列轮转：**
```bash
docker logs dispatcharr 2>&1 | grep "Queue rotation"
```
```

- [ ] **Step 2: 添加故障排查章节**

继续添加：

```markdown
## 故障排查

### M3U 刷新后没有自动执行

1. **检查插件设置**：确认"M3U 刷新后自动执行"已启用
2. **查看事件接收日志**：
   ```bash
   docker logs dispatcharr 2>&1 | grep "M3U refresh event received"
   ```
   如果没有此日志，说明事件未触发

3. **查看任务提交日志**：
   ```bash
   docker logs dispatcharr 2>&1 | grep "Job.*auto_m3u_refresh"
   ```
   确认任务是否被提交、排队或拒绝

4. **查看队列状态**：插件页面点击"查看结果"，检查 `queue` 字段

### 任务一直卡住不执行

1. **检查是否有过期任务**：
   ```bash
   docker logs dispatcharr 2>&1 | grep "Stale.*job detected"
   ```

2. **手动清理**：等待 30 分钟后，过期任务会自动清理

3. **强制重启**：
   ```bash
   docker-compose restart dispatcharr
   ```
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: 添加日志查询和故障排查章节"
```

---

### Task 20: 更新插件版本号

**Files:**
- Modify: `plugin.py:49`
- Modify: `plugin.json:3`

- [ ] **Step 1: 更新 plugin.py 版本号**

将 `version = "0.3.1"` 改为 `version = "0.4.0"`

- [ ] **Step 2: 更新 plugin.json 版本号**

将 `"version": "0.3.1"` 改为 `"version": "0.4.0"`

- [ ] **Step 3: Commit**

```bash
git add plugin.py plugin.json
git commit -m "chore: 升级版本至 0.4.0"
```

---

### Task 21: 最终验证

**Files:**
- Test: Complete integration test

- [ ] **Step 1: 重新打包插件**

```bash
zip -r channel_stream_regex_assigner_v0.4.0.zip plugin.py plugin.json channel_rules.txt README.md
```

- [ ] **Step 2: 在测试环境安装**

上传新版本插件到 Dispatcharr 测试实例

- [ ] **Step 3: 完整功能测试**

执行 Task 15、16、17、18 的所有测试用例

- [ ] **Step 4: 验证向后兼容性**

确认：
- 原有功能正常工作
- 前端可以正常解析响应（忽略新增字段）
- 旧的 `job_state.json` 文件不影响新版本

- [ ] **Step 5: 最终 commit**

```bash
git add .
git commit -m "feat: 任务队列重构完成 v0.4.0"
git tag v0.4.0
```

---

## 总结

本实现计划完成了以下目标：

✅ **核心队列逻辑**：双槽位队列（1 running + 1 pending）+ 智能任务替换
✅ **状态管理**：自动清理过期任务（running 30分钟，pending 2小时）
✅ **队列轮转**：任务完成后自动启动下一个
✅ **完善日志**：事件接收、任务提交、替换、清理、完成全流程可追踪
✅ **用户体验**：队列状态可见、错误消息清晰、故障排查文档完善
✅ **向后兼容**：不破坏现有 API，前端无需升级

**关键改进：**
- M3U 刷新事件不再丢失（总会执行最新的一次）
- 任务不会永久阻塞（自动清理 + 轮转）
- 调试友好（完整日志 + 队列状态可见）

**升级路径：**
- 平滑升级，无需数据迁移
- 旧版本 `job_state.json` 会被新版本兼容
- 降级回旧版本也不会有问题
