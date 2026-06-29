import json
import logging
import os
import re
import gzip
import zipfile
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PLUGIN_KEY = "channel_stream_regex_assigner"
# 模块级日志器：target 函数（后台线程内）拿不到 context logger，统一用这里记录，
# 与 Dispatcharr 的 logging 配置走同一出口，Docker 日志可见。
LOGGER = logging.getLogger(PLUGIN_KEY)
EXPORT_DIR = "exports"
PLUGIN_DATA_DIR = "plugin_data"
LAST_RESULT_FILE = "last_result.json"
JOB_STATE_FILE = "job_state.json"
DAILY_SCHEDULER_STATE_FILE = "daily_scheduler_state.json"
RULES_FILE_NAME = "channel_rules.txt"
LEGACY_RULES_TEMPLATE_FILENAME = "channel_rules_template.txt"
RULE_DELIMITERS = ("|||", "\t")
PROGRESS_LOG_INTERVAL_SECONDS = 5
JOB_STATE_STALE_SECONDS = 6 * 60 * 60
AUTO_M3U_REFRESH_JOB_NAME = "auto_m3u_refresh"
SCHEDULED_MATCH_JOB_NAME = "scheduled_match"
DAILY_SCHEDULER_JOB_NAME = "daily_scheduler"
DAILY_SCHEDULER_DEFAULT_TIME = "04:30"
DAILY_SCHEDULER_POLL_SECONDS = 60
AUTO_REFRESH_PENDING_KEY = "pending_auto_refresh"
AUTO_REFRESH_PENDING_COUNT_KEY = "pending_auto_refresh_count"
AUTO_REFRESH_PENDING_AT_KEY = "pending_auto_refresh_at"
EPG_URL_ATTR_RE = re.compile(
    r"(?:x-tvg-url|url-tvg|tvg-url)\s*=\s*([\"'])(.*?)\1",
    re.IGNORECASE,
)
EPG_URL_FALLBACK_RE = re.compile(
    r"https?://[^\s\"']+(?:\.xml|\.xml\.gz|\.xml\.zip|xmltv)[^\s\"']*",
    re.IGNORECASE,
)
LAST_RESULT_WRITE_LOCK = threading.Lock()
JOB_STATE_LOCK = threading.Lock()
DAILY_SCHEDULER_LOCK = threading.Lock()
DAILY_SCHEDULERS: Dict[str, Dict[str, Any]] = {}
JOB_CANCEL_LOCK = threading.Lock()
JOB_CANCEL_EVENTS: Dict[str, threading.Event] = {}


class JobCancelled(Exception):
    """Raised inside background jobs when the user requests a cooperative stop."""


@dataclass
class Rule:
    line_no: int
    channel_ref: str
    channel_name: str
    pattern: str
    mode: str
    max_streams: int


class Plugin:
    name = "Channel Stream Regex Assigner"
    version = "0.3.7"
    description = "按正则规则把 Streams 自动挂到 Channels，支持跟随 M3U 刷新自动执行、每日定时执行，并从 M3U 头部自动导入 EPG。"
    author = "Fengbao"

    fields = []
    actions = []

    def __init__(self):
        try:
            _ensure_rules_file(_plugin_dir())
        except Exception:
            pass
        manifest = _read_own_manifest()
        self.fields = manifest.get("fields", [])
        self.actions = manifest.get("actions", [])

    def run(self, action: str, params: dict, context: dict):
        settings = context.get("settings", {})
        logger = context.get("logger")
        plugin_dir = _plugin_dir()
        # 入口触发日志：自动事件触发和手动按钮触发都记录一行，方便排查
        # m3u_refresh 是否真的派发到本插件。latest_result 是纯状态查询，跳过避免刷屏。
        _log_run_trigger(logger, action, params, settings)

        if action in ("generate_rules", "generate_template"):
            result = generate_channel_rules_file(settings, plugin_dir, overwrite=True)
            return {
                "status": "ok",
                "message": (
                    f"规则文件已生成：{result['channel_count']} 个频道。"
                    f"请直接编辑规则文件：{result['file']}"
                ),
                "file": result["file"],
                "channel_count": result["channel_count"],
            }

        if action == "test_regex":
            result = test_regex(settings, plugin_dir)
            return {
                "status": "ok",
                "message": (
                    f"测试完成：扫描 {result['scanned_count']} 条可用 Streams，"
                    f"匹配 {result['matched_count']} 条；"
                    f"另有 {result['stale_matched_count']} 条过期 Streams 命中但被省略。"
                    f"报告：{result['file']}"
                ),
                "file": result["file"],
                "pattern": result["pattern"],
                "target": result["target"],
                "ignore_case": result["ignore_case"],
                "skip_stale_streams": result["skip_stale_streams"],
                "stream_group_filter": result["stream_group_filter"],
                "scanned_count": result["scanned_count"],
                "matched_count": result["matched_count"],
                "stale_scanned_count": result["stale_scanned_count"],
                "stale_matched_count": result["stale_matched_count"],
            }

        if action in ("preview_match", "apply_match"):
            dry_run = action == "preview_match"
            result = _start_background_job(
                "preview_match" if dry_run else "apply_match",
                plugin_dir,
                run_channel_stream_regex_job,
                logger=logger,
                settings=settings,
                plugin_dir=plugin_dir,
                dry_run=dry_run,
            )
            verb = "预览" if dry_run else "执行"
            if logger:
                logger.info("%s background job result: %s", self.name, result)
            return _format_background_job_result(result, f"{verb}任务已提交后台")

        if action == "sort_existing_streams":
            result = _start_background_job(
                "sort_existing_streams",
                plugin_dir,
                sort_existing_channel_streams,
                logger=logger,
                settings=settings,
                plugin_dir=plugin_dir,
            )
            return _format_background_job_result(result, "已挂流重排任务已提交后台")

        if action == "scan_m3u_epg":
            result = _start_background_job(
                "scan_m3u_epg",
                plugin_dir,
                import_m3u_epg_sources,
                logger=logger,
                settings=settings,
                plugin_dir=plugin_dir,
                payload={},
            )
            return _format_background_job_result(result, "M3U 头部 EPG 扫描任务已提交")

        if action == "start_daily_scheduler":
            result = start_daily_scheduler(settings, plugin_dir, logger=logger)
            if logger:
                logger.info("%s daily scheduler start result: %s", self.name, result)
            return result

        if action == "stop_daily_scheduler":
            result = stop_daily_scheduler(plugin_dir, logger=logger)
            if logger:
                logger.info("%s daily scheduler stop result: %s", self.name, result)
            return result

        if action == "stop_all_tasks":
            result = stop_all_tasks(plugin_dir, logger=logger)
            if logger:
                logger.info("%s stop all tasks result: %s", self.name, result)
            return result

        if action == "auto_m3u_refresh":
            if not _truthy(settings.get("auto_on_m3u_refresh")):
                skipped = {
                    "status": "skipped",
                    "message": "M3U 刷新后自动执行未启用，已跳过。",
                    "job_name": AUTO_M3U_REFRESH_JOB_NAME,
                }
                _write_last_result(plugin_dir, skipped)
                if logger:
                    logger.info(
                        "%s auto refresh skipped because setting is disabled",
                        PLUGIN_KEY,
                    )
                return {
                    "status": "skipped",
                    "message": "M3U 刷新后自动执行未启用，已跳过。",
                }
            delay_minutes = _non_negative_int(
                settings.get("m3u_refresh_delay_minutes"), 3
            )
            payload = params.get("payload", {}) if isinstance(params, dict) else {}
            result = _start_background_job(
                AUTO_M3U_REFRESH_JOB_NAME,
                plugin_dir,
                handle_m3u_refresh_job,
                logger=logger,
                delay_seconds=delay_minutes * 60,
                settings=settings,
                plugin_dir=plugin_dir,
                payload=payload,
            )
            if logger:
                logger.info("%s background job result: %s", self.name, result)
            return _format_background_job_result(
                result,
                f"M3U 刷新成功事件已接收，将在 {delay_minutes} 分钟后扫描 EPG 并执行匹配",
            )

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
                return {
                    "status": "ok",
                    "message": active_job.get("message", "已有后台任务正在执行。"),
                    "file": result.get("file") if result else None,
                    "summary": result,
                    "active_job": active_job,
                }
            if not result:
                return {"status": "ok", "message": "还没有结果报告。"}
            return {
                "status": "ok",
                "message": result.get("message", "已读取最近结果。"),
                "file": result.get("file"),
                "summary": result,
            }

        return {"status": "error", "message": f"Unknown action: {action}"}


def _log_run_trigger(logger, action: str, params: Any, settings: Dict[str, Any]) -> None:
    """记录每次 run() 调用，便于排查自动/手动触发是否真的到达本插件。

    自动事件触发时 Dispatcharr 会传入 params={"event": "...", "payload": {...}}；
    手动点按钮触发时 params 一般为空。据此区分来源并写入一行 INFO 日志。
    """
    if not logger or action == "latest_result":
        return
    raw_params = params if isinstance(params, dict) else {}
    payload = raw_params.get("payload") if isinstance(raw_params.get("payload"), dict) else {}
    event_name = raw_params.get("event")
    if event_name:
        source = f"自动触发(事件={event_name})"
    else:
        source = "手动触发(UI按钮)"
    logger.info(
        "%s run() 被调用: action=%s 来源=%s auto_on_m3u_refresh=%s 触发账号=%r",
        PLUGIN_KEY,
        action,
        source,
        _truthy(settings.get("auto_on_m3u_refresh")),
        payload.get("account_name"),
    )


def _format_background_job_result(result: Dict[str, Any], queued_prefix: str) -> Dict[str, Any]:
    if result.get("status") == "blocked":
        return {
            "status": "blocked",
            "message": result.get("message", "已有后台任务正在执行，请稍后再试。"),
            "active_job": result.get("active_job"),
        }
    if result.get("status") == "coalesced":
        return {
            "status": "queued",
            "message": result.get("message", "自动任务已合并到当前队列中。"),
            "task_id": result.get("job_id"),
            "active_job": result.get("job"),
            "merged": True,
        }
    job_id = result.get("job_id", "")
    return {
        "status": "queued",
        "message": f"{queued_prefix}，任务 ID：{job_id}",
        "task_id": job_id,
        "active_job": result.get("job"),
    }


def _start_background_job(
    job_name: str,
    result_dir: str,
    target,
    *,
    logger=None,
    delay_seconds: int = 0,
    **kwargs,
) -> Dict[str, Any]:
    job_kwargs = dict(kwargs)
    delay_seconds = max(int(delay_seconds or 0), 0)
    reservation = _reserve_background_job(result_dir, job_name, delay_seconds)
    if reservation.get("status") == "blocked":
        blocked_state = {
            "status": "blocked",
            "message": reservation.get("message", "已有后台任务正在执行，请稍后再试。"),
            "job_name": job_name,
            "blocked_by": reservation.get("active_job"),
            "active_job": reservation.get("active_job"),
            "updated_at": _now_label(),
        }
        _write_last_result(result_dir, blocked_state)
        if logger:
            logger.warning(
                "%s job %s blocked by active job %s",
                PLUGIN_KEY,
                job_name,
                (reservation.get("active_job") or {}).get("job_id", ""),
            )
        return reservation
    job_id = reservation["job_id"]
    _write_last_result(result_dir, reservation["job"])

    if reservation.get("status") == "coalesced":
        if logger:
            logger.info(
                "%s job %s coalesced into active job %s",
                PLUGIN_KEY,
                job_name,
                job_id,
            )
        return reservation

    def runner():
        try:
            from django.db import close_old_connections

            close_old_connections()
        except Exception:
            pass

        if delay_seconds > 0:
            LOGGER.info(
                "%s 任务 %s 已排队，等待 %s 秒后执行，job_id=%s",
                PLUGIN_KEY, job_name, delay_seconds, job_id,
            )
            delay_deadline = time.monotonic() + delay_seconds
            while time.monotonic() < delay_deadline:
                _raise_if_job_cancelled(job_name, job_id)
                time.sleep(min(delay_deadline - time.monotonic(), 1))
            running_state = _job_state_payload(
                job_id,
                job_name,
                "running",
                f"{_job_display_name(job_name)}任务正在后台运行。任务 ID：{job_id}",
            )
            _write_job_state(result_dir, running_state)
            _write_last_result(result_dir, running_state)

        try:
            progress_log_state = {"last_at": 0.0, "last_message": ""}
            rerun_count = 0
            current_payload = dict(job_kwargs)

            def progress_callback(current, total, message, extra=None):
                _raise_if_job_cancelled(job_name, job_id)
                payload = _write_progress(
                    result_dir,
                    job_id,
                    job_name,
                    current,
                    total,
                    message,
                    extra=extra,
                )
                _log_progress(logger, payload, progress_log_state)
                _raise_if_job_cancelled(job_name, job_id)

            while True:
                _raise_if_job_cancelled(job_name, job_id)
                LOGGER.info(
                    "%s 任务 %s 开始执行，job_id=%s",
                    PLUGIN_KEY, job_name, job_id,
                )
                result = target(progress_callback=progress_callback, **current_payload)
                _raise_if_job_cancelled(job_name, job_id)
                if not isinstance(result, dict):
                    result = {
                        "status": "ok",
                        "message": (
                            f"{_job_display_name(job_name)}任务完成。"
                            f"任务 ID：{job_id}"
                        ),
                    }

                if job_name == AUTO_M3U_REFRESH_JOB_NAME:
                    pending_count = _consume_pending_auto_refresh_count(
                        result_dir, job_id
                    )
                    if pending_count > 0:
                        rerun_count += pending_count
                        rerun_state = _job_state_payload(
                            job_id,
                            job_name,
                            "running",
                            (
                                f"{_job_display_name(job_name)}任务已合并 "
                                f"{pending_count} 个新的 M3U 刷新事件，"
                                "正在自动补跑。"
                                f"任务 ID：{job_id}"
                            ),
                        )
                        _write_job_state(result_dir, rerun_state)
                        _write_last_result(result_dir, rerun_state)
                        if logger:
                            logger.info(
                                "%s job %s will rerun after merging %s pending events",
                                PLUGIN_KEY,
                                job_id,
                                pending_count,
                            )
                        current_payload = dict(job_kwargs)
                        current_payload["payload"] = {}
                        continue

                completed = dict(result)
                completed["job_id"] = job_id
                completed["job_name"] = job_name
                completed["finished_at"] = _now_label()
                completed["status"] = completed.get("status") or "ok"
                if rerun_count > 0:
                    completed["message"] = (
                        f"{completed.get('message', 'done')} "
                        f"已自动补跑 {rerun_count} 个额外刷新事件。"
                    )
                    completed["rerun_count"] = rerun_count
                _write_job_state(result_dir, completed)
                _write_last_result(result_dir, completed)
                if logger:
                    logger.info(
                        "%s job %s completed: %s",
                        PLUGIN_KEY,
                        job_id,
                        completed.get("message", "done"),
                    )
                break
        except JobCancelled as exc:
            if logger:
                logger.warning("%s background job %s canceled", PLUGIN_KEY, job_id)
            canceled = {
                "status": "canceled",
                "message": str(exc) or (
                    f"{_job_display_name(job_name)}任务已被手动停止。"
                    f"任务 ID：{job_id}"
                ),
                "job_id": job_id,
                "job_name": job_name,
                "finished_at": _now_label(),
            }
            _write_job_state(result_dir, canceled)
            _write_last_result(result_dir, canceled)
        except Exception as exc:
            if logger:
                logger.exception("%s background job %s failed", PLUGIN_KEY, job_id)
            try:
                failed = {
                    "status": "error",
                    "message": f"{job_name} 后台任务失败：{exc}",
                    "job_id": job_id,
                    "job_name": job_name,
                    "finished_at": _now_label(),
                }
                _write_job_state(result_dir, failed)
                _write_last_result(result_dir, failed)
            except Exception:
                if logger:
                    logger.exception("%s failed to write error report", PLUGIN_KEY)
        finally:
            _unregister_job_cancel_event(job_id)
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
    _register_job_cancel_event(job_id)
    thread.start()
    return reservation


def _reserve_background_job(plugin_dir: str, job_name: str, delay_seconds: int) -> Dict[str, Any]:
    job_id = str(uuid.uuid4())
    status = "running" if delay_seconds == 0 else "waiting"
    wait_text = (
        "正在后台运行"
        if delay_seconds == 0
        else f"已排队，将在 {delay_seconds} 秒后运行"
    )
    job = _job_state_payload(
        job_id,
        job_name,
        status,
        f"{_job_display_name(job_name)}任务{wait_text}。任务 ID：{job_id}",
    )
    with JOB_STATE_LOCK:
        active_job = _read_active_job_state_unlocked(plugin_dir)
        if active_job:
            if (
                job_name == AUTO_M3U_REFRESH_JOB_NAME
                and active_job.get("job_name") == AUTO_M3U_REFRESH_JOB_NAME
            ):
                pending_job = _mark_pending_auto_refresh_unlocked(plugin_dir, active_job)
                return {
                    "status": "coalesced",
                    "message": (
                        "M3U 刷新事件已合并到当前自动任务，"
                        f"任务 ID：{active_job.get('job_id', '')}。"
                    ),
                    "job_id": active_job.get("job_id", ""),
                    "job": pending_job,
                    "active_job": pending_job,
                }
            message = (
                f"当前已有{_job_display_name(active_job.get('job_name', ''))}"
                f"任务正在执行，任务 ID：{active_job.get('job_id', '')}。"
                "请等待完成后再启动新任务。"
            )
            return {
                "status": "blocked",
                "message": message,
                "active_job": active_job,
            }
        _write_job_state_unlocked(plugin_dir, job)
    return {"status": "queued", "job_id": job_id, "job": job}


def _job_state_payload(
    job_id: str,
    job_name: str,
    status: str,
    message: str,
) -> Dict[str, Any]:
    now_label = _now_label()
    return {
        "status": status,
        "message": message,
        "job_id": job_id,
        "job_name": job_name,
        "job_label": _job_display_name(job_name),
        "started_at": now_label,
        "runner_pid": os.getpid(),
        "updated_at_ts": time.time(),
    }


def read_active_job_state(plugin_dir: str) -> Optional[Dict[str, Any]]:
    with JOB_STATE_LOCK:
        return _read_active_job_state_unlocked(plugin_dir)


def _read_active_job_state_unlocked(plugin_dir: str) -> Optional[Dict[str, Any]]:
    state = _read_job_state_unlocked(plugin_dir)
    if not _is_active_job_state(state):
        return None
    return state


def _mark_pending_auto_refresh_unlocked(
    plugin_dir: str,
    active_job: Dict[str, Any],
) -> Dict[str, Any]:
    current = _read_job_state_unlocked(plugin_dir) or dict(active_job)
    if current.get("job_id") != active_job.get("job_id"):
        current = dict(active_job)
    pending_count = int(current.get(AUTO_REFRESH_PENDING_COUNT_KEY) or 0) + 1
    current[AUTO_REFRESH_PENDING_KEY] = True
    current[AUTO_REFRESH_PENDING_COUNT_KEY] = pending_count
    current[AUTO_REFRESH_PENDING_AT_KEY] = _now_label()
    current["message"] = (
        f"{_job_display_name(AUTO_M3U_REFRESH_JOB_NAME)}任务正在执行，"
        f"已合并 {pending_count} 个新的 M3U 刷新事件，"
        "当前任务完成后会自动补跑。"
        f"任务 ID：{current.get('job_id', '')}"
    )
    _write_job_state_unlocked(plugin_dir, current)
    return current


def _consume_pending_auto_refresh_count(
    plugin_dir: str,
    job_id: str,
) -> int:
    with JOB_STATE_LOCK:
        state = _read_job_state_unlocked(plugin_dir)
        if not state or state.get("job_id") != job_id:
            return 0
        pending_count = int(state.get(AUTO_REFRESH_PENDING_COUNT_KEY) or 0)
        if pending_count <= 0:
            return 0
        state[AUTO_REFRESH_PENDING_KEY] = False
        state[AUTO_REFRESH_PENDING_COUNT_KEY] = 0
        state[AUTO_REFRESH_PENDING_AT_KEY] = None
        _write_job_state_unlocked(plugin_dir, state)
        return pending_count


def _is_active_job_state(state: Optional[Dict[str, Any]]) -> bool:
    if not state or state.get("status") not in ("waiting", "running"):
        return False
    runner_pid = state.get("runner_pid")
    if runner_pid is None:
        return False
    try:
        if int(runner_pid) != os.getpid():
            return False
    except (TypeError, ValueError):
        return False
    updated_at_ts = float(state.get("updated_at_ts") or 0)
    return time.time() - updated_at_ts <= JOB_STATE_STALE_SECONDS


def _job_state_path(plugin_dir: str) -> str:
    return os.path.join(_exports_dir(plugin_dir), JOB_STATE_FILE)


def _read_job_state_unlocked(plugin_dir: str) -> Optional[Dict[str, Any]]:
    path = _job_state_path(plugin_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_job_state(plugin_dir: str, state: Dict[str, Any]) -> None:
    with JOB_STATE_LOCK:
        _write_job_state_unlocked(plugin_dir, state)


def _write_job_state_unlocked(plugin_dir: str, state: Dict[str, Any]) -> None:
    current = _read_job_state_unlocked(plugin_dir)
    payload = dict(state)
    if current and current.get("job_id") == payload.get("job_id"):
        payload["started_at"] = current.get("started_at") or payload.get("started_at")
        payload.setdefault("job_label", current.get("job_label"))
        for key in (
            AUTO_REFRESH_PENDING_KEY,
            AUTO_REFRESH_PENDING_COUNT_KEY,
            AUTO_REFRESH_PENDING_AT_KEY,
        ):
            if key in current and key not in payload:
                payload[key] = current[key]
    if payload.get("status") in ("waiting", "running"):
        payload["runner_pid"] = int(payload.get("runner_pid") or os.getpid())
    payload["updated_at"] = _now_label()
    payload["updated_at_ts"] = float(payload.get("updated_at_ts") or time.time())
    _write_json_file_atomic(_job_state_path(plugin_dir), payload)


def _job_display_name(job_name: str) -> str:
    names = {
        "preview_match": "预览规则匹配",
        "apply_match": "立即执行规则匹配",
        "sort_existing_streams": "重排已挂流",
        "scan_m3u_epg": "扫描 M3U EPG",
        AUTO_M3U_REFRESH_JOB_NAME: "M3U 刷新后自动执行",
        SCHEDULED_MATCH_JOB_NAME: "每日定时规则匹配",
        DAILY_SCHEDULER_JOB_NAME: "每日定时器",
    }
    return names.get(job_name, job_name)


def start_daily_scheduler(
    settings: Dict[str, Any],
    plugin_dir: str,
    *,
    logger=None,
) -> Dict[str, Any]:
    daily_time = str(
        settings.get("daily_schedule_time") or DAILY_SCHEDULER_DEFAULT_TIME
    ).strip()
    if not _truthy(settings.get("daily_schedule_enabled")):
        _stop_daily_scheduler_entry(plugin_dir)
        result = {
            "status": "skipped",
            "message": "每日定时执行未启用，定时器未启动。",
            "job_name": DAILY_SCHEDULER_JOB_NAME,
        }
        _write_daily_scheduler_state(plugin_dir, result)
        _write_last_result(plugin_dir, result)
        return result

    try:
        _parse_daily_schedule_time(daily_time)
    except ValueError as exc:
        result = {
            "status": "error",
            "message": f"每日执行时间无效：{exc}",
            "job_name": DAILY_SCHEDULER_JOB_NAME,
            "daily_schedule_time": daily_time,
        }
        _write_daily_scheduler_state(plugin_dir, result)
        _write_last_result(plugin_dir, result)
        return result

    _stop_daily_scheduler_entry(plugin_dir)
    now = datetime.now()
    next_run = _next_daily_run_datetime(now, daily_time)
    stop_event = threading.Event()
    scheduler_settings = dict(settings)
    thread = threading.Thread(
        target=_daily_scheduler_loop,
        name=f"{PLUGIN_KEY}-daily-scheduler",
        daemon=True,
        kwargs={
            "settings": scheduler_settings,
            "plugin_dir": plugin_dir,
            "daily_time": daily_time,
            "stop_event": stop_event,
            "logger": logger,
        },
    )
    with DAILY_SCHEDULER_LOCK:
        DAILY_SCHEDULERS[_daily_scheduler_key(plugin_dir)] = {
            "thread": thread,
            "stop_event": stop_event,
            "daily_time": daily_time,
        }
    thread.start()

    result = {
        "status": "ok",
        "message": f"每日定时器已启动：每天 {daily_time} 执行规则匹配。",
        "job_name": DAILY_SCHEDULER_JOB_NAME,
        "daily_schedule_time": daily_time,
        "next_run_at": _format_datetime(next_run),
    }
    _write_daily_scheduler_state(plugin_dir, result)
    _write_last_result(plugin_dir, result)
    return result


def stop_daily_scheduler(plugin_dir: str, *, logger=None) -> Dict[str, Any]:
    stopped = _stop_daily_scheduler_entry(plugin_dir)
    result = {
        "status": "stopped",
        "message": "每日定时器已停止。" if stopped else "每日定时器未运行。",
        "job_name": DAILY_SCHEDULER_JOB_NAME,
    }
    _write_daily_scheduler_state(plugin_dir, result)
    _write_last_result(plugin_dir, result)
    if logger and stopped:
        logger.info("%s daily scheduler stopped", PLUGIN_KEY)
    return result


def stop_all_tasks(plugin_dir: str, *, logger=None) -> Dict[str, Any]:
    scheduler_stopped = _stop_daily_scheduler_entry(plugin_dir)
    cancelled_job_ids = _request_all_job_cancellation()
    canceled_active_job = _cancel_active_job_state(plugin_dir)
    message_parts = ["已请求停止全部后台任务。"]
    if scheduler_stopped:
        message_parts.append("每日定时器已停止。")
    if canceled_active_job:
        message_parts.append(
            f"当前{_job_display_name(canceled_active_job.get('job_name', ''))}"
            f"任务已标记取消，任务 ID：{canceled_active_job.get('job_id', '')}。"
        )
    elif not cancelled_job_ids:
        message_parts.append("当前没有发现正在运行的后台任务。")
    result = {
        "status": "ok",
        "message": "".join(message_parts),
        "job_name": "stop_all_tasks",
        "scheduler_stopped": scheduler_stopped,
        "cancelled_job_ids": cancelled_job_ids,
        "canceled_active_job": canceled_active_job,
    }
    if scheduler_stopped:
        _write_daily_scheduler_state(
            plugin_dir,
            {
                "status": "stopped",
                "message": "停止全部任务时已停止每日定时器。",
                "job_name": DAILY_SCHEDULER_JOB_NAME,
            },
        )
    _write_last_result(plugin_dir, result)
    if logger:
        logger.warning("%s stop all tasks requested: %s", PLUGIN_KEY, result)
    return result


def read_daily_scheduler_state(plugin_dir: str) -> Optional[Dict[str, Any]]:
    path = _daily_scheduler_state_path(plugin_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _daily_scheduler_loop(
    *,
    settings: Dict[str, Any],
    plugin_dir: str,
    daily_time: str,
    stop_event: threading.Event,
    logger=None,
) -> None:
    while not stop_event.is_set():
        now = datetime.now()
        next_run = _next_daily_run_datetime(now, daily_time)
        _write_daily_scheduler_state(
            plugin_dir,
            {
                "status": "waiting",
                "message": f"每日定时器等待中：下次 {daily_time} 执行。",
                "job_name": DAILY_SCHEDULER_JOB_NAME,
                "daily_schedule_time": daily_time,
                "next_run_at": _format_datetime(next_run),
            },
        )
        wait_seconds = max((next_run - now).total_seconds(), 0)
        while wait_seconds > 0 and not stop_event.is_set():
            if stop_event.wait(min(wait_seconds, DAILY_SCHEDULER_POLL_SECONDS)):
                return
            wait_seconds = max((next_run - datetime.now()).total_seconds(), 0)
        if stop_event.is_set():
            return

        result = _start_background_job(
            SCHEDULED_MATCH_JOB_NAME,
            plugin_dir,
            run_channel_stream_regex_job,
            logger=logger,
            settings=settings,
            plugin_dir=plugin_dir,
            dry_run=False,
        )
        scheduler_state = {
            "status": "triggered" if result.get("status") != "blocked" else "blocked",
            "message": (
                f"每日定时器已触发规则匹配，任务 ID：{result.get('job_id', '')}。"
                if result.get("status") != "blocked"
                else result.get("message", "每日定时执行被已有任务阻塞。")
            ),
            "job_name": DAILY_SCHEDULER_JOB_NAME,
            "daily_schedule_time": daily_time,
            "last_run_at": _now_label(),
            "result": result,
        }
        _write_daily_scheduler_state(plugin_dir, scheduler_state)
        if logger:
            logger.info("%s daily scheduler trigger result: %s", PLUGIN_KEY, result)


def _stop_daily_scheduler_entry(plugin_dir: str) -> bool:
    with DAILY_SCHEDULER_LOCK:
        entry = DAILY_SCHEDULERS.pop(_daily_scheduler_key(plugin_dir), None)
    if not entry:
        return False
    stop_event = entry.get("stop_event")
    thread = entry.get("thread")
    if stop_event:
        stop_event.set()
    if thread and thread.is_alive():
        thread.join(timeout=1)
    return True


def _stop_all_daily_schedulers_for_tests() -> None:
    with DAILY_SCHEDULER_LOCK:
        entries = list(DAILY_SCHEDULERS.values())
        DAILY_SCHEDULERS.clear()
    for entry in entries:
        stop_event = entry.get("stop_event")
        thread = entry.get("thread")
        if stop_event:
            stop_event.set()
        if thread and thread.is_alive():
            thread.join(timeout=1)


def _register_job_cancel_event(job_id: str) -> threading.Event:
    with JOB_CANCEL_LOCK:
        event = threading.Event()
        JOB_CANCEL_EVENTS[job_id] = event
        return event


def _unregister_job_cancel_event(job_id: str) -> None:
    with JOB_CANCEL_LOCK:
        JOB_CANCEL_EVENTS.pop(job_id, None)


def _request_all_job_cancellation() -> List[str]:
    with JOB_CANCEL_LOCK:
        items = list(JOB_CANCEL_EVENTS.items())
    for _job_id, event in items:
        event.set()
    return [job_id for job_id, _event in items]


def _is_job_cancelled(job_id: str = "") -> bool:
    with JOB_CANCEL_LOCK:
        if job_id:
            event = JOB_CANCEL_EVENTS.get(job_id)
            if event and event.is_set():
                return True
        return any(event.is_set() for event in JOB_CANCEL_EVENTS.values())


def _raise_if_job_cancelled(job_name: str = "", job_id: str = "") -> None:
    if _is_job_cancelled(job_id):
        raise JobCancelled(
            f"{_job_display_name(job_name)}任务已收到停止请求。任务 ID：{job_id}"
        )


def _cancel_active_job_state(plugin_dir: str) -> Optional[Dict[str, Any]]:
    with JOB_STATE_LOCK:
        active_job = _read_job_state_unlocked(plugin_dir)
        if not active_job or active_job.get("status") not in ("waiting", "running"):
            return None
        canceled = dict(active_job)
        canceled["status"] = "canceled"
        canceled["message"] = (
            f"{_job_display_name(canceled.get('job_name', ''))}任务已被手动停止。"
            f"任务 ID：{canceled.get('job_id', '')}"
        )
        canceled["finished_at"] = _now_label()
        _write_job_state_unlocked(plugin_dir, canceled)
    _write_last_result(plugin_dir, canceled)
    return canceled


def _parse_daily_schedule_time(value: Any) -> Tuple[int, int]:
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
    if not match:
        raise ValueError("请使用 HH:MM 格式，例如 04:30。")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        raise ValueError("小时必须为 0-23，分钟必须为 0-59。")
    return hour, minute


def _next_daily_run_datetime(now: datetime, daily_time: Any) -> datetime:
    hour, minute = _parse_daily_schedule_time(daily_time)
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return next_run


def _daily_scheduler_key(plugin_dir: str) -> str:
    return os.path.abspath(plugin_dir)


def _daily_scheduler_state_path(plugin_dir: str) -> str:
    return os.path.join(_exports_dir(plugin_dir), DAILY_SCHEDULER_STATE_FILE)


def _write_daily_scheduler_state(plugin_dir: str, state: Dict[str, Any]) -> None:
    payload = dict(state)
    payload["updated_at"] = _now_label()
    _write_json_file_atomic(_daily_scheduler_state_path(plugin_dir), payload)


def _format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def handle_m3u_refresh_job(
    settings: Dict[str, Any],
    plugin_dir: str,
    payload: Dict[str, Any],
    progress_callback=None,
):
    LOGGER.info(
        "%s 自动执行开始：先扫描 M3U 头部 EPG，再执行频道挂流，payload=%s",
        PLUGIN_KEY, payload,
    )
    if progress_callback:
        progress_callback(0, 2, "正在扫描 M3U 头部 EPG")
    epg_summary = import_m3u_epg_sources(
        settings, plugin_dir, payload, progress_callback=progress_callback
    )
    LOGGER.info(
        "%s 自动执行：EPG 扫描完成 created=%s existing=%s，开始频道挂流",
        PLUGIN_KEY, epg_summary.get("created", 0), epg_summary.get("existing", 0),
    )
    if progress_callback:
        progress_callback(1, 2, "EPG 扫描完成，正在执行频道挂流")
    assign_summary = run_channel_stream_regex_job(
        settings=settings,
        plugin_dir=plugin_dir,
        dry_run=False,
        progress_callback=progress_callback,
    )
    if progress_callback:
        progress_callback(2, 2, "M3U 刷新后自动执行完成")
    combined = {
        "status": "ok",
        "message": (
            "M3U 刷新后任务完成："
            f"EPG 新增 {epg_summary.get('created', 0)} 个，"
            f"复用 {epg_summary.get('existing', 0)} 个；"
            f"频道挂流写入 {assign_summary.get('streams_added', 0)} 条。"
        ),
        "epg": epg_summary,
        "assignment": assign_summary,
    }
    LOGGER.info("%s 自动执行完成：%s", PLUGIN_KEY, combined.get("message"))
    _write_last_result(plugin_dir, combined)
    return combined


def run_channel_stream_regex_job(
    settings: Dict[str, Any],
    plugin_dir: str,
    dry_run: bool,
    progress_callback=None,
):
    from django.db import transaction
    from django.db.utils import IntegrityError
    from django.db import close_old_connections
    from apps.channels.models import Channel, ChannelStream

    close_old_connections()
    started_at = _now_label()
    rules, parse_errors, rules_source = parse_rules(settings, plugin_dir)
    total_rules = len(rules)
    LOGGER.info(
        "%s 规则匹配开始：mode=%s 规则=%s 条 解析错误=%s 条 来源=%s",
        PLUGIN_KEY, "dry-run" if dry_run else "write", total_rules,
        len(parse_errors), rules_source,
    )
    if progress_callback:
        progress_callback(
            0,
            max(total_rules, 1),
            f"{'预览' if dry_run else '执行'}开始：已解析 {total_rules} 条规则",
            extra={"rules_total": total_rules, "parse_errors": len(parse_errors)},
        )
    report_lines = [
        f"Channel Stream Regex Assigner {'Preview' if dry_run else 'Apply'}",
        f"Started: {started_at}",
        f"Mode: {'dry-run' if dry_run else 'write'}",
        f"Target: {settings.get('test_target') or 'name'}",
        f"Skip stale streams: {_truthy(settings.get('skip_stale_streams'))}",
        f"Keyword priority: {settings.get('stream_keyword_priority') or '(none)'}",
        f"Source priority: {settings.get('stream_source_priority') or '(none)'}",
        f"Rules source: {rules_source}",
        "",
    ]

    summary = {
        "status": "ok",
        "dry_run": dry_run,
        "rules_total": len(rules),
        "parse_errors": len(parse_errors),
        "channels_seen": 0,
        "channels_changed": 0,
        "streams_matched": 0,
        "streams_added": 0,
        "links_removed": 0,
        "links_reordered": 0,
        "streams_skipped": 0,
        "rules_skipped": 0,
        "errors": 0,
    }

    if parse_errors:
        report_lines.append("Parse errors:")
        report_lines.extend(f"- {err}" for err in parse_errors)
        report_lines.append("")

    for index, rule in enumerate(rules, start=1):
        channel = _find_channel(Channel, rule.channel_ref, rule.channel_name)
        if channel is None:
            summary["rules_skipped"] += 1
            report_lines.append(
                f"[line {rule.line_no}] SKIP channel not found: "
                f"{rule.channel_ref} / {rule.channel_name}"
            )
            if progress_callback:
                progress_callback(
                    index,
                    max(total_rules, 1),
                    f"已处理 {index}/{total_rules} 条规则：频道不存在",
                    extra=summary,
                )
            continue

        existing_links = list(
            ChannelStream.objects.filter(channel=channel)
            .select_related("stream")
            .order_by("order", "id")
        )
        if _truthy(settings.get("only_empty_channels")) and existing_links:
            summary["rules_skipped"] += 1
            report_lines.append(
                f"[line {rule.line_no}] SKIP channel has existing streams: "
                f"{channel.id} {channel.name}"
            )
            if progress_callback:
                progress_callback(
                    index,
                    max(total_rules, 1),
                    f"已处理 {index}/{total_rules} 条规则：跳过已有流频道 {channel.name}",
                    extra=summary,
                )
            continue

        summary["channels_seen"] += 1
        if progress_callback:
            progress_callback(
                index - 1,
                max(total_rules, 1),
                f"正在处理 {index}/{total_rules}：{channel.name}",
                extra=summary,
            )
        try:
            def match_progress_callback(scanned: int, matched: int):
                if progress_callback:
                    progress_callback(
                        index - 1,
                        max(total_rules, 1),
                        (
                            f"正在处理 {index}/{total_rules}：{channel.name}，"
                            f"已扫描 Streams {scanned} 条，当前匹配 {matched} 条"
                        ),
                        extra=summary,
                    )

            matched_streams, regex_error = _match_streams(
                rule, settings, progress_callback=match_progress_callback
            )
        except Exception as exc:
            summary["errors"] += 1
            report_lines.append(
                f"[line {rule.line_no}] ERROR matching {channel.id} {channel.name}: {exc}"
            )
            if progress_callback:
                progress_callback(
                    index,
                    max(total_rules, 1),
                    f"已处理 {index}/{total_rules} 条规则：{channel.name} 出错",
                    extra=summary,
                )
            continue

        if regex_error:
            summary["errors"] += 1
            report_lines.append(f"[line {rule.line_no}] REGEX ERROR: {regex_error}")
            if progress_callback:
                progress_callback(
                    index,
                    max(total_rules, 1),
                    f"已处理 {index}/{total_rules} 条规则：正则错误",
                    extra=summary,
                )
            continue

        matched_streams = _sort_streams_for_assignment(matched_streams, settings)

        raw_matched_count = len(matched_streams)
        deduped, duplicate_skipped = _dedupe_streams_with_details(
            matched_streams,
            preferred_name=channel.name,
        )
        if rule.max_streams > 0:
            deduped = deduped[: rule.max_streams]
        summary["streams_matched"] += len(deduped)
        if rule.mode == "replace":
            planned, skipped = _plan_replace(existing_links, deduped, settings)
            merge_skipped = []
        else:
            merge_existing_links, _merge_removed_links = _filter_channel_stream_links(
                existing_links,
                settings,
            )
            planned, merge_skipped = _plan_merge_with_details(merge_existing_links, deduped)
            skipped = len(merge_skipped)

        summary["streams_skipped"] += skipped
        action_label = "PREVIEW" if dry_run else "APPLY"
        report_lines.append(
            f"[line {rule.line_no}] {action_label} channel={channel.id} "
            f"name={channel.name!r} mode={rule.mode} raw_matched={raw_matched_count} "
            f"deduped={len(deduped)} planned={len(planned)} "
            f"duplicate_skipped={len(duplicate_skipped)} merge_skipped={skipped}"
        )
        for stream in planned[:50]:
            report_lines.append(
                f"  + stream={stream.id} name={stream.name!r} url={stream.url or ''}"
            )
        if len(planned) > 50:
            report_lines.append(f"  ... {len(planned) - 50} more planned streams")
        for stream in duplicate_skipped[:20]:
            report_lines.append(
                f"  - duplicate-skip stream={stream.id} name={stream.name!r} "
                f"url={stream.url or ''}"
            )
        if len(duplicate_skipped) > 20:
            report_lines.append(
                f"  ... {len(duplicate_skipped) - 20} more duplicate-skipped streams"
            )
        for stream in merge_skipped[:20]:
            report_lines.append(
                f"  - merge-skip stream={stream.id} name={stream.name!r} "
                f"url={stream.url or ''}"
            )
        if len(merge_skipped) > 20:
            report_lines.append(
                f"  ... {len(merge_skipped) - 20} more merge-skipped streams"
            )

        if dry_run:
            if progress_callback:
                progress_callback(
                    index,
                    max(total_rules, 1),
                    f"已处理 {index}/{total_rules} 条规则：{channel.name}",
                    extra=summary,
                )
            continue

        if progress_callback:
            progress_callback(
                index - 1,
                max(total_rules, 1),
                f"正在写入 {index}/{total_rules}：{channel.name}",
                extra=summary,
            )

        try:
            with transaction.atomic():
                channel_changed = False
                if rule.mode == "replace":
                    if not planned and not _truthy(settings.get("allow_empty_replace")):
                        report_lines.append(
                            "  ! replace skipped because match is empty and "
                            "allow_empty_replace is false"
                        )
                        continue
                    ChannelStream.objects.filter(channel=channel).delete()
                    links = [
                        ChannelStream(channel=channel, stream=stream, order=index)
                        for index, stream in enumerate(planned)
                    ]
                    ChannelStream.objects.bulk_create(links, ignore_conflicts=True)
                    summary["streams_added"] += len(links)
                    channel_changed = True
                else:
                    ordered_streams, active_existing_links, removed_links = (
                        _order_channel_streams_for_merge(existing_links, planned, settings)
                    )
                    existing_link_by_stream_identity = {
                        id(link.stream): link
                        for link in active_existing_links
                        if getattr(link, "stream", None) is not None
                    }
                    changed_links = []
                    links = []
                    for stream_order, stream in enumerate(ordered_streams):
                        existing_link = existing_link_by_stream_identity.get(id(stream))
                        if existing_link is not None:
                            if existing_link.order != stream_order:
                                existing_link.order = stream_order
                                changed_links.append(existing_link)
                            continue
                        links.append(
                            ChannelStream(
                                channel=channel,
                                stream=stream,
                                order=stream_order,
                            )
                        )
                    if removed_links:
                        ChannelStream.objects.filter(
                            id__in=[link.id for link in removed_links]
                        ).delete()
                        channel_changed = True
                    if changed_links:
                        ChannelStream.objects.bulk_update(changed_links, ["order"])
                        channel_changed = True
                    if links:
                        ChannelStream.objects.bulk_create(links, ignore_conflicts=True)
                        channel_changed = True
                    summary["streams_added"] += len(links)
                    summary["links_removed"] += len(removed_links)
                    summary["links_reordered"] += len(changed_links)
                if channel_changed:
                    summary["channels_changed"] += 1
        except IntegrityError as exc:
            summary["errors"] += 1
            report_lines.append(f"  ! DB integrity error: {exc}")

        if progress_callback:
            progress_callback(
                index,
                max(total_rules, 1),
                f"已处理 {index}/{total_rules} 条规则：{channel.name}",
                extra=summary,
            )

    report_lines.extend(["", "Summary:", json.dumps(summary, ensure_ascii=False, indent=2)])
    filename = "preview_result.txt" if dry_run else "apply_result.txt"
    file_path = _write_text_report(plugin_dir, filename, "\n".join(report_lines))
    summary["file"] = file_path
    summary["message"] = (
        f"{'预览' if dry_run else '执行'}完成：规则 {summary['rules_total']} 条，"
        f"匹配流 {summary['streams_matched']} 条，写入 {summary['streams_added']} 条，"
        f"移除失效挂流 {summary['links_removed']} 条，"
        f"更新顺序 {summary['links_reordered']} 条，"
        f"错误 {summary['errors']} 条。报告：{file_path}"
    )
    LOGGER.info("%s 规则匹配完成：%s", PLUGIN_KEY, summary["message"])
    _write_last_result(plugin_dir, summary)
    close_old_connections()
    return summary


def sort_existing_channel_streams(
    settings: Dict[str, Any],
    plugin_dir: str,
    progress_callback=None,
):
    from django.db import transaction
    from django.db import close_old_connections
    from apps.channels.models import Channel, ChannelStream

    close_old_connections()
    started_at = _now_label()
    rules, parse_errors, rules_source = parse_rules(settings, plugin_dir)
    channels, missing_channels = _channels_for_existing_stream_sort(Channel, rules)
    total_channels = len(channels)
    LOGGER.info(
        "%s 已挂流重排开始：待处理 %s 个频道（%s）",
        PLUGIN_KEY, total_channels,
        "规则频道" if rules else "全部有挂流频道",
    )
    if progress_callback:
        progress_callback(
            0,
            max(total_channels, 1),
            f"开始重排已挂流：待处理 {total_channels} 个频道",
            extra={"channels_total": total_channels, "parse_errors": len(parse_errors)},
        )

    report_lines = [
        "Channel Stream Regex Assigner Existing Stream Sort",
        f"Started: {started_at}",
        f"Keyword priority: {settings.get('stream_keyword_priority') or '(none)'}",
        f"Source priority: {settings.get('stream_source_priority') or '(none)'}",
        f"Rules source: {rules_source}",
        f"Scope: {'rule channels' if rules else 'all channels with streams'}",
        "",
    ]
    summary = {
        "status": "ok",
        "channels_total": total_channels,
        "channels_seen": 0,
        "channels_changed": 0,
        "links_seen": 0,
        "links_removed": 0,
        "links_reordered": 0,
        "rules_total": len(rules),
        "parse_errors": len(parse_errors),
        "missing_channels": len(missing_channels),
        "errors": 0,
    }

    if parse_errors:
        report_lines.append("Parse errors:")
        report_lines.extend(f"- {err}" for err in parse_errors)
        report_lines.append("")
    if missing_channels:
        report_lines.append("Missing rule channels:")
        report_lines.extend(f"- {label}" for label in missing_channels[:100])
        if len(missing_channels) > 100:
            report_lines.append(f"... {len(missing_channels) - 100} more missing channels")
        report_lines.append("")

    for index, channel in enumerate(channels, start=1):
        summary["channels_seen"] += 1
        if progress_callback:
            progress_callback(
                index - 1,
                max(total_channels, 1),
                f"正在重排 {index}/{total_channels}：{channel.name}",
                extra=summary,
            )

        links = list(
            ChannelStream.objects.filter(channel=channel)
            .select_related("stream", "stream__m3u_account")
            .order_by("order", "id")
        )
        summary["links_seen"] += len(links)
        filtered_links, removed_links = _filter_channel_stream_links(links, settings)
        sorted_links = _sort_channel_stream_links(filtered_links, settings)
        changed_links = []
        for new_order, link in enumerate(sorted_links):
            if link.order != new_order:
                link.order = new_order
                changed_links.append(link)

        if changed_links or removed_links:
            try:
                with transaction.atomic():
                    if removed_links:
                        ChannelStream.objects.filter(id__in=[link.id for link in removed_links]).delete()
                    ChannelStream.objects.bulk_update(changed_links, ["order"])
                summary["channels_changed"] += 1
                summary["links_removed"] += len(removed_links)
                summary["links_reordered"] += len(changed_links)
                change_bits = []
                if removed_links:
                    change_bits.append(f"removed {len(removed_links)} stale links")
                if changed_links:
                    change_bits.append(
                        f"reordered {len(changed_links)}/{len(sorted_links)} links"
                    )
                report_lines.append(
                    f"[channel {channel.id}] {', '.join(change_bits)}: {channel.name!r}"
                )
                for link in sorted_links[:50]:
                    stream = link.stream
                    source = _stream_source_name(stream) or "(no source)"
                    report_lines.append(
                        f"  {link.order}. stream={stream.id} source={source!r} "
                        f"name={stream.name!r}"
                    )
                if len(sorted_links) > 50:
                    report_lines.append(f"  ... {len(sorted_links) - 50} more links")
            except Exception as exc:
                summary["errors"] += 1
                report_lines.append(
                    f"[channel {channel.id}] ERROR reorder {channel.name!r}: {exc}"
                )

        if progress_callback:
            progress_callback(
                index,
                max(total_channels, 1),
                f"已重排 {index}/{total_channels}：{channel.name}",
                extra=summary,
            )

    report_lines.extend(["", "Summary:", json.dumps(summary, ensure_ascii=False, indent=2)])
    file_path = _write_text_report(plugin_dir, "sort_existing_streams_result.txt", "\n".join(report_lines))
    summary["file"] = file_path
    summary["message"] = (
        f"已挂流重排完成：处理频道 {summary['channels_seen']} 个，"
        f"调整频道 {summary['channels_changed']} 个，"
        f"移除失效挂流 {summary['links_removed']} 条，"
        f"更新顺序 {summary['links_reordered']} 条，"
        f"错误 {summary['errors']} 条。报告：{file_path}"
    )
    LOGGER.info("%s 已挂流重排完成：%s", PLUGIN_KEY, summary["message"])
    _write_last_result(plugin_dir, summary)
    close_old_connections()
    return summary


def _channels_for_existing_stream_sort(Channel, rules: Sequence[Rule]):
    if not rules:
        channels = list(
            Channel.objects.filter(channelstream__isnull=False)
            .distinct()
            .order_by("channel_number", "name", "id")
        )
        return channels, []

    channels = []
    missing = []
    seen_ids = set()
    for rule in rules:
        channel = _find_channel(Channel, rule.channel_ref, rule.channel_name)
        if channel is None:
            missing.append(f"line {rule.line_no}: {rule.channel_ref} / {rule.channel_name}")
            continue
        if channel.id in seen_ids:
            continue
        seen_ids.add(channel.id)
        channels.append(channel)
    return channels, missing


def _sort_channel_stream_links(links: Sequence[Any], settings: Dict[str, Any]) -> List[Any]:
    filtered_links, _removed_links = _filter_channel_stream_links(links, settings)
    stream_to_links: Dict[int, List[Any]] = {}
    stream_items = []
    for link in filtered_links:
        stream = getattr(link, "stream", None)
        if stream is None:
            continue
        stream_items.append(stream)
        stream_to_links.setdefault(id(stream), []).append(link)

    sorted_streams = _sort_streams_for_assignment(stream_items, settings)
    sorted_links = []
    for stream in sorted_streams:
        sorted_links.extend(stream_to_links.get(id(stream), []))
    return sorted_links


def _filter_channel_stream_links(links: Sequence[Any], settings: Dict[str, Any]) -> Tuple[List[Any], List[Any]]:
    remove_stale = _truthy(settings.get("remove_stale_existing_streams", True))
    filtered_links = []
    removed_links = []
    for link in links:
        stream = getattr(link, "stream", None)
        if stream is None:
            continue
        if remove_stale and _truthy(getattr(stream, "is_stale", False)):
            removed_links.append(link)
            continue
        filtered_links.append(link)
    return filtered_links, removed_links


def _order_channel_streams_for_merge(
    existing_links: Sequence[Any],
    planned_streams: Sequence[Any],
    settings: Dict[str, Any],
) -> Tuple[List[Any], List[Any], List[Any]]:
    active_existing_links, removed_links = _filter_channel_stream_links(existing_links, settings)
    existing_streams = [
        link.stream for link in active_existing_links if getattr(link, "stream", None) is not None
    ]
    ordered_streams = _sort_streams_for_assignment(
        list(existing_streams) + list(planned_streams),
        settings,
    )
    return ordered_streams, active_existing_links, removed_links


def import_m3u_epg_sources(
    settings: Dict[str, Any],
    plugin_dir: str,
    payload: Optional[Dict[str, Any]] = None,
    progress_callback=None,
) -> Dict[str, Any]:
    from django.conf import settings as django_settings
    from django.db import close_old_connections
    from apps.m3u.models import M3UAccount
    from apps.epg.models import EPGSource
    from apps.epg.tasks import refresh_epg_data

    close_old_connections()
    payload = payload or {}
    if not _truthy(settings.get("auto_import_m3u_epg")):
        result = {
            "status": "skipped",
            "message": "自动导入 M3U 头部 EPG 未启用，已跳过。",
        }
        LOGGER.info("%s M3U EPG 扫描跳过：auto_import_m3u_epg 未启用", PLUGIN_KEY)
        _write_last_result(plugin_dir, result)
        return result

    account_name = str(payload.get("account_name") or "").strip()
    if account_name:
        accounts = M3UAccount.objects.filter(name=account_name)
    else:
        accounts = M3UAccount.objects.filter(is_active=True).exclude(
            name__iexact="custom"
        )
    accounts = accounts.order_by("name", "id")
    total_accounts = accounts.count()
    LOGGER.info(
        "%s M3U EPG 扫描开始：账号过滤=%s，待扫描 %s 个账号",
        PLUGIN_KEY, account_name or "全部启用账号", total_accounts,
    )
    if progress_callback:
        progress_callback(
            0,
            max(total_accounts, 1),
            f"开始扫描 M3U 头部 EPG：共 {total_accounts} 个账号",
        )

    scan_lines = _positive_int(settings.get("m3u_epg_scan_lines"), 30)
    refresh_existing = _truthy(settings.get("refresh_epg_after_import"))
    m3u_cache_dir = os.path.join(django_settings.MEDIA_ROOT, "cached_m3u")
    summary = {
        "status": "ok",
        "accounts_scanned": 0,
        "urls_found": 0,
        "created": 0,
        "existing": 0,
        "refreshed": 0,
        "errors": 0,
        "details": [],
    }
    report_lines = [
        "M3U Header EPG Import Result",
        f"Started: {_now_label()}",
        f"Account filter: {account_name or 'all active accounts'}",
        "",
    ]

    for index, account in enumerate(accounts, start=1):
        summary["accounts_scanned"] += 1
        if progress_callback:
            progress_callback(
                index - 1,
                max(total_accounts, 1),
                f"正在扫描 {index}/{total_accounts}：{account.name}",
                extra=summary,
            )
        try:
            path = _m3u_source_path(account, m3u_cache_dir)
            if not path or not os.path.exists(path):
                report_lines.append(
                    f"[{account.name}] SKIP no readable M3U file: {path or 'none'}"
                )
                if progress_callback:
                    progress_callback(
                        index,
                        max(total_accounts, 1),
                        f"已扫描 {index}/{total_accounts}：{account.name} 没有可读 M3U 文件",
                        extra=summary,
                    )
                continue
            header_lines = _read_m3u_header_lines(path, scan_lines)
            urls = extract_epg_urls_from_m3u_header(header_lines)
            summary["urls_found"] += len(urls)
            if not urls:
                report_lines.append(f"[{account.name}] no EPG URL found in header")
                continue

            for url in urls:
                source = EPGSource.objects.filter(url=url).order_by("id").first()
                created = source is None
                if created:
                    source = EPGSource.objects.create(
                        url=url,
                        name=_unique_epg_source_name(account.name, EPGSource),
                        source_type="xmltv",
                        is_active=True,
                        refresh_interval=0,
                        custom_properties={
                            "created_by": PLUGIN_KEY,
                            "m3u_account_id": account.id,
                            "m3u_account_name": account.name,
                        },
                    )
                if created:
                    summary["created"] += 1
                    report_lines.append(
                        f"[{account.name}] CREATED EPGSource id={source.id} url={url}"
                    )
                    # Dispatcharr's post_save signal queues the first refresh.
                    summary["refreshed"] += 1
                else:
                    summary["existing"] += 1
                    report_lines.append(
                        f"[{account.name}] EXISTS EPGSource id={source.id} url={url}"
                    )
                    if refresh_existing and source.is_active and source.source_type != "dummy":
                        refresh_epg_data.delay(source.id)
                        summary["refreshed"] += 1
                        report_lines.append(f"  queued refresh for EPGSource id={source.id}")
                summary["details"].append(
                    {
                        "account": account.name,
                        "url": url,
                        "source_id": source.id,
                        "created": created,
                    }
                )
        except Exception as exc:
                summary["errors"] += 1
                report_lines.append(f"[{account.name}] ERROR {exc}")

        if progress_callback:
            progress_callback(
                index,
                max(total_accounts, 1),
                f"已扫描 {index}/{total_accounts}：{account.name}",
                extra=summary,
            )

    report_lines.extend(["", "Summary:", json.dumps(summary, ensure_ascii=False, indent=2)])
    file_path = _write_text_report(plugin_dir, "m3u_epg_import_result.txt", "\n".join(report_lines))
    summary["file"] = file_path
    summary["message"] = (
        f"M3U 头部 EPG 扫描完成：新增 {summary['created']} 个，"
        f"复用 {summary['existing']} 个，刷新队列 {summary['refreshed']} 个。"
        f"报告：{file_path}"
    )
    LOGGER.info("%s M3U EPG 扫描完成：%s", PLUGIN_KEY, summary["message"])
    _write_last_result(plugin_dir, summary)
    close_old_connections()
    return summary


def extract_epg_urls_from_m3u_header(lines: Sequence[str]) -> List[str]:
    urls: List[str] = []
    for line in lines:
        for match in EPG_URL_ATTR_RE.finditer(line):
            urls.extend(_split_epg_url_value(match.group(2)))
        if not urls:
            urls.extend(EPG_URL_FALLBACK_RE.findall(line))
    return _dedupe_strings(urls)


def _split_epg_url_value(value: str) -> List[str]:
    parts = [part.strip() for part in re.split(r"\s*,\s*", value or "") if part.strip()]
    if not parts and value:
        parts = [value.strip()]
    return [part for part in parts if part.lower().startswith(("http://", "https://"))]


def _m3u_source_path(account: Any, cache_dir: str) -> Optional[str]:
    cached = os.path.join(cache_dir, f"{account.id}.m3u")
    if os.path.exists(cached):
        return cached
    if account.file_path:
        return account.file_path
    return cached if account.server_url else None


def _read_m3u_header_lines(path: str, max_lines: int) -> List[str]:
    lines: List[str] = []
    for line in _iter_text_lines(path):
        stripped = line.strip()
        if stripped.startswith("#EXTINF"):
            break
        lines.append(stripped)
        if len(lines) >= max_lines:
            break
    return lines


def _iter_text_lines(path: str):
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            yield from fh
        return
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith((".m3u", ".m3u8", ".txt")):
                    with zf.open(name) as raw:
                        for line in raw:
                            yield line.decode("utf-8", errors="ignore")
                    return
        return
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        yield from fh


def _unique_epg_source_name(account_name: str, EPGSource: Any) -> str:
    base = f"Auto EPG - {account_name}".strip()
    if not EPGSource.objects.filter(name=base).exists():
        return base
    index = 2
    while True:
        candidate = f"{base} ({index})"
        if not EPGSource.objects.filter(name=candidate).exists():
            return candidate
        index += 1


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def generate_channel_rules_file(
    settings: Dict[str, Any],
    plugin_dir: str,
    *,
    overwrite: bool = False,
) -> Dict[str, Any]:
    from apps.channels.models import Channel

    default_mode = _mode(settings.get("default_mode"), "merge")
    default_max = _max_streams(settings.get("default_max_streams"), 0)
    content, channel_count = _build_channel_rules_content(
        Channel.objects.all().order_by("channel_number", "name", "id"),
        default_mode=default_mode,
        default_max=default_max,
    )
    write_result = _write_rules_file(
        plugin_dir,
        content,
        overwrite=overwrite,
    )
    file_path = write_result["file"]
    result = {
        "status": write_result["status"],
        "message": (
            f"规则文件生成完成：{file_path}"
            if write_result["written"]
            else f"规则文件已存在，未覆盖：{file_path}"
        ),
        "file": file_path,
        "channel_count": channel_count,
        "written": write_result["written"],
    }
    if write_result["written"]:
        result["content"] = content
    _write_last_result(plugin_dir, result)
    return result


def _build_channel_rules_content(channels, default_mode: str, default_max: int) -> Tuple[str, int]:
    lines = [
        "# Channel Stream Regex Assigner 规则文件",
        "#",
        "# 用法：每个非注释行对应一个频道规则；以 # 开头的行会被忽略。",
        "# 推荐分隔符：|||，不要改成单个 |，否则正则里的 | 可能被误拆。",
        "# 编辑完成后保存本文件即可；插件会直接读取这个规则文件。",
        "# 如果需要重新生成，请先确认是否覆盖现有手工修改。",
        "# 建议回到插件页面先点“预览”，确认报告无误后再点“立即执行”。",
        "#",
        "# channel_id ||| channel_name ||| regex ||| mode ||| max_streams",
        "#",
        "# 字段说明：",
        "# - channel_id: 可选频道 ID，主要用于本机兼容；共享规则时可保留但不会优先使用。",
        "# - channel_name: 频道名称，优先按它查找频道；重名时使用 ID 最小的第一个频道。",
        "# - regex: 用来匹配 Stream 的正则；匹配字段由插件设置里的“正则测试字段”决定。",
        "# - mode: merge 或 replace。merge=合并，保留已有 Streams；replace=覆盖已有 Streams。",
        "# - max_streams: 最大添加流数量，0 表示无限。",
        "#",
        "# 默认建议：mode 使用 merge（合并），max_streams 使用 0（无限）。",
        "# 合并去重：merge 模式下，同一个 stream_id 或相同 URL 不会重复添加。",
        "# 覆盖保护：replace 模式下，如果正则没有匹配到任何 Stream，默认不会清空频道；",
        "#         只有打开插件设置里的“允许空匹配覆盖清空频道”才会清空。",
        "#",
        "# 示例：",
        "# 12 ||| CCTV-1 ||| ^CCTV[-_ ]?1($|高清|HD) ||| merge ||| 0",
        "# 13 ||| CCTV-5 ||| ^CCTV[-_ ]?5($|体育|HD) ||| replace ||| 2",
        "#",
    ]
    channel_count = 0
    for channel in channels:
        channel_count += 1
        name = str(channel.name or "").replace("\n", " ").strip()
        escaped = re.escape(name)
        lines.append(
            f"{channel.id} ||| {name} ||| {escaped} ||| {default_mode} ||| {default_max}"
        )
    content = "\n".join(lines) + "\n"
    return content, channel_count


def test_regex(settings: Dict[str, Any], plugin_dir: str) -> Dict[str, Any]:
    from apps.channels.models import Stream

    pattern = str(settings.get("test_regex") or "").strip()
    if not pattern:
        raise ValueError("测试正则不能为空")

    flags = re.IGNORECASE if _truthy(settings.get("ignore_case")) else 0
    compiled = re.compile(pattern, flags)
    target = str(settings.get("test_target") or "name")
    qs = _stream_queryset(Stream, settings)
    matches = []
    scanned_count = 0
    for stream in qs.iterator(chunk_size=1000):
        scanned_count += 1
        haystack = _stream_haystack(stream, target)
        if compiled.search(haystack):
            matches.append(stream)
    stale_matches = []
    stale_scanned_count = 0
    if _truthy(settings.get("skip_stale_streams")):
        for stream in _stream_queryset(Stream, settings, only_stale=True).iterator(chunk_size=1000):
            stale_scanned_count += 1
            haystack = _stream_haystack(stream, target)
            if compiled.search(haystack):
                stale_matches.append(stream)

    skip_stale = _truthy(settings.get("skip_stale_streams"))
    group_filter = str(settings.get("stream_group_filter") or "").strip()
    report = _build_regex_test_report(
        pattern=pattern,
        target=target,
        ignore_case=bool(flags & re.IGNORECASE),
        skip_stale=skip_stale,
        group_filter=group_filter,
        scanned_count=scanned_count,
        matches=matches,
        stale_scanned_count=stale_scanned_count,
        stale_matches=stale_matches,
    )
    file_path = _write_text_report(plugin_dir, "regex_test_result.txt", report)
    result = {
        "status": "ok",
        "message": (
            f"正则测试完成：扫描 {scanned_count} 条可用 Streams，"
            f"匹配 {len(matches)} 条；"
            f"过期命中 {len(stale_matches)} 条。报告：{file_path}"
        ),
        "file": file_path,
        "pattern": pattern,
        "target": target,
        "ignore_case": bool(flags & re.IGNORECASE),
        "skip_stale_streams": skip_stale,
        "stream_group_filter": group_filter,
        "scanned_count": scanned_count,
        "matched_count": len(matches),
        "stale_scanned_count": stale_scanned_count,
        "stale_matched_count": len(stale_matches),
    }
    _write_last_result(plugin_dir, result)
    return result


def _build_regex_test_report(
    pattern: str,
    target: str,
    ignore_case: bool,
    skip_stale: bool,
    group_filter: str,
    scanned_count: int,
    matches: Sequence[Any],
    stale_scanned_count: int,
    stale_matches: Sequence[Any],
) -> str:
    lines = [
        "Regex Test Result",
        f"Pattern: {pattern}",
        f"Target: {target}",
        f"Ignore case setting: {ignore_case}",
        f"Skip stale streams: {skip_stale}",
        f"Stream group filter: {group_filter or '(none)'}",
        f"Scanned active streams: {scanned_count}",
        f"Matched active streams: {len(matches)}",
        f"Scanned stale streams: {stale_scanned_count}",
        f"Omitted stale matches: {len(stale_matches)}",
        "",
        "Matched active stream samples:",
    ]
    if matches:
        for stream in matches[:500]:
            lines.append(_stream_sample_line(stream))
        if len(matches) > 500:
            lines.append(f"... {len(matches) - 500} more active matches omitted")
    else:
        lines.append("(none)")

    lines.append("")
    lines.append("Omitted stale match samples:")
    if stale_matches:
        for stream in stale_matches[:100]:
            lines.append(_stream_sample_line(stream))
        if len(stale_matches) > 100:
            lines.append(f"... {len(stale_matches) - 100} more stale matches omitted")
    else:
        lines.append("(none)")
    return "\n".join(lines)


def _stream_sample_line(stream: Any) -> str:
    return f"{stream.id} | {stream.name} | {stream.url or ''}"


def parse_rules(
    settings: Dict[str, Any],
    plugin_dir: str = "",
) -> Tuple[List[Rule], List[str], str]:
    default_mode = _mode(settings.get("default_mode"), "merge")
    default_max = _max_streams(settings.get("default_max_streams"), 0)
    text, source = _load_rules_text(settings, plugin_dir)
    rules: List[Rule] = []
    errors: List[str] = []

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = _split_rule_line(line)
        if len(parts) == 5:
            channel_ref, channel_name, pattern, mode_value, max_value = parts
        elif len(parts) == 4:
            channel_ref, pattern, mode_value, max_value = parts
            channel_name = ""
        elif len(parts) == 3:
            channel_ref, pattern, mode_value = parts
            channel_name = ""
            max_value = default_max
        elif len(parts) == 2:
            channel_ref, pattern = parts
            channel_name = ""
            mode_value = default_mode
            max_value = default_max
        else:
            errors.append(f"line {line_no}: expected 2-5 columns, got {len(parts)}")
            continue

        pattern = str(pattern).strip()
        if not pattern:
            errors.append(f"line {line_no}: regex is empty")
            continue

        rules.append(
            Rule(
                line_no=line_no,
                channel_ref=str(channel_ref).strip(),
                channel_name=str(channel_name).strip(),
                pattern=pattern,
                mode=_mode(mode_value, default_mode),
                max_streams=_max_streams(max_value, default_max),
            )
        )
    return rules, errors, source


def _load_rules_text(settings: Dict[str, Any], plugin_dir: str = "") -> Tuple[str, str]:
    if not plugin_dir:
        return "", f"missing {RULES_FILE_NAME}"

    rules_path = _ensure_rules_file(plugin_dir)
    if os.path.isfile(rules_path):
        with open(rules_path, "r", encoding="utf-8") as fh:
            return fh.read(), rules_path
    return "", rules_path


def _ensure_rules_file(plugin_dir: str) -> str:
    rules_path = _rules_file_path(plugin_dir)
    if os.path.isfile(rules_path):
        return rules_path

    for source_path in _rules_file_seed_paths(plugin_dir):
        if os.path.isfile(source_path):
            os.makedirs(os.path.dirname(rules_path), exist_ok=True)
            shutil.copyfile(source_path, rules_path)
            return rules_path

    return _write_rules_file(
        plugin_dir,
        _empty_rules_file_content(),
        overwrite=False,
        create_if_missing=True,
    )["file"]


def _write_rules_file(
    plugin_dir: str,
    content: str,
    *,
    overwrite: bool = False,
    create_if_missing: bool = True,
) -> Dict[str, Any]:
    file_path = _rules_file_path(plugin_dir)
    exists = os.path.isfile(file_path)
    if exists and not overwrite:
        return {"status": "exists", "file": file_path, "written": False}
    if exists or create_if_missing:
        with open(file_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return {"status": "ok", "file": file_path, "written": True}
    return {"status": "missing", "file": file_path, "written": False}


def _empty_rules_file_content() -> str:
    return "\n".join(
        [
            "# Channel Stream Regex Assigner 规则文件",
            "#",
            "# 请按以下格式填写频道规则，或使用“生成规则”按当前 Channels 生成初始内容：",
            "# channel_id ||| channel_name ||| regex ||| mode ||| max_streams",
            "",
        ]
    ) + "\n"


def _rules_file_path(plugin_dir: str) -> str:
    return os.path.join(_rules_data_dir(plugin_dir), RULES_FILE_NAME)


def _rules_file_seed_paths(plugin_dir: str) -> List[str]:
    return [
        os.path.join(plugin_dir, EXPORT_DIR, LEGACY_RULES_TEMPLATE_FILENAME),
        os.path.join(_rules_data_dir(plugin_dir), LEGACY_RULES_TEMPLATE_FILENAME),
        os.path.join(plugin_dir, RULES_FILE_NAME),
    ]


def _split_rule_line(line: str) -> List[str]:
    for delimiter in RULE_DELIMITERS:
        if delimiter in line:
            return [part.strip() for part in line.split(delimiter)]
    return [part.strip() for part in re.split(r"\s+\|\s+", line)]


def _match_streams(rule: Rule, settings: Dict[str, Any], progress_callback=None):
    from apps.channels.models import Stream

    flags = re.IGNORECASE if _truthy(settings.get("ignore_case")) else 0
    try:
        compiled = re.compile(rule.pattern, flags)
    except re.error as exc:
        return [], f"line {rule.line_no}: {exc}"

    target = str(settings.get("test_target") or "name")
    matches = []
    scanned = 0
    last_progress_at = time.monotonic()
    for stream in _stream_queryset(Stream, settings).iterator(chunk_size=1000):
        scanned += 1
        if compiled.search(_stream_haystack(stream, target)):
            matches.append(stream)
        if progress_callback:
            now = time.monotonic()
            if now - last_progress_at >= PROGRESS_LOG_INTERVAL_SECONDS:
                progress_callback(scanned, len(matches))
                last_progress_at = now
    return matches, None


def _stream_queryset(Stream, settings: Dict[str, Any], only_stale: bool = False):
    qs = Stream.objects.select_related("channel_group", "m3u_account").order_by("id")
    if only_stale:
        qs = qs.filter(is_stale=True)
    elif _truthy(settings.get("skip_stale_streams")):
        qs = qs.filter(is_stale=False)
    group_filter = str(settings.get("stream_group_filter") or "").strip()
    if group_filter:
        groups = [g.strip() for g in group_filter.split(",") if g.strip()]
        if groups:
            qs = qs.filter(channel_group__name__in=groups)
    return qs


def _stream_haystack(stream, target: str) -> str:
    name = stream.name or ""
    url = stream.url or ""
    if target == "url":
        return url
    if target == "both":
        return f"{name}\n{url}"
    return name


def _sort_streams_for_assignment(streams: Iterable[Any], settings: Dict[str, Any]) -> List[Any]:
    keyword_priority = _parse_priority_values(settings.get("stream_keyword_priority"))
    source_priority = _parse_priority_values(settings.get("stream_source_priority"))
    if not keyword_priority and not source_priority:
        return list(streams)

    source_rank_by_name = {
        source.casefold(): index for index, source in enumerate(source_priority)
    }

    def sort_key(item: Tuple[int, Any]):
        original_index, stream = item
        source_name = _stream_source_name(stream)
        source_folded = source_name.casefold()
        source_rank = source_rank_by_name.get(source_folded)
        keyword_rank = _stream_keyword_rank(stream, keyword_priority)
        has_keyword = keyword_rank < len(keyword_priority)
        name_key = str(getattr(stream, "name", "") or "").casefold()

        if source_priority:
            if source_rank is not None and has_keyword:
                section = 0
                source_key = source_rank
                keyword_key = keyword_rank
            elif source_rank is not None:
                section = 1
                source_key = source_rank
                keyword_key = len(keyword_priority)
            else:
                section = 2
                source_key = source_folded or "\uffff"
                keyword_key = keyword_rank
        else:
            section = 0 if has_keyword else 1
            source_key = source_folded or "\uffff"
            keyword_key = keyword_rank

        return (
            section,
            source_key,
            keyword_key,
            source_folded,
            name_key,
            getattr(stream, "id", 0) or 0,
            original_index,
        )

    return [stream for _index, stream in sorted(enumerate(streams), key=sort_key)]


def _parse_priority_values(value: Any) -> List[str]:
    raw_values = re.split(r"[,，\n]+", str(value or ""))
    values = []
    seen = set()
    for raw in raw_values:
        item = raw.strip()
        folded = item.casefold()
        if not item or folded in seen:
            continue
        seen.add(folded)
        values.append(item)
    return values


def _stream_keyword_rank(stream: Any, keywords: Sequence[str]) -> int:
    if not keywords:
        return 0
    name = str(getattr(stream, "name", "") or "").casefold()
    for index, keyword in enumerate(keywords):
        if keyword.casefold() in name:
            return index
    return len(keywords)


def _stream_source_name(stream: Any) -> str:
    account = getattr(stream, "m3u_account", None)
    return str(getattr(account, "name", "") or "").strip()


def _find_channel(Channel, channel_ref: str, channel_name: str):
    ref = str(channel_ref or "").strip()
    name = str(channel_name or "").strip()
    if name:
        channel = _find_channel_by_name_candidates(Channel, name)
        if channel:
            return channel
    if ref.isdigit():
        channel = Channel.objects.filter(id=int(ref)).first()
        if channel:
            return channel
    name = ref
    if not name:
        return None
    return _find_channel_by_name_candidates(Channel, name)


def _find_channel_by_name_candidates(Channel, name: str):
    for candidate in _channel_name_candidates(name):
        channel = Channel.objects.filter(name=candidate).order_by("id").first()
        if channel:
            return channel
    return None


def _channel_name_candidates(name: str) -> List[str]:
    stripped = str(name or "").strip()
    if not stripped:
        return []

    base_names = [stripped]
    base_names.extend(_cctv_descriptive_name_candidates(stripped))

    seen = set()
    unique = []
    for base_name in base_names:
        for candidate in _quality_suffix_name_candidates(base_name):
            if candidate not in seen:
                seen.add(candidate)
                unique.append(candidate)
    return unique


def _quality_suffix_name_candidates(name: str) -> List[str]:
    if name.endswith("超清"):
        return [name]
    candidates = [name]
    for separator in ("", " ", "-", "_"):
        candidates.append(f"{name}{separator}超清")
    return candidates


def _cctv_descriptive_name_candidates(name: str) -> List[str]:
    match = re.fullmatch(r"(?i)CCTV[-_ ]?0?(\d+)", name)
    if not match:
        return []

    descriptors_by_number = {
        "1": ("综合",),
        "2": ("财经",),
        "3": ("综艺",),
        "4": ("中文国际",),
        "5": ("体育",),
        "6": ("电影",),
        "7": ("国防军事",),
        "8": ("电视剧",),
        "9": ("纪录",),
        "10": ("科教",),
        "11": ("戏曲",),
        "12": ("社会与法",),
        "13": ("新闻",),
        "14": ("少儿",),
        "15": ("音乐",),
        "16": ("奥林匹克",),
        "17": ("农业农村",),
    }
    number = str(int(match.group(1)))
    descriptors = descriptors_by_number.get(number, ())
    names = []
    for descriptor in descriptors:
        names.append(f"CCTV{number}{descriptor}")
        names.append(f"CCTV{number} {descriptor}")
        names.append(f"CCTV-{number}{descriptor}")
        names.append(f"CCTV-{number} {descriptor}")
    return names


def _plan_merge(existing_links: Sequence[Any], matched_streams: Sequence[Any]):
    planned, skipped_streams = _plan_merge_with_details(existing_links, matched_streams)
    return planned, len(skipped_streams)


def _plan_merge_with_details(existing_links: Sequence[Any], matched_streams: Sequence[Any]):
    existing_ids = {link.stream_id for link in existing_links}
    existing_urls = {
        (link.stream.url or "").strip()
        for link in existing_links
        if link.stream and link.stream.url
    }
    planned = []
    skipped_streams = []
    for stream in matched_streams:
        url = (stream.url or "").strip()
        if stream.id in existing_ids or (url and url in existing_urls):
            skipped_streams.append(stream)
            continue
        existing_ids.add(stream.id)
        if url:
            existing_urls.add(url)
        planned.append(stream)
    return planned, skipped_streams


def _plan_replace(existing_links: Sequence[Any], matched_streams: Sequence[Any], settings: Dict[str, Any]):
    if not matched_streams and not _truthy(settings.get("allow_empty_replace")):
        return [], 0
    return list(matched_streams), 0


def _dedupe_streams(streams: Iterable[Any], preferred_name: str = "") -> List[Any]:
    deduped, _skipped = _dedupe_streams_with_details(streams, preferred_name)
    return deduped


def _dedupe_streams_with_details(
    streams: Iterable[Any],
    preferred_name: str = "",
) -> Tuple[List[Any], List[Any]]:
    seen_ids = set()
    seen_urls = set()
    deduped = []
    skipped = []
    preferred = str(preferred_name or "").strip().casefold()
    ordered_streams = sorted(
        enumerate(streams),
        key=lambda item: (
            0
            if preferred
            and str(getattr(item[1], "name", "") or "").strip().casefold() == preferred
            else 1,
            item[0],
        ),
    )
    for _index, stream in ordered_streams:
        url = (stream.url or "").strip()
        if stream.id in seen_ids or (url and url in seen_urls):
            skipped.append(stream)
            continue
        seen_ids.add(stream.id)
        if url:
            seen_urls.add(url)
        deduped.append(stream)
    return deduped, skipped


def _mode(value: Any, default: str) -> str:
    value = str(value or default or "merge").strip().lower()
    if value in ("replace", "overwrite", "覆盖"):
        return "replace"
    return "merge"


def _max_streams(value: Any, default: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = int(default if default is not None else 1)
    return max(parsed, 0)


def _non_negative_int(value: Any, default: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = int(default or 0)
    return max(parsed, 0)


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = int(default or 1)
    return max(parsed, 1)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "y", "on")
    return False


def _plugin_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _exports_dir(plugin_dir: str) -> str:
    path = os.path.join(plugin_dir, EXPORT_DIR)
    os.makedirs(path, exist_ok=True)
    return path


def _rules_data_dir(plugin_dir: str) -> str:
    plugins_root = os.path.dirname(os.path.abspath(plugin_dir))
    data_root = os.environ.get("DISPATCHARR_PLUGIN_DATA_DIR")
    if not data_root:
        data_root = os.path.join(os.path.dirname(plugins_root), PLUGIN_DATA_DIR)
    path = os.path.join(data_root, PLUGIN_KEY)
    os.makedirs(path, exist_ok=True)
    return path


def _now_label() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_text_report(plugin_dir: str, filename: str, content: str) -> str:
    path = os.path.join(_exports_dir(plugin_dir), filename)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


def _write_progress(
    plugin_dir: str,
    job_id: str,
    job_name: str,
    current: int,
    total: int,
    message: str,
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    total = max(int(total or 0), 1)
    current = min(max(int(current or 0), 0), total)
    percent = round((current / total) * 100, 1)
    progress = {
        "current": current,
        "total": total,
        "percent": percent,
        "bar": _progress_bar(percent),
    }
    payload: Dict[str, Any] = {
        "status": "running",
        "message": f"{message} {progress['bar']} {percent}%",
        "job_id": job_id,
        "job_name": job_name,
        "job_label": _job_display_name(job_name),
        "progress": progress,
    }
    if extra:
        payload["summary"] = dict(extra)
    _write_job_state(plugin_dir, payload)
    _write_last_result(plugin_dir, payload)
    return payload


def _log_progress(logger, payload: Dict[str, Any], state: Dict[str, Any]) -> None:
    if not logger:
        return
    now = time.monotonic()
    message = payload.get("message", "")
    progress = payload.get("progress") or {}
    current = progress.get("current")
    total = progress.get("total")
    should_log = (
        current == 0
        or current == total
        or now - float(state.get("last_at") or 0) >= PROGRESS_LOG_INTERVAL_SECONDS
    )
    if not should_log:
        return
    state["last_at"] = now
    state["last_message"] = message
    logger.info(
        "%s job %s progress: %s",
        PLUGIN_KEY,
        payload.get("job_id", ""),
        message,
    )


def _progress_bar(percent: float) -> str:
    width = 20
    filled = int(round(width * max(0.0, min(float(percent), 100.0)) / 100.0))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _write_last_result(plugin_dir: str, result: Dict[str, Any]) -> None:
    path = os.path.join(_exports_dir(plugin_dir), LAST_RESULT_FILE)
    payload = dict(result)
    payload["updated_at"] = _now_label()
    _write_json_file_atomic(path, payload, lock=LAST_RESULT_WRITE_LOCK)


def _write_json_file_atomic(
    path: str,
    payload: Dict[str, Any],
    *,
    lock: Optional[threading.Lock] = None,
) -> None:
    tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    try:
        write_lock = lock
        if write_lock is None:
            class _NoopLock:
                def __enter__(self):
                    return None

                def __exit__(self, *_args):
                    return False

            write_lock = _NoopLock()
        with write_lock:
            for attempt in range(5):
                try:
                    os.replace(tmp_path, path)
                    break
                except PermissionError:
                    if attempt >= 4:
                        raise
                    time.sleep(0.05 * (attempt + 1))
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass


def read_latest_result(plugin_dir: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(_exports_dir(plugin_dir), LAST_RESULT_FILE)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _read_own_manifest() -> Dict[str, Any]:
    path = os.path.join(_plugin_dir(), "plugin.json")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
