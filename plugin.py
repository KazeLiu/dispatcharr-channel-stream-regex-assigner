import json
import os
import re
import gzip
import zipfile
import shutil
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PLUGIN_KEY = "channel_stream_regex_assigner"
EXPORT_DIR = "exports"
PLUGIN_DATA_DIR = "plugin_data"
LAST_RESULT_FILE = "last_result.json"
RULES_FILE_NAME = "channel_rules.txt"
LEGACY_RULES_TEMPLATE_FILENAME = "channel_rules_template.txt"
RULE_DELIMITERS = ("|||", "\t")
PROGRESS_LOG_INTERVAL_SECONDS = 5
EPG_URL_ATTR_RE = re.compile(
    r"(?:x-tvg-url|url-tvg|tvg-url)\s*=\s*([\"'])(.*?)\1",
    re.IGNORECASE,
)
EPG_URL_FALLBACK_RE = re.compile(
    r"https?://[^\s\"']+(?:\.xml|\.xml\.gz|\.xml\.zip|xmltv)[^\s\"']*",
    re.IGNORECASE,
)


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
    version = "0.2.18"
    description = "Use regex rules to attach Streams to Channels and import EPG URLs from M3U headers."
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
            job_id = _start_background_job(
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
                logger.info("%s queued background job %s", self.name, job_id)
            return {
                "status": "queued",
                "message": f"{verb}任务已提交后台，任务 ID：{job_id}",
                "task_id": job_id,
            }

        if action == "sort_existing_streams":
            job_id = _start_background_job(
                "sort_existing_streams",
                plugin_dir,
                sort_existing_channel_streams,
                logger=logger,
                settings=settings,
                plugin_dir=plugin_dir,
            )
            return {
                "status": "queued",
                "message": f"已挂流重排任务已提交后台，任务 ID：{job_id}",
                "task_id": job_id,
            }

        if action == "scan_m3u_epg":
            job_id = _start_background_job(
                "scan_m3u_epg",
                plugin_dir,
                import_m3u_epg_sources,
                logger=logger,
                settings=settings,
                plugin_dir=plugin_dir,
                payload={},
            )
            return {
                "status": "queued",
                "message": f"M3U 头部 EPG 扫描任务已提交，任务 ID：{job_id}",
                "task_id": job_id,
            }

        if action == "auto_m3u_refresh":
            if not _truthy(settings.get("auto_on_m3u_refresh")):
                return {
                    "status": "skipped",
                    "message": "M3U 刷新后自动执行未启用，已跳过。",
                }
            delay_minutes = _non_negative_int(
                settings.get("m3u_refresh_delay_minutes"), 3
            )
            payload = params.get("payload", {}) if isinstance(params, dict) else {}
            job_id = _start_background_job(
                "auto_m3u_refresh",
                plugin_dir,
                handle_m3u_refresh_job,
                logger=logger,
                delay_seconds=delay_minutes * 60,
                settings=settings,
                plugin_dir=plugin_dir,
                payload=payload,
            )
            return {
                "status": "queued",
                "message": (
                    f"M3U 刷新成功事件已接收，将在 {delay_minutes} 分钟后扫描 EPG 并执行匹配，"
                    f"任务 ID：{job_id}"
                ),
                "task_id": job_id,
            }

        if action == "latest_result":
            try:
                result = read_latest_result(plugin_dir)
            except Exception as exc:
                return {
                    "status": "error",
                    "message": f"读取最近结果失败：{exc}",
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


def _start_background_job(
    job_name: str,
    result_dir: str,
    target,
    *,
    logger=None,
    delay_seconds: int = 0,
    **kwargs,
) -> str:
    job_id = str(uuid.uuid4())
    job_kwargs = dict(kwargs)
    delay_seconds = max(int(delay_seconds or 0), 0)
    _write_last_result(
        result_dir,
        {
            "status": "running" if delay_seconds == 0 else "waiting",
            "message": (
                f"{_job_display_name(job_name)}任务"
                f"{'正在后台运行' if delay_seconds == 0 else f'已排队，将在 {delay_seconds} 秒后运行'}。"
                f"任务 ID：{job_id}"
            ),
            "job_id": job_id,
            "job_name": job_name,
            "started_at": _now_label(),
        },
    )

    def runner():
        try:
            from django.db import close_old_connections

            close_old_connections()
        except Exception:
            pass

        if delay_seconds > 0:
            time.sleep(delay_seconds)
            _write_last_result(
                result_dir,
                {
                    "status": "running",
                    "message": (
                        f"{_job_display_name(job_name)}任务正在后台运行。"
                        f"任务 ID：{job_id}"
                    ),
                    "job_id": job_id,
                    "job_name": job_name,
                    "started_at": _now_label(),
                },
            )

        try:
            progress_log_state = {"last_at": 0.0, "last_message": ""}

            def progress_callback(current, total, message, extra=None):
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

            result = target(progress_callback=progress_callback, **job_kwargs)
            if isinstance(result, dict):
                completed = dict(result)
                completed["job_id"] = job_id
                completed["job_name"] = job_name
                completed["finished_at"] = _now_label()
                _write_last_result(result_dir, completed)
                if logger:
                    logger.info(
                        "%s job %s completed: %s",
                        PLUGIN_KEY,
                        job_id,
                        completed.get("message", "done"),
                    )
            else:
                completed = {
                    "status": "ok",
                    "message": (
                        f"{_job_display_name(job_name)}任务完成。"
                        f"任务 ID：{job_id}"
                    ),
                    "job_id": job_id,
                    "job_name": job_name,
                    "finished_at": _now_label(),
                }
                _write_last_result(result_dir, completed)
                if logger:
                    logger.info(
                        "%s job %s completed: %s",
                        PLUGIN_KEY,
                        job_id,
                        completed["message"],
                    )
        except Exception as exc:
            if logger:
                logger.exception("%s background job %s failed", PLUGIN_KEY, job_id)
            try:
                _write_last_result(
                    result_dir,
                    {
                        "status": "error",
                        "message": f"{job_name} 后台任务失败：{exc}",
                        "job_id": job_id,
                    },
                )
            except Exception:
                if logger:
                    logger.exception("%s failed to write error report", PLUGIN_KEY)
        finally:
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
    return job_id


def _job_display_name(job_name: str) -> str:
    names = {
        "preview_match": "预览规则匹配",
        "apply_match": "立即执行规则匹配",
        "sort_existing_streams": "重排已挂流",
        "scan_m3u_epg": "扫描 M3U EPG",
        "auto_m3u_refresh": "M3U 刷新后自动执行",
    }
    return names.get(job_name, job_name)


def handle_m3u_refresh_job(
    settings: Dict[str, Any],
    plugin_dir: str,
    payload: Dict[str, Any],
    progress_callback=None,
):
    if progress_callback:
        progress_callback(0, 2, "正在扫描 M3U 头部 EPG")
    epg_summary = import_m3u_epg_sources(
        settings, plugin_dir, payload, progress_callback=progress_callback
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
            planned, merge_skipped = _plan_merge_with_details(existing_links, deduped)
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

        if dry_run or not planned and rule.mode != "replace":
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
                else:
                    start_order = len(existing_links)
                    links = [
                        ChannelStream(
                            channel=channel,
                            stream=stream,
                            order=start_order + index,
                        )
                        for index, stream in enumerate(planned)
                    ]
                    ChannelStream.objects.bulk_create(links, ignore_conflicts=True)
                    summary["streams_added"] += len(links)
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
        f"错误 {summary['errors']} 条。报告：{file_path}"
    )
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
        sorted_links = _sort_channel_stream_links(links, settings)
        changed_links = []
        for new_order, link in enumerate(sorted_links):
            if link.order != new_order:
                link.order = new_order
                changed_links.append(link)

        if changed_links:
            try:
                with transaction.atomic():
                    ChannelStream.objects.bulk_update(changed_links, ["order"])
                summary["channels_changed"] += 1
                summary["links_reordered"] += len(changed_links)
                report_lines.append(
                    f"[channel {channel.id}] reordered {len(changed_links)}/{len(links)} "
                    f"links: {channel.name!r}"
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
        f"更新顺序 {summary['links_reordered']} 条，"
        f"错误 {summary['errors']} 条。报告：{file_path}"
    )
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
    stream_to_links: Dict[int, List[Any]] = {}
    stream_items = []
    for link in links:
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
        channel = Channel.objects.filter(name=name).order_by("id").first()
        if channel:
            return channel
    if ref.isdigit():
        channel = Channel.objects.filter(id=int(ref)).first()
        if channel:
            return channel
    name = ref
    if not name:
        return None
    return Channel.objects.filter(name=name).order_by("id").first()


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
        "progress": progress,
    }
    if extra:
        payload["summary"] = dict(extra)
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
    tmp_path = f"{path}.tmp"
    payload = dict(result)
    payload["updated_at"] = _now_label()
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


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
