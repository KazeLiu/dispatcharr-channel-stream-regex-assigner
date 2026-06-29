import os
import tempfile
import threading
import time
import unittest
from datetime import datetime

import plugin


class FakeQuerySet:
    def __init__(self, items):
        self.items = list(items)

    def first(self):
        return self.items[0] if self.items else None

    def order_by(self, *_fields):
        return self


class FakeChannelManager:
    def __init__(self, channels):
        self.channels = list(channels)

    def filter(self, **kwargs):
        items = self.channels
        if "id" in kwargs:
            items = [channel for channel in items if channel.id == kwargs["id"]]
        if "name" in kwargs:
            items = [channel for channel in items if channel.name == kwargs["name"]]
        return FakeQuerySet(items)


class FakeChannelModel:
    def __init__(self, channels):
        self.objects = FakeChannelManager(channels)


class FakeChannel:
    def __init__(self, channel_id, name):
        self.id = channel_id
        self.name = name


class FakeStreamAccount:
    def __init__(self, name):
        self.name = name


class FakeStream:
    def __init__(self, stream_id, name, source_name, is_stale=False, url=""):
        self.id = stream_id
        self.name = name
        self.is_stale = is_stale
        self.url = url
        self.m3u_account = FakeStreamAccount(source_name)


class FakeChannelStreamLink:
    def __init__(self, link_id, stream, order=0):
        self.id = link_id
        self.stream = stream
        self.order = order


class RulesStorageTests(unittest.TestCase):
    def test_rules_file_path_uses_upgrade_safe_plugin_data_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(
                tmp, "plugins", plugin.PLUGIN_KEY
            )
            os.makedirs(plugin_dir)

            rules_path = plugin._rules_file_path(plugin_dir)

            self.assertEqual(
                rules_path,
                os.path.join(
                    tmp,
                    "plugin_data",
                    plugin.PLUGIN_KEY,
                    plugin.RULES_FILE_NAME,
                ),
            )

    def test_ensure_rules_file_migrates_legacy_exports_file_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(
                tmp, "plugins", plugin.PLUGIN_KEY
            )
            legacy_dir = os.path.join(plugin_dir, plugin.EXPORT_DIR)
            os.makedirs(legacy_dir)
            legacy_path = os.path.join(
                legacy_dir, plugin.LEGACY_RULES_TEMPLATE_FILENAME
            )
            with open(legacy_path, "w", encoding="utf-8") as fh:
                fh.write("123 ||| CCTV ||| ^CCTV$ ||| merge ||| 0\n")

            migrated_path = plugin._ensure_rules_file(plugin_dir)

            self.assertEqual(
                migrated_path,
                os.path.join(
                    tmp,
                    "plugin_data",
                    plugin.PLUGIN_KEY,
                    plugin.RULES_FILE_NAME,
                ),
            )
            with open(migrated_path, "r", encoding="utf-8") as fh:
                self.assertEqual(
                    fh.read(), "123 ||| CCTV ||| ^CCTV$ ||| merge ||| 0\n"
                )

            with open(legacy_path, "w", encoding="utf-8") as fh:
                fh.write("legacy changed after migration\n")

            second_path = plugin._ensure_rules_file(plugin_dir)

            self.assertEqual(second_path, migrated_path)
            with open(second_path, "r", encoding="utf-8") as fh:
                self.assertEqual(
                    fh.read(), "123 ||| CCTV ||| ^CCTV$ ||| merge ||| 0\n"
                )

    def test_ensure_rules_file_migrates_legacy_plugin_data_file_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(
                tmp, "plugins", plugin.PLUGIN_KEY
            )
            os.makedirs(plugin_dir)
            legacy_data_dir = os.path.join(
                tmp, "plugin_data", plugin.PLUGIN_KEY
            )
            os.makedirs(legacy_data_dir)
            legacy_path = os.path.join(
                legacy_data_dir, plugin.LEGACY_RULES_TEMPLATE_FILENAME
            )
            with open(legacy_path, "w", encoding="utf-8") as fh:
                fh.write("234 ||| GDTV ||| ^GDTV$ ||| merge ||| 0\n")

            migrated_path = plugin._ensure_rules_file(plugin_dir)

            self.assertEqual(
                migrated_path,
                os.path.join(legacy_data_dir, plugin.RULES_FILE_NAME),
            )
            with open(migrated_path, "r", encoding="utf-8") as fh:
                self.assertEqual(
                    fh.read(), "234 ||| GDTV ||| ^GDTV$ ||| merge ||| 0\n"
                )

    def test_ensure_rules_file_uses_packaged_seed_when_no_user_file_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(
                tmp, "plugins", plugin.PLUGIN_KEY
            )
            os.makedirs(plugin_dir)
            seed_path = os.path.join(plugin_dir, plugin.RULES_FILE_NAME)
            with open(seed_path, "w", encoding="utf-8") as fh:
                fh.write("456 ||| Hunan ||| ^Hunan$ ||| merge ||| 0\n")

            rules_path = plugin._ensure_rules_file(plugin_dir)

            with open(rules_path, "r", encoding="utf-8") as fh:
                self.assertEqual(
                    fh.read(), "456 ||| Hunan ||| ^Hunan$ ||| merge ||| 0\n"
                )

    def test_find_channel_falls_back_to_ultra_clear_suffix_name(self):
        Channel = FakeChannelModel(
            [
                FakeChannel(1, "CCTV10超清"),
                FakeChannel(2, "湖南卫视超清"),
            ]
        )

        cctv = plugin._find_channel(Channel, "11440", "CCTV10")
        province = plugin._find_channel(Channel, "", "湖南卫视")

        self.assertEqual(cctv.name, "CCTV10超清")
        self.assertEqual(province.name, "湖南卫视超清")

    def test_find_channel_suffix_fallback_does_not_cross_match_cctv_numbers(self):
        Channel = FakeChannelModel([FakeChannel(10, "CCTV10超清")])

        channel = plugin._find_channel(Channel, "", "CCTV1")

        self.assertIsNone(channel)

    def test_write_last_result_handles_parallel_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            errors = []

            def write_many(worker_id):
                try:
                    for index in range(50):
                        plugin._write_last_result(
                            plugin_dir,
                            {
                                "status": "running",
                                "worker_id": worker_id,
                                "index": index,
                            },
                        )
                except Exception as exc:
                    errors.append(exc)

            threads = [
                threading.Thread(target=write_many, args=(worker_id,))
                for worker_id in range(8)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            latest = plugin.read_latest_result(plugin_dir)
            self.assertEqual(latest["status"], "running")

    def test_reserve_background_job_blocks_when_another_job_is_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)

            first = plugin._reserve_background_job(plugin_dir, "apply_match", 0)
            second = plugin._reserve_background_job(plugin_dir, "sort_existing_streams", 0)

            self.assertEqual(first["status"], "queued")
            self.assertEqual(second["status"], "blocked")
            self.assertEqual(second["active_job"]["job_name"], "apply_match")

    def test_reserve_background_job_allows_stale_running_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin._write_job_state(
                plugin_dir,
                {
                    "status": "running",
                    "job_id": "old-job",
                    "job_name": "apply_match",
                    "message": "old job",
                    "updated_at_ts": time.time() - plugin.JOB_STATE_STALE_SECONDS - 1,
                },
            )

            result = plugin._reserve_background_job(plugin_dir, "sort_existing_streams", 0)

            self.assertEqual(result["status"], "queued")
            self.assertNotEqual(result["job_id"], "old-job")

    def test_auto_m3u_refresh_event_coalesces_and_reruns_after_active_auto_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            first_started = threading.Event()
            finish_first = threading.Event()
            calls = []

            def target(settings, plugin_dir, payload, progress_callback=None):
                calls.append(dict(payload))
                if len(calls) == 1:
                    first_started.set()
                    finish_first.wait(2)
                return {"status": "ok", "message": "ok"}

            first = plugin._start_background_job(
                "auto_m3u_refresh",
                plugin_dir,
                target,
                settings={},
                plugin_dir=plugin_dir,
                payload={"account_name": "first"},
            )
            self.assertEqual(first["status"], "queued")
            self.assertTrue(first_started.wait(2))

            second = plugin._start_background_job(
                "auto_m3u_refresh",
                plugin_dir,
                target,
                settings={},
                plugin_dir=plugin_dir,
                payload={"account_name": "second"},
            )

            self.assertEqual(second["status"], "coalesced")
            active_job = plugin.read_active_job_state(plugin_dir)
            self.assertTrue(active_job["pending_auto_refresh"])
            self.assertEqual(active_job["pending_auto_refresh_count"], 1)

            finish_first.set()
            deadline = time.time() + 2
            latest = None
            while time.time() < deadline:
                latest = plugin.read_latest_result(plugin_dir)
                if len(calls) >= 2 and latest and latest.get("status") == "ok":
                    break
                time.sleep(0.01)

            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0], {"account_name": "first"})
            self.assertEqual(calls[1], {})
            self.assertEqual(latest["rerun_count"], 1)

    def test_progress_updates_preserve_pending_auto_refresh_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)

            first = plugin._reserve_background_job(plugin_dir, "auto_m3u_refresh", 0)
            plugin._reserve_background_job(plugin_dir, "auto_m3u_refresh", 0)

            plugin._write_progress(
                plugin_dir,
                first["job_id"],
                "auto_m3u_refresh",
                1,
                2,
                "正在处理",
            )

            active_job = plugin.read_active_job_state(plugin_dir)
            self.assertTrue(active_job["pending_auto_refresh"])
            self.assertEqual(active_job["pending_auto_refresh_count"], 1)

    def test_auto_m3u_refresh_blocked_by_manual_job_writes_visible_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin._reserve_background_job(plugin_dir, "apply_match", 0)

            result = plugin._start_background_job(
                "auto_m3u_refresh",
                plugin_dir,
                lambda **_kwargs: {"status": "ok"},
                settings={},
                plugin_dir=plugin_dir,
                payload={},
            )

            latest = plugin.read_latest_result(plugin_dir)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(latest["status"], "blocked")
            self.assertEqual(latest["job_name"], "auto_m3u_refresh")
            self.assertEqual(latest["blocked_by"]["job_name"], "apply_match")

    def test_read_active_job_state_returns_running_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin._write_job_state(
                plugin_dir,
                {
                    "status": "running",
                    "job_id": "current-job",
                    "job_name": "apply_match",
                    "message": "apply running",
                    "updated_at_ts": time.time(),
                },
            )

            active_job = plugin.read_active_job_state(plugin_dir)

            self.assertEqual(active_job["job_id"], "current-job")
            self.assertEqual(active_job["job_name"], "apply_match")


class ChannelLookupTests(unittest.TestCase):
    def test_find_channel_prefers_channel_name_over_foreign_id(self):
        channel_by_foreign_id = FakeChannel(100, "Not CCTV1")
        channel_by_name = FakeChannel(200, "CCTV1")
        Channel = FakeChannelModel([channel_by_foreign_id, channel_by_name])

        found = plugin._find_channel(Channel, "100", "CCTV1")

        self.assertIs(found, channel_by_name)

    def test_find_channel_uses_first_matching_name_when_duplicates_exist(self):
        first = FakeChannel(200, "CCTV1")
        second = FakeChannel(201, "CCTV1")
        Channel = FakeChannelModel([first, second])

        found = plugin._find_channel(Channel, "", "CCTV1")

        self.assertIs(found, first)

    def test_find_channel_falls_back_to_id_when_name_is_missing(self):
        channel = FakeChannel(100, "CCTV1")
        Channel = FakeChannelModel([channel])

        found = plugin._find_channel(Channel, "100", "")

        self.assertIs(found, channel)


class StreamSortingTests(unittest.TestCase):
    def test_order_channel_streams_for_merge_sorts_existing_and_planned_streams(self):
        links = [
            FakeChannelStreamLink(
                1,
                FakeStream(1, "Alpha News", "Alpha Source", is_stale=True, url="alpha"),
                0,
            ),
            FakeChannelStreamLink(2, FakeStream(2, "Beta News", "Beta Source", is_stale=False), 1),
        ]
        planned_streams = [
            FakeStream(3, "Alpha Sports", "Alpha Source", is_stale=False, url="alpha"),
        ]

        ordered_streams, active_links, removed_links = plugin._order_channel_streams_for_merge(
            links,
            planned_streams,
            {"stream_source_priority": "Alpha Source,Beta Source"},
        )

        self.assertEqual([stream.id for stream in ordered_streams], [3, 2])
        self.assertEqual([link.id for link in active_links], [2])
        self.assertEqual([link.id for link in removed_links], [1])

    def test_plan_merge_ignores_stale_existing_links_when_matching_url(self):
        links = [
            FakeChannelStreamLink(
                1,
                FakeStream(1, "Alpha News", "Alpha Source", is_stale=True, url="shared"),
                0,
            )
        ]
        planned_streams = [
            FakeStream(2, "Alpha News", "Alpha Source", is_stale=False, url="shared"),
        ]

        active_links, _removed = plugin._filter_channel_stream_links(
            links,
            {"remove_stale_existing_streams": True},
        )
        planned, skipped = plugin._plan_merge_with_details(active_links, planned_streams)

        self.assertEqual([stream.id for stream in planned], [2])
        self.assertEqual([stream.id for stream in skipped], [])

    def test_dedupe_keeps_priority_source_over_channel_name_match(self):
        # 同 URL 两条流：B 源(不在优先级, 名字恰好等于频道名) vs 咪咕(在优先级+含关键词)
        # 排序后咪咕超清应在前；去重必须保留它，不能被"名字==频道名"的隐式插队覆盖。
        shared_url = "http://same.example.com/cctv9.m3u8"
        streams = [
            FakeStream(100, "CCTV9", "B源", url=shared_url),
            FakeStream(200, "CCTV9超清", "咪咕", url=shared_url),
        ]
        settings = {
            "stream_keyword_priority": "超清",
            "stream_source_priority": "咪咕",
        }
        ordered = plugin._sort_streams_for_assignment(streams, settings)
        # 模拟主流程：传入频道名作为 preferred_name（历史实现会因此插队覆盖优先级）
        deduped, skipped = plugin._dedupe_streams_with_details(
            ordered, preferred_name="CCTV9"
        )

        self.assertEqual([stream.id for stream in ordered], [200, 100])
        self.assertEqual([stream.id for stream in deduped], [200])
        self.assertEqual([stream.id for stream in skipped], [100])

    def test_dedupe_keeps_first_by_url_when_no_priority_configured(self):
        # 未配置任何优先级时，按传入顺序（id 升序）先到先得，行为可预测。
        streams = [
            FakeStream(100, "X", "A", url="u"),
            FakeStream(200, "Y", "B", url="u"),
        ]
        deduped, skipped = plugin._dedupe_streams_with_details(streams)

        self.assertEqual([stream.id for stream in deduped], [100])
        self.assertEqual([stream.id for stream in skipped], [200])

class DailySchedulerTests(unittest.TestCase):
    def tearDown(self):
        plugin._stop_all_daily_schedulers_for_tests()

    def test_next_daily_run_uses_docker_local_time_today_when_time_is_future(self):
        now = datetime(2026, 6, 11, 3, 59, 0)

        next_run = plugin._next_daily_run_datetime(now, "04:30")

        self.assertEqual(next_run, datetime(2026, 6, 11, 4, 30, 0))

    def test_next_daily_run_uses_tomorrow_when_time_has_passed(self):
        now = datetime(2026, 6, 11, 4, 31, 0)

        next_run = plugin._next_daily_run_datetime(now, "04:30")

        self.assertEqual(next_run, datetime(2026, 6, 12, 4, 30, 0))

    def test_parse_daily_schedule_time_rejects_invalid_time(self):
        with self.assertRaises(ValueError):
            plugin._parse_daily_schedule_time("25:00")

    def test_start_daily_scheduler_writes_state_and_stop_marks_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)

            started = plugin.start_daily_scheduler(
                {"daily_schedule_enabled": True, "daily_schedule_time": "04:30"},
                plugin_dir,
            )
            stopped = plugin.stop_daily_scheduler(plugin_dir)

            self.assertEqual(started["status"], "ok")
            self.assertEqual(started["daily_schedule_time"], "04:30")
            self.assertEqual(stopped["status"], "stopped")
            latest = plugin.read_latest_result(plugin_dir)
            self.assertEqual(latest["status"], "stopped")
            self.assertEqual(latest["job_name"], "daily_scheduler")


class StopAllTasksTests(unittest.TestCase):
    def tearDown(self):
        plugin._stop_all_daily_schedulers_for_tests()

    def test_stop_all_tasks_cancels_active_job_and_clears_active_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin._write_job_state(
                plugin_dir,
                {
                    "status": "running",
                    "job_id": "active-job",
                    "job_name": "apply_match",
                    "message": "still running",
                    "updated_at_ts": time.time(),
                },
            )

            result = plugin.stop_all_tasks(plugin_dir)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["job_name"], "stop_all_tasks")
            self.assertIsNone(plugin.read_active_job_state(plugin_dir))
            latest = plugin.read_latest_result(plugin_dir)
            self.assertEqual(latest["status"], "ok")
            self.assertEqual(latest["job_name"], "stop_all_tasks")
            self.assertEqual(latest["canceled_active_job"]["status"], "canceled")
            self.assertEqual(latest["canceled_active_job"]["job_name"], "apply_match")

    def test_stop_all_tasks_stops_daily_scheduler(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin.start_daily_scheduler(
                {"daily_schedule_enabled": True, "daily_schedule_time": "04:30"},
                plugin_dir,
            )

            result = plugin.stop_all_tasks(plugin_dir)

            self.assertEqual(result["status"], "ok")
            scheduler_state = plugin.read_daily_scheduler_state(plugin_dir)
            self.assertEqual(scheduler_state["status"], "stopped")

    def test_background_job_writes_canceled_result_when_stop_all_tasks_is_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            started = threading.Event()

            def target(progress_callback=None, **_kwargs):
                started.set()
                while True:
                    plugin._raise_if_job_cancelled("apply_match", "job-1")
                    if progress_callback:
                        progress_callback(0, 1, "running")
                    time.sleep(0.01)

            plugin._start_background_job(
                "apply_match",
                plugin_dir,
                target,
                settings={},
                plugin_dir=plugin_dir,
            )
            self.assertTrue(started.wait(2))

            plugin.stop_all_tasks(plugin_dir)

            deadline = time.time() + 2
            latest = None
            while time.time() < deadline:
                latest = plugin.read_latest_result(plugin_dir)
                if latest and latest.get("status") == "canceled":
                    break
                time.sleep(0.01)

            self.assertIsNotNone(latest)
            self.assertEqual(latest["status"], "canceled")

    def test_active_job_from_other_process_does_not_block_new_job(self):
        with tempfile.TemporaryDirectory() as tmp:
            plugin_dir = os.path.join(tmp, "plugins", plugin.PLUGIN_KEY)
            os.makedirs(plugin_dir)
            plugin._write_job_state(
                plugin_dir,
                {
                    "status": "running",
                    "job_id": "old-process-job",
                    "job_name": "auto_m3u_refresh",
                    "message": "old process",
                    "runner_pid": -1,
                    "updated_at_ts": time.time(),
                },
            )

            result = plugin._reserve_background_job(plugin_dir, "scan_m3u_epg", 0)

            self.assertEqual(result["status"], "queued")


if __name__ == "__main__":
    unittest.main()
