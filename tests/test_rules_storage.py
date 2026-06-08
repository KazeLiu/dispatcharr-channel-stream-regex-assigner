import os
import tempfile
import threading
import time
import unittest

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


if __name__ == "__main__":
    unittest.main()
