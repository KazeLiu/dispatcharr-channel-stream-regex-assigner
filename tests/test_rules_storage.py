import os
import tempfile
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


if __name__ == "__main__":
    unittest.main()
