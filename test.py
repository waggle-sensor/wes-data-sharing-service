import unittest
from main import match_plugin_user_id


class TestMain(unittest.TestCase):

    def test_match_plugin_user_id(self):
        tests = [
            ("plugin.plugin-metsense:1.2.3", "plugin-metsense:1.2.3"),
            ("plugin.plugin-raingauge-1-2-3-ae43fc12", "plugin-raingauge:1.2.3"),
            ("plugin.hello-world-4-0-2-aabbccdd", "hello-world:4.0.2"),
        ]

        for user_id, want in tests:
            self.assertEqual(match_plugin_user_id(user_id), want)


if __name__ == "__main__":
    unittest.main()
