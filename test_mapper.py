import unittest
import time
import mapper

class TestMapper(unittest.TestCase):

    def test_parse_version_str(self):
        self.assertEqual(mapper.parse_version_str('0.0.0'), (0, 0, 0))
        self.assertEqual(mapper.parse_version_str('1.2.3'), (1, 2, 3))
        self.assertEqual(mapper.parse_version_str('1.22.333'), (1, 22, 333))
        with self.assertRaises(ValueError):
            mapper.parse_version_str('')
        with self.assertRaises(ValueError):
            mapper.parse_version_str('1')
        with self.assertRaises(ValueError):
            mapper.parse_version_str('1.2')
        with self.assertRaises(ValueError):
            mapper.parse_version_str('1.2.3.4')
    
    def test_parse_plugin_name_version(self):
        self.assertEqual(
            mapper.parse_plugin_name_version('simple:1.2.3'),
            ('simple', (1, 2, 3)))
        self.assertEqual(
            mapper.parse_plugin_name_version('test:0.0.0'),
            ('test', (0, 0, 0)))
        with self.assertRaises(ValueError):
            mapper.parse_plugin_name_version('test')
        with self.assertRaises(ValueError):
            mapper.parse_plugin_name_version('test:')
        with self.assertRaises(ValueError):
            mapper.parse_plugin_name_version(':1.2.3')
        with self.assertRaises(ValueError):
            mapper.parse_plugin_name_version('test:1.2.3:')

    def test_end_to_end(self):
        test_cases = [
            {
                'ts': 1600973660233210000,
                'value': 31.2,
                'name': 'env.temperature.tmp112',
                'scope': 'beehive',
                'plugin': 'simple:0.1.0',
            },
            {
                'ts': 1600973661327352000,
                'value': 20.7,
                'name': 'env.temperature.htu21d',
                'scope': 'beehive',
                'plugin': 'simple:0.2.0',
            },
            {
                'ts': 1600973661327352000,
                'value': 96.1,
                'name': 'env.humidity.htu21d',
                'scope': 'beehive',
                'plugin': 'simple:0.2.1',
            },
            {
                'ts': 1600973662457836000,
                'value': 0.1,
                'name': 'env.humidity.hih4030',
                'scope': 'beehive',
                'plugin': 'simple:0.2.1',
            },
            {
                'ts': 1600973662457836000,
                'value': 0.1,
                'name': 'env.humidity.hih4030',
                'scope': 'beehive',
                'plugin': 'simple:0.2.1',
            },
            {
                'ts': 1609765905843405828,
                'value': 1,
                'name': 'env.count.car',
                'scope': 'beehive',
                'plugin': 'carped:1.2.3',
            },
            {
                'ts': 1609765905843405828,
                'value': 1,
                'name': 'env.count.pedestrian',
                'scope': 'beehive',
                'plugin': 'carped:0.1.0',
            },
        ]

        for msg in test_cases:
            out = mapper.waggle_to_local(mapper.local_to_waggle(msg))
            self.assertEqual(msg['name'], out['name'])
            self.assertEqual(msg['plugin'], out['plugin'])
            self.assertAlmostEqual(msg['value'], out['value'], places=5)
            # NOTE waggle protocol only support second granularity right now, but
            # intra-node format is using nanoseconds. this test is known to fail.
            self.assertEqual(msg['ts']//1e9, out['ts']//1e9)


if __name__ == '__main__':
    unittest.main()
