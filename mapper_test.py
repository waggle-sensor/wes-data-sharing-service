import unittest
import time
import mapper

class TestMapper(unittest.TestCase):

    def test_end_to_end(self):
        test_cases = [
            {
                'ts': 1600973660233210000,
                'value': 31.2,
                'topic': 'env/temperature/tmp112',
                'scope': 'beehive',
                'plugin': 'simple:0.1.0',
            },
            {
                'ts': 1600973661327352000,
                'value': 20.7,
                'topic': 'env/temperature/htu21d',
                'scope': 'beehive',
                'plugin': 'simple:0.2.0',
            },
            {
                'ts': 1600973661327352000,
                'value': 96.1,
                'topic': 'env/humidity/htu21d',
                'scope': 'beehive',
                'plugin': 'simple:0.2.1',
            },
            {
                'ts': 1600973662457836000,
                'value': 0.1,
                'topic': 'env/humidity/hih4030',
                'scope': 'beehive',
                'plugin': 'simple:0.2.1',
            },
        ]

        for msg in test_cases:
            out = mapper.waggle_to_local(mapper.local_to_waggle(msg))
            self.assertEqual(msg['topic'], out['topic'])
            self.assertEqual(msg['plugin'], out['plugin'])
            self.assertAlmostEqual(msg['value'], out['value'], places=5)
            # NOTE waggle protocol only support second granularity right now, but
            # intra-node format is using nanoseconds. this test is known to fail.
            self.assertEqual(msg['ts']//1e9, out['ts']//1e9)


if __name__ == '__main__':
    unittest.main()
