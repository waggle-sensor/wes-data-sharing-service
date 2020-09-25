import time
import random
import json
import mapper


def pub(msg):
    print('msg', msg)
    send = mapper.local_to_waggle(msg)
    print('send', send)
    recv = mapper.waggle_to_local(send)
    print('recv', recv)
    print()


def main():
    pub({
        'ts': time.time_ns(),
        'value': 31.2,
        'topic': 'env/temperature/tmp112',
        'scope': 'beehive',
        'plugin': 'simple:0.1.0',
        'tags': {},
    })

    pub({
        'ts': time.time_ns(),
        'value': b'12da',
        'topic': 'raw/tmp112',
        'scope': 'beehive',
        'plugin': 'simple:0.1.0',
        'tags': {},
    })


if __name__ == '__main__':
    main()
