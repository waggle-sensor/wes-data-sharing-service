import time
import random
import json
import mapper

def main():
    msg = {
        'ts': time.time_ns(),
        'value': 31.2,
        'topic': 'env/temperature/tmp112',
        'scope': 'beehive',
        'plugin': 'simple:0.1.0',
        'tags': {},
    }
    print('msg', msg)

    send = mapper.local_to_waggle(msg)
    print('send', send)

    recv = mapper.waggle_to_local(send)
    print('recv', recv)


if __name__ == '__main__':
    main()
