from waggle.protocol import pack_sensorgram, unpack_sensorgram
from waggle.protocol import pack_datagram, unpack_datagram
from waggle.protocol import pack_message, unpack_message
import os

WAGGLE_NODE_ID = os.environ.get('WAGGLE_NODE_ID', '0000000000000000')
WAGGLE_NODE_SUB_ID = os.environ.get('WAGGLE_NODE_SUB_ID', '0000000000000000')

# NOTE When I say ECR, I really mean, some service to manage SDF / PDF.

# NOTE This can be generated entirely by the ECR! The user technically never needs to be involved
# in requesting a plugin ID since `repo:tag` uniquely defines the plugin.
#
# Should *really* rethink if we want to split id / version at protocol level instead of just name.
plugin_table = [
    {'name': 'simple:0.1.0', 'waggle_id': 1, 'waggle_version': (0, 1, 0)},
    {'name': 'simple:0.2.0', 'waggle_id': 1, 'waggle_version': (0, 2, 0)},
    {'name': 'simple:0.2.1', 'waggle_id': 1, 'waggle_version': (0, 2, 1)},
    {'name': 'carped:0.1.0', 'waggle_id': 2, 'waggle_version': (0, 1, 0)},
    {'name': 'carped:0.2.0', 'waggle_id': 2, 'waggle_version': (0, 2, 0)},
]

plugin_by_name = {r['name']: r for r in plugin_table}
plugin_by_waggle_id_version = {(r['waggle_id'], r['waggle_version']): r for r in plugin_table}

# NOTE This can *possibly* be generated automatically by ECR, but it may be better to curate a
# standard list of sensors.
sensor_table = [
    {'name': 'raw.tmp112', 'waggle_id': 0x0001, 'waggle_sub_id': 0, 'unit': '', 'type': str},
    {'name': 'env.temperature.tmp112', 'waggle_id': 0x0001, 'waggle_sub_id': 1, 'unit': 'C', 'type': float},
    {'name': 'raw.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 0, 'unit': 'C', 'type': str},
    {'name': 'env.temperature.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 1, 'unit': 'C', 'type': float},
    {'name': 'env.humidity.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 2, 'unit': '%RH', 'type': float},
    {'name': 'env.humidity.hih4030', 'waggle_id': 0x0003, 'waggle_sub_id': 1, 'unit': '%RH', 'type': float},
]

sensor_by_name = {r['name']: r for r in sensor_table}
sensor_by_id_sub_id = {(r['waggle_id'], r['waggle_sub_id']): r for r in sensor_table}

def validate_type(v, t):
    if not isinstance(v, t):
        raise TypeError(f'Value type {type(v)} does not match schema type {t}.')

# transform intra-node format to waggle protocol
def local_to_waggle(msg):
    sensor = sensor_by_name[msg['name']]
    plugin = plugin_by_name[msg['plugin']]
    validate_type(msg['value'], sensor['type'])
    return pack_message({
        'sender_id': WAGGLE_NODE_ID,
        'sender_sub_id': WAGGLE_NODE_SUB_ID,
        'body': pack_datagram({
            'plugin_id': plugin['waggle_id'],
            'plugin_major_version': plugin['waggle_version'][0],
            'plugin_minor_version': plugin['waggle_version'][1],
            'plugin_patch_version': plugin['waggle_version'][2],
            'body': pack_sensorgram({
                'timestamp': int(msg['ts']//1e9),
                'id': sensor['waggle_id'],
                'sub_id': sensor['waggle_sub_id'],
                'value': msg['value'],
            })
        })
    })


# transform waggle protocol to intra-node format
def waggle_to_local(data):
    m = unpack_message(data)
    print(m)
    d = unpack_datagram(m['body'])
    s = unpack_sensorgram(d['body'])
    sensor = sensor_by_id_sub_id[(s['id'], s['sub_id'])]
    plugin_id = d['plugin_id']
    plugin_version = (d['plugin_major_version'], d['plugin_minor_version'], d['plugin_patch_version'])
    plugin = plugin_by_waggle_id_version[(plugin_id, plugin_version)]
    validate_type(s['value'], sensor['type'])
    return {
        'ts': int(s['timestamp'] * 1e9),
        'value': s['value'],
        'name': sensor['name'],
        'plugin': plugin['name'],
    }

# NOTE Detailed hardware config should be tracked in the cloud. I don't see any reasonable way
# to capture all possible info as a short set of tags.
