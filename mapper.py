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
    {'name': 'simple', 'waggle_id': 1},
    {'name': 'carped', 'waggle_id': 2},
    {'name': 'pub', 'waggle_id': 3},
    {'name': 'transform', 'waggle_id': 4},
    {'name': 'data-store', 'waggle_id': 5},
    {'name': 'test', 'waggle_id': 0xffff},
]

plugin_by_name = {r['name']: r for r in plugin_table}
plugin_by_waggle_id = {r['waggle_id']: r for r in plugin_table}

# NOTE This can *possibly* be generated automatically by ECR, but it may be better to curate a
# standard list of sensors.
sensor_table = [
    {'name': 'raw.tmp112', 'waggle_id': 0x0001, 'waggle_sub_id': 0, 'unit': '', 'type': bytes},
    {'name': 'env.temperature.tmp112', 'waggle_id': 0x0001, 'waggle_sub_id': 1, 'unit': 'C', 'type': float},
    {'name': 'raw.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 0, 'unit': 'C', 'type': bytes},
    {'name': 'env.temperature.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 1, 'unit': 'C', 'type': float},
    {'name': 'env.humidity.htu21d', 'waggle_id': 0x0002, 'waggle_sub_id': 2, 'unit': '%RH', 'type': float},
    {'name': 'env.humidity.hih4030', 'waggle_id': 0x0003, 'waggle_sub_id': 1, 'unit': '%RH', 'type': float},
    # test values for debugging
    {'name': 'test.int.1', 'waggle_id': 0xffff, 'waggle_sub_id': 1, 'unit': '', 'type': int},
    {'name': 'test.int.2', 'waggle_id': 0xffff, 'waggle_sub_id': 2, 'unit': '', 'type': int},
    {'name': 'test.int.3', 'waggle_id': 0xffff, 'waggle_sub_id': 3, 'unit': '', 'type': int},
    {'name': 'test.float.1', 'waggle_id': 0xfffe, 'waggle_sub_id': 1, 'unit': '', 'type': float},
    {'name': 'test.float.2', 'waggle_id': 0xfffe, 'waggle_sub_id': 2, 'unit': '', 'type': float},
    {'name': 'test.float.3', 'waggle_id': 0xfffe, 'waggle_sub_id': 3, 'unit': '', 'type': float},
    {'name': 'test.bytes.1', 'waggle_id': 0xfffd, 'waggle_sub_id': 1, 'unit': '', 'type': bytes},
    {'name': 'test.bytes.2', 'waggle_id': 0xfffd, 'waggle_sub_id': 2, 'unit': '', 'type': bytes},
    {'name': 'test.bytes.3', 'waggle_id': 0xfffd, 'waggle_sub_id': 3, 'unit': '', 'type': bytes},
    {'name': 'test.str.1', 'waggle_id': 0xfffc, 'waggle_sub_id': 1, 'unit': '', 'type': str},
    {'name': 'test.str.2', 'waggle_id': 0xfffc, 'waggle_sub_id': 2, 'unit': '', 'type': str},
    {'name': 'test.str.3', 'waggle_id': 0xfffc, 'waggle_sub_id': 3, 'unit': '', 'type': str},
]

sensor_by_name = {r['name']: r for r in sensor_table}
sensor_by_id_sub_id = {(r['waggle_id'], r['waggle_sub_id']): r for r in sensor_table}

def validate_type(v, t):
    if not isinstance(v, t):
        raise TypeError(f'Value type {type(v)} does not match schema type {t}.')


def parse_version_str(s):
    fields = s.split('.')
    if len(fields) != 3:
        raise ValueError('version string must have form "x.y.z"')
    return tuple(map(int, fields))


def parse_plugin_name_version(s):
    fields = s.split(':')
    if len(fields) != 2:
        raise ValueError('plugin string must have form "name:version"')
    if len(fields[0]) == 0:
        raise ValueError('plugin name must be non empty')
    return fields[0], parse_version_str(fields[1])


# transform intra-node format to waggle protocol
def local_to_waggle(msg):
    sensor = sensor_by_name[msg['name']]
    name, version = parse_plugin_name_version(msg['plugin'])
    plugin = plugin_by_name[name]
    validate_type(msg['value'], sensor['type'])
    return pack_message({
        'sender_id': WAGGLE_NODE_ID,
        'sender_sub_id': WAGGLE_NODE_SUB_ID,
        'body': pack_datagram({
            'plugin_id': plugin['waggle_id'],
            'plugin_major_version': version[0],
            'plugin_minor_version': version[1],
            'plugin_patch_version': version[2],
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
    d = unpack_datagram(m['body'])
    s = unpack_sensorgram(d['body'])
    sensor = sensor_by_id_sub_id[(s['id'], s['sub_id'])]
    plugin_id = d['plugin_id']
    plugin_version = (d['plugin_major_version'], d['plugin_minor_version'], d['plugin_patch_version'])
    plugin = plugin_by_waggle_id[plugin_id]
    validate_type(s['value'], sensor['type'])
    return {
        'ts': int(s['timestamp'] * 1e9),
        'value': s['value'],
        'name': sensor['name'],
        'plugin': plugin['name'] + ':' + '.'.join(map(str, plugin_version)),
    }

# NOTE Detailed hardware config should be tracked in the cloud. I don't see any reasonable way
# to capture all possible info as a short set of tags.
