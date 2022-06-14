import pytest

from aiobus.ring import HashRing


redis_servers = [
    '192.168.0.246:6379',
    '192.168.0.247:6379',
    '192.168.0.249:6379',
    '192.168.0.250:6379',
    '192.168.0.251:6379',
    '192.168.0.252:6379'
]


@pytest.mark.asyncio
async def test_get_node():
    ring = HashRing(redis_servers)
    server = ring.get_node('amir')
    assert server in redis_servers
    assert ring.get_node('amir') == ring.get_node('amir')


def test_iterate_nodes():
    simple_list = ['1', '2', '3', '4', '5']
    new_ring = HashRing(simple_list)

    nodes = []
    for node in new_ring.iterate_nodes('a'):
        nodes.append(node)

    assert len(nodes) == len(simple_list)
    for elm in simple_list:
        assert elm in nodes


def test_with_objects():

    class Server(object):

        def __init__(self, name):
            self.name = name

        def __str__(self):
            return str(self.name)

    simple_list = [Server(1), Server(2), Server(3)]

    new_ring = HashRing(simple_list)

    node = new_ring.get_node('BABU')
    assert node in simple_list

    nodes = []
    for node in new_ring.iterate_nodes('aloha'):
        nodes.append(node)

    assert len(nodes) == len(simple_list)
    for elm in simple_list:
        assert elm in nodes
