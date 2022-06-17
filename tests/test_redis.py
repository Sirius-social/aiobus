import asyncio
import uuid
import datetime
from typing import List

import pytest
import aioredis

from aiobus.redis import RedisBus

from .helpers import TcpProxy


@pytest.mark.asyncio
async def test_connection(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    # check single connection
    for addr in redis_servers:
        async with bus.connection('redis://' + addr) as redis:
            assert isinstance(redis, aioredis.Redis)
            pong = await redis.ping()
            assert pong is True


@pytest.mark.asyncio
async def test_connection_pool(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    # Check connection pool
    for addr in redis_servers:
        async with bus.connection_pool('redis://' + addr) as redis:
            assert isinstance(redis, aioredis.Redis)
            pong = await redis.ping()
            assert pong is True


@pytest.mark.asyncio
async def test_subscribe(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic = 'topic-' + uuid.uuid4().hex

    __read_messages = []
    __pub_messages = [
        {'id': uuid.uuid4().hex, 'done': False},
        {'id': uuid.uuid4().hex, 'done': False},
        {'id': uuid.uuid4().hex, 'done': True}
    ]

    async def subscriber():
        await bus.subscribe(topic)
        async for _msg in await bus.listen():
            __read_messages.append(_msg)
            if _msg.get('done') is True:
                return
        await bus.unsubscribe(topic)

    async def publisher():
        await asyncio.sleep(1)
        for _msg in __pub_messages:
            counter = await bus.publish(topic, _msg)
            assert counter == 1

    done, pending = await asyncio.wait([subscriber(), publisher()], timeout=50)
    assert len(done) == 2
    assert len(pending) == 0
    assert len(__read_messages) == len(__pub_messages)
    assert all(expected == actual for expected, actual in zip(__pub_messages, __read_messages))


@pytest.mark.asyncio
async def test_subscribe_multiple(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic1 = 'topic-' + uuid.uuid4().hex
    topic2 = 'topic-' + uuid.uuid4().hex

    __read_messages = []
    __pub_messages = [
        {'id': uuid.uuid4().hex, 'done': False},
        {'id': uuid.uuid4().hex, 'done': False},
        {'id': uuid.uuid4().hex, 'done': True}
    ]

    async def subscriber():
        __done = 0
        await bus.subscribe(topic1, topic2)
        async for _msg in await bus.listen():
            __read_messages.append(_msg)
            if _msg.get('done') is True:
                __done += 1
                if __done == 2:
                    return
        await bus.unsubscribe()

    async def publisher(topic: str):
        await asyncio.sleep(1)
        for _msg in __pub_messages:
            pub_msg = dict(**_msg)
            pub_msg['topic'] = topic
            await bus.publish(topic, pub_msg)

    done, pending = await asyncio.wait([subscriber(), publisher(topic1), publisher(topic2)], timeout=50)
    assert len(done) == 3
    assert len(pending) == 0
    # Check total Count
    assert len(__read_messages) == 2*len(__pub_messages)
    # Check sequences order
    topic1_messages = [msg for msg in __read_messages if msg['topic'] == topic1]
    topic2_messages = [msg for msg in __read_messages if msg['topic'] == topic1]
    assert all(expected['id'] == actual['id'] for expected, actual in zip(__pub_messages, topic1_messages))
    assert all(expected['id'] == actual['id'] for expected, actual in zip(__pub_messages, topic2_messages))


@pytest.mark.asyncio
async def test_sub_num(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic = 'topic-' + uuid.uuid4().hex
    msg = {'id': uuid.uuid4().hex}

    async def subscriber1():
        print("Subscriber 1")
        await bus.subscribe(topic)
        async for _msg in await bus.listen():
            print(str(_msg))
        await bus.unsubscribe(topic)

    async def subscriber2():
        print("Subscriber 2")
        await bus.subscribe(topic)
        async for _msg in await bus.listen():
            print(str(_msg))
        await bus.unsubscribe(topic)

    sub1 = asyncio.ensure_future(subscriber1())
    try:
        await asyncio.sleep(0.1)
        count = await bus.publish(topic, msg)
        assert count == 1
        sub2 = asyncio.ensure_future(subscriber2())
        try:
            await asyncio.sleep(0.1)
            count = await bus.publish(topic, msg)
            assert count == 2
        finally:
            sub2.cancel()
    finally:
        sub1.cancel()


@pytest.mark.asyncio
async def test_unsubscribe(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic = 'topic-' + uuid.uuid4().hex

    async def __async_publisher(delay: float = 0.1):
        while True:
            await bus.publish(topic, {'stamp': str(datetime.datetime.now())})
            await asyncio.sleep(delay)

    fut = asyncio.ensure_future(__async_publisher())

    try:
        await bus.subscribe(topic)
        listener = await bus.listen()
        msg1 = await listener.get_one(timeout=1)
        assert msg1 is not None

        await bus.unsubscribe(topic)
        msg2 = await listener.get_one(timeout=1)
        assert msg2 is None
    finally:
        fut.cancel()


@pytest.mark.asyncio
async def test_unsubscribe_multiple(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic1 = 'topic-1-' + uuid.uuid4().hex
    topic2 = 'topic-2-' + uuid.uuid4().hex

    async def __async_publisher(delay: float = 0.1):
        while True:
            for topic in [topic1, topic2]:
                await bus.publish(topic, {'stamp': str(datetime.datetime.now()), 'topic': topic})
            await asyncio.sleep(delay)

    fut = asyncio.ensure_future(__async_publisher())

    try:
        await bus.subscribe(topic1, topic2)
        listener = await bus.listen()
        income_messages = []
        for n in range(10):
            msg = await listener.get_one(timeout=1)
            income_messages.append(msg)
        assert any([msg['topic'] == topic1 for msg in income_messages])
        assert any([msg['topic'] == topic2 for msg in income_messages])
        # Unsubscribe specific topic only
        await bus.unsubscribe(topic1)
        income_messages = []
        for n in range(10):
            msg = await listener.get_one(timeout=1)
            income_messages.append(msg)
        assert all([msg['topic'] == topic2 for msg in income_messages])
        # Unsubscribe all topics
        await bus.unsubscribe()
        msg = await listener.get_one(timeout=1)
        assert msg is None
    finally:
        fut.cancel()
