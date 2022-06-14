import uuid
from typing import List

import pytest
import aioredis

from aiobus.redis import RedisBus


@pytest.mark.asyncio
async def test_connection(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    # check single connection
    topic = 'topic-' + uuid.uuid4().hex
    async with bus.connection(topic) as redis:
        assert isinstance(redis, aioredis.Redis)
        pong = await redis.ping()
        assert pong is True


@pytest.mark.asyncio
async def test_connection_pool(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    # Check connection pool
    topic = 'topic-' + uuid.uuid4().hex
    async with bus.connection_pool(topic) as redis:
        assert isinstance(redis, aioredis.Redis)
        pong = await redis.ping()
        assert pong is True


@pytest.mark.asyncio
async def test_pubsub(redis_servers: List[str]):
    bus = RedisBus(redis_servers)
    topic = 'topic-' + uuid.uuid4().hex
    await bus.subs_num(topic)
