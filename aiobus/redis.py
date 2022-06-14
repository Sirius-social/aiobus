import json
import asyncio
from typing import Dict, List
import threading
from urllib.parse import urlparse
from contextlib import asynccontextmanager

import aioredis

from .base import AbstractBus
from .ring import HashRing


class RedisBus(AbstractBus):

    __thread_local = threading.local()

    def __init__(self, servers: List[str], max_pool_size: int = 1000):
        self.__servers = servers
        self.__max_pool_size = max_pool_size

    async def publish(self, topic: str, message: Dict):
        async with self.connection(topic) as conn:
            redis: aioredis.Redis = conn
            payload = json.dumps(message).encode()
            await redis.publish(topic, payload)

    async def subscribe(self, topic: str):
        async with self.connection(topic) as conn:
            redis: aioredis.Redis = conn
            pub = redis.pubsub()
            await pub.subscribe()

    async def subs_num(self, topic: str,) -> int:
        async with self.connection_pool(topic) as conn:
            redis: aioredis.Redis = conn
            pub = redis.pubsub()
            ret_val = await pub.execute_command("NUMSUB", topic)
        return ret_val or 0

    @asynccontextmanager
    async def connection(self, topic: str):
        redis = aioredis.Redis.from_url(url=self.__get_redis_url(topic), max_connections=1)
        try:
            yield redis
        finally:
            # release conn to pool
            await redis.close()

    @asynccontextmanager
    async def connection_pool(self, topic: str):
        cur_loop_id = id(asyncio.get_event_loop())
        try:
            pools = self.__thread_local.pools
        except AttributeError:
            pools = {}
            self.__thread_local.pools = pools
        url = self.__get_redis_url(topic)
        key = f'{cur_loop_id}:{url}'
        pool = pools.get(key, aioredis.ConnectionPool.from_url(url, max_connections=self.__max_pool_size))
        self.__thread_local.pools[url] = pool
        redis = aioredis.Redis(connection_pool=pool)
        try:
            yield redis
        finally:
            # release conn to pool
            await redis.close()

    def __get_redis_url(self, topic: str) -> str:
        addr = HashRing(self.__servers).get_node(topic)
        parts = urlparse(addr)
        if not parts.scheme:
            url = 'redis://' + addr
        else:
            url = addr
        return url
