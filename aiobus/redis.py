import json
import asyncio
import threading
from typing import Dict, List, Any, Optional
from urllib.parse import urlparse
from contextlib import asynccontextmanager

import aioredis

from .base import AbstractBus, AbstractListener
from .ring import HashRing
from .aiocontext import get as context_get, set as context_set
from .errors import ListenError, SubscriberError


class RedisBus(AbstractBus):

    __thread_local = threading.local()
    __ctx_sub_key = 'aiobus.subscribers'

    class RedisListener(AbstractListener):

        def __init__(self, subscribers: Dict[str, aioredis.client.PubSub]):
            self.__subscribers: Dict[str, aioredis.client.PubSub] = subscribers
            self.__cached = []
            self.__in_loop = False

        async def get_one(self, timeout: float = None) -> Any:
            if self.__cached:
                return self.__cached.pop()
            read_routines = [self.__reader(pub) for pub in self.__subscribers.values()]
            if len(read_routines) == 0:
                if self.__in_loop:
                    raise StopAsyncIteration
                else:
                    return None
            done, pending = await asyncio.wait(read_routines, timeout=timeout)
            for fut in done:
                self.__cached.append(fut.result())
            if self.__cached:
                return self.__cached.pop()
            else:
                return None

        async def is_active(self) -> bool:
            if len(self.__subscribers) > 0:
                return any([sub.subscribed for sub in self.__subscribers.values()])
            else:
                return False

        def _start_loop(self):
            self.__in_loop = True

        def _stop_loop(self):
            self.__in_loop = False

        async def __reader(self, sub: aioredis.client.PubSub) -> Any:
            sub.ignore_subscribe_messages = True
            async for event in sub.listen():
                if event['type'] == 'message':
                    payload = event['data']
                    msg = json.loads(payload.decode())
                    return msg

    def __init__(self, servers: List[str], max_pool_size: int = 1000):
        self.__servers = servers
        self.__max_pool_size = max_pool_size

    async def publish(self, topic: str, message: Dict) -> Optional[int]:
        url = self.__get_redis_url(topic)
        async with self.connection(url) as conn:
            redis: aioredis.Redis = conn
            payload = json.dumps(message).encode()
            counter = await redis.publish(topic, payload)
            return counter

    async def subscribe(self, *topics: str):
        if len(topics) == 0:
            raise SubscriberError('Topic list is Empty!')
        subscribers = context_get(self.__ctx_sub_key, default={})
        try:
            for topic in topics:
                success = await self.__subscribe_to_topic(subscribers, topic)
                if not success:
                    raise SubscriberError(f'Error while try to subscribe to topic "{topic}"')
        finally:
            context_set(self.__ctx_sub_key, subscribers)

    async def unsubscribe(self, *topics: str):
        subscribers = context_get(self.__ctx_sub_key, default={})
        try:
            if len(topics) == 0:
                topics = list(subscribers.keys())
            for topic in topics:
                if topic in subscribers.keys():
                    pub = subscribers[topic]
                    del subscribers[topic]
                    await pub.close()
        finally:
            context_set(self.__ctx_sub_key, subscribers)

    async def listen(self) -> AbstractListener:
        subscribers = context_get(self.__ctx_sub_key, default={})
        if not subscribers:
            raise ListenError('You should subscribe for some topic before to listen them!')
        listener = self.RedisListener(subscribers)
        return listener

    @asynccontextmanager
    async def connection(self, url: str):
        redis = aioredis.Redis.from_url(url=url, max_connections=1)
        try:
            yield redis
        finally:
            # release conn to pool
            await redis.close()

    @asynccontextmanager
    async def connection_pool(self, url: str):
        cur_loop_id = id(asyncio.get_event_loop())
        try:
            pools = self.__thread_local.pools
        except AttributeError:
            pools = {}
            self.__thread_local.pools = pools
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

    async def __subscribe_to_topic(self, subscribers: Dict[str, aioredis.client.PubSub], topic: str) -> bool:
        if topic in subscribers.keys():
            pub = subscribers[topic]
            if pub.subscribed:
                return True
            else:
                # release conn
                del subscribers[topic]
                await pub.close()
        url = self.__get_redis_url(topic)
        async with self.connection(url) as conn:
            redis: aioredis.Redis = conn
            pub = redis.pubsub()
            await pub.subscribe(topic)
            success = pub.subscribed
            if success:
                subscribers[topic] = pub
            return success
