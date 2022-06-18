import json
import asyncio
import datetime
from aiobus.redis import RedisBus


bus = RedisBus(
    # Don't forget to setup redis instances on localhost with command:
    # docker-compose up -d
    servers=['127.0.0.1:6379', '127.0.0.1:6380']
)


async def publisher():
    while True:
        await bus.publish('my-topic', {'stamp': str(datetime.datetime.now())})
        await asyncio.sleep(0.5)


async def subscriber():
    await bus.subscribe('my-topic')
    async for msg in await bus.listen():
        print(json.dumps(msg, indent=2, sort_keys=True))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(subscriber(), publisher()))
