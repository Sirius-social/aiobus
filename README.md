# An event-bus application layer, support redis

Simply **ASYNC** framework to integrate **DISTRIBUTED** message bus to your application:
 - utilize redis pub/sub features in backend
 - fast to start and simply configure
 - thread-safe and python async-friendly thanks to coroutines contexts
 - fault tolerance if some redis instances down for a little time
 - scalable thanks to consistent hashing for mapping `topic` to `instance` without cluster setup

## Install
```pip install aiobus```

## Usage

```python
    import json
    import asyncio
    import datetime
    from aiobus.redis import RedisBus
    ...
    bus = RedisBus(
        servers=['192.168.100.1', '192.168.100.2:6380'],
        max_pool_size=1000
    )
    ...
    # Publisher Coroutine
    async def publisher():
        while True:
            await bus.publish('my-topic', {'stamp': str(datetime.datetime.now())})
            await asyncio.sleep(0.1)
    ...
    # Subscriber Coroutine
    async def subscriber():
        await bus.subscribe('my-topic')
        async for msg in await bus.listen():
            print(json.dumps(msg, indent=2, sort_keys=True))
    ...
```

## Demo
1. Setup redis instances on localhost ```docker-compose up -d``` for demo purposes
2. Run/Debug [Demo script](/demo.py) 

## Running tests
Before to run tests you should 
 - install dependencies: ```pip install -r requirements.txt```
 - start the dependent services with command: ```docker-compose up -d```