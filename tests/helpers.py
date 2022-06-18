import asyncio


class TcpUpstream:

    def __init__(self, remote_host: str, remote_port: int):
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.remote_reader = None
        self.remote_writer = None
        self.local_reader = None
        self.local_writer = None

    @staticmethod
    async def pipe(reader, writer):
        try:
            while not reader.at_eof():
                writer.write(await reader.read(2048))
        finally:
            writer.close()

    async def handle_client(self, local_reader, local_writer):
        self.local_reader = local_reader
        self.local_writer = local_writer
        try:
            self.remote_reader, self.remote_writer = await asyncio.open_connection(self.remote_host, self.remote_port)
            pipe1 = self.pipe(local_reader, self.remote_writer)
            pipe2 = self.pipe(self.remote_reader, local_writer)
            await asyncio.gather(pipe1, pipe2)
        finally:
            local_writer.close()

    async def close(self):
        if self.remote_writer:
            self.remote_writer.close()
            self.remote_writer = None
        if self.local_writer:
            self.local_writer.close()
            self.local_writer = None
        if self.local_reader:
            self.local_reader.feed_eof()
            self.local_reader = None
        if self.remote_reader:
            self.remote_reader.feed_eof()
            self.remote_reader = None


class TcpProxy:

    def __init__(self, local_host: str = '127.0.0.1', local_port: int = 8000):
        self.local_host = local_host
        self.local_port = local_port
        self.__proxy = None
        self.__upstream = None

    async def start(self, remote_host: str, remote_port: int):
        loop = asyncio.get_event_loop()
        self.__upstream = TcpUpstream(remote_host, remote_port)
        self.__proxy = await asyncio.start_server(self.__upstream.handle_client, self.local_host, self.local_port, loop=loop)

    async def stop(self):
        if self.__proxy:
            self.__proxy.close()
            self.__proxy = None
            await self.__upstream.close()
            self.__upstream = None
