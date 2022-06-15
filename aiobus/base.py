from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class AbstractListener:
    """Base class for in-loop listeners"""

    @abstractmethod
    async def get_one(self, timeout: float = None) -> Any:
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def is_active(self) -> bool:
        raise NotImplementedError('Must be overridden in the implementations')

    def __aiter__(self):
        self._start_loop()
        return self

    async def __anext__(self):
        try:
            while await self.is_active():
                msg = await self.get_one()
                return msg
            raise StopAsyncIteration
        finally:
            self._start_loop()

    def _start_loop(self):
        pass

    def _stop_loop(self):
        pass


class AbstractBus(ABC):

    @abstractmethod
    async def publish(self, topic: str, message: Dict) -> Optional[int]:
        """Publish message to topic

        :param topic: topic
        :param message: json-serializable message
        :return: count of active subscribers
        """
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def subscribe(self, *topics: str):
        """Subscribe to topic

        Participant may subscribe to multiple topics

        :param topics: topics names
        :return: if-success
        """
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def unsubscribe(self, *topics: str):
        """Unsubscribe from topic

        :param topics: topics names. If is empty list then deactivate ALL active subscriptions
        """
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def listen(self) -> AbstractListener:
        """In-Loop listener instance

        :return: in-loop implementation instance
        """
        raise NotImplementedError('Must be overridden in the implementations')
