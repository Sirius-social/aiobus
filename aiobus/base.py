from abc import ABC, abstractmethod
from typing import Dict, List


class AbstractBus(ABC):

    @abstractmethod
    async def publish(self, topic: str, message: Dict):
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def subscribe(self, topic: str):
        raise NotImplementedError('Must be overridden in the implementations')

    @abstractmethod
    async def subs_num(self, topic: str,) -> int:
        raise NotImplementedError('Must be overridden in the implementations')
