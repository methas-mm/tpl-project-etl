from abc import ABC, abstractmethod
from typing import Any


class AppDataBase(ABC):
    @abstractmethod
    def parse(obj: Any):
        pass
