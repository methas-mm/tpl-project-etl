from dataclasses import dataclass
from typing import Any, Dict, Type, TypeVar

from core.models.app_data import AppData

T = TypeVar("T")


@dataclass
class AppDatas(dict):
    def __init__(self, obj: dict):
        self.update(obj)

    def get_data(self, key: str, datas_type: Type[T]) -> T:
        obj = self.get(key)

        if isinstance(obj, str):
            return obj

        instance = datas_type()
        assert isinstance(instance, AppData)

        instance.parse(obj)
        return instance

    @staticmethod
    def from_dict(obj: Any) -> "AppDatas":
        assert isinstance(obj, dict)
        return AppDatas(obj)

    def to_dict(self) -> dict:
        return self
