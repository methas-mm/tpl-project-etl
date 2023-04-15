from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class NfsDataProperties(DataProperties):
    path: str

    def __init__(self, path):
        parsed_obj: dict = {}
        parsed_obj["path"] = path
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "NfsDataProperties":
        assert isinstance(obj, dict)
        path = from_str(obj.get("path"))
        return NfsDataProperties(path)

    def to_dict(self) -> dict:
        result: dict = {}
        result["path"] = from_str(self.path)
        return result

    def from_properties(self) -> "NfsDataProperties":
        return NfsDataProperties(self.path)
