from dataclasses import dataclass
from typing import Any
from core.common import from_dict, from_str
from core.models.module.properties.data_module_properties import DataModuleProperties


@dataclass
class DatabaseDataModuleProperties(DataModuleProperties):
    storage: str
    table: str

    def __init__(self, storage, table):
        parsed_obj: dict = {}
        parsed_obj["storage"] = storage
        parsed_obj["table"] = table
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "DatabaseDataModuleProperties":
        assert isinstance(obj, dict)
        storage = from_str(obj.get("storage"))
        table = from_str(obj.get("table"))
        return DatabaseDataModuleProperties(storage, table)

    def to_dict(self) -> dict:
        result: dict = {}
        result["storage"] = from_str(self.storage)
        result["table"] = from_str(self.table)
        return result

    def from_properties(self) -> "DatabaseDataModuleProperties":
        return DatabaseDataModuleProperties(self.storage, self.table)
