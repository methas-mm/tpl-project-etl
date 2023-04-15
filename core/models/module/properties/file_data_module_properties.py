from dataclasses import dataclass
from typing import Any
from core.common import from_dict, from_str
from core.models.module.properties.data_module_properties import DataModuleProperties


@dataclass
class FileDataModuleProperties(DataModuleProperties):
    storage: str
    type: str
    path: str
    options: dict

    def __init__(self, storage, type, path, options):
        parsed_obj: dict = {}
        parsed_obj["storage"] = storage
        parsed_obj["type"] = type
        parsed_obj["path"] = path
        parsed_obj["options"] = options
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "FileDataModuleProperties":
        assert isinstance(obj, dict)
        storage = from_str(obj.get("storage"))
        type = from_str(obj.get("type"))
        path = from_str(obj.get("path"))

        # optional for options
        options = from_dict(obj.get("options")) if "options" in obj else {}

        return FileDataModuleProperties(
            storage=storage, type=type, path=path, options=options
        )

    def to_dict(self) -> dict:
        result: dict = {}
        result["storage"] = from_str(self.storage)
        result["type"] = from_str(self.type)
        result["path"] = from_str(self.path)
        result["options"] = from_dict(self.options)
        return result

    def from_properties(self) -> "FileDataModuleProperties":
        return FileDataModuleProperties(
            storage=self.storage, type=self.type, path=self.path, options=self.options
        )
