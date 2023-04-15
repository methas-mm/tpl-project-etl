from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class AzureBlobStorageDataProperties(DataProperties):
    endpoint: str
    path: str

    def __init__(self, endpoint, path):
        parsed_obj: dict = {}
        parsed_obj["endpoint"] = endpoint
        parsed_obj["path"] = path
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "AzureBlobStorageDataProperties":
        assert isinstance(obj, dict)
        endpoint = from_str(obj.get("endpoint"))
        path = from_str(obj.get("path"))
        return AzureBlobStorageDataProperties(endpoint, path)

    def to_dict(self) -> dict:
        result: dict = {}
        result["endpoint"] = from_str(self.endpoint)
        result["path"] = from_str(self.path)
        return result

    def from_properties(self) -> "AzureBlobStorageDataProperties":
        return AzureBlobStorageDataProperties(self.endpoint, self.path)
