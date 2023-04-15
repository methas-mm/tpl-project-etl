from dataclasses import dataclass
from typing import Any
from core.common import from_dict, from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class ApiDataProperties(DataProperties):
    endpoint: str
    headers: dict
    path: str

    def __init__(self, endpoint, path, headers):
        parsed_obj: dict = {}
        parsed_obj["endpoint"] = endpoint
        parsed_obj["path"] = path
        parsed_obj["headers"] = headers
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "ApiDataProperties":
        assert isinstance(obj, dict)
        endpoint = from_str(obj.get("endpoint"))
        path = from_str(obj.get("path"))
        headers = from_dict(obj.get("headers"))

        return ApiDataProperties(endpoint=endpoint, path=path, headers=headers)

    def to_dict(self) -> dict:
        result: dict = {}
        result["endpoint"] = from_str(self.endpoint)
        result["path"] = from_str(self.path)
        result["headers"] = from_dict(self.headers)
        return result

    def from_properties(self) -> "ApiDataProperties":
        return ApiDataProperties(
            endpoint=self.endpoint,
            path=self.path,
            headers=self.headers,
        )
