from dataclasses import dataclass
from typing import Any
from core.common import from_dict, from_str
from core.models.module.properties.data_module_properties import DataModuleProperties


@dataclass
class ApiDataModuleProperties(DataModuleProperties):
    storage: str
    path: str
    method: str
    parameters: dict
    body: dict

    def __init__(self, storage, path, method, parameters, body):
        parsed_obj: dict = {}
        parsed_obj["storage"] = storage
        parsed_obj["path"] = path
        parsed_obj["method"] = method
        parsed_obj["parameters"] = parameters
        parsed_obj["body"] = body
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "ApiDataModuleProperties":
        assert isinstance(obj, dict)
        storage = from_str(obj.get("storage"))
        path = from_str(obj.get("path"))
        method = str.lower(from_str(obj.get("method")))
        parameters = from_dict(obj.get("parameters"))

        # optional for POST method
        body = from_dict(obj.get("body")) if "body" in obj else {}

        return ApiDataModuleProperties(
            storage=storage, path=path, method=method, parameters=parameters, body=body
        )

    def to_dict(self) -> dict:
        result: dict = {}
        result["storage"] = from_str(self.storage)
        result["path"] = from_str(self.path)
        result["method"] = from_str(self.method)
        result["parameters"] = from_dict(self.parameters)
        result["body"] = from_dict(self.body)
        return result

    def from_properties(self) -> "ApiDataModuleProperties":
        return ApiDataModuleProperties(
            storage=self.storage,
            path=self.path,
            method=self.method,
            parameters=self.parameters,
            body=self.body,
        )
