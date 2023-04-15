from dataclasses import dataclass

from core.common import to_class
from core.enums import DataModuleType
from core.models.module.properties import (
    DataModuleProperties,
    FileDataModuleProperties,
    DatabaseDataModuleProperties,
    ApiDataModuleProperties,
)


@dataclass
class DataModule:
    type: DataModuleType
    properties: DataModuleProperties

    def get_properties(self):
        if self.type == DataModuleType.File:
            return FileDataModuleProperties.from_properties(self.properties)
        elif self.type == DataModuleType.Database:
            return DatabaseDataModuleProperties.from_properties(self.properties)
        elif self.type == DataModuleType.Api:
            return ApiDataModuleProperties.from_properties(self.properties)

    @staticmethod
    def from_dict(obj: any) -> "DataModule":
        assert isinstance(obj, dict)
        parse_type = DataModuleType(obj.get("type"))

        if parse_type == DataModuleType.File:
            parse_properties = FileDataModuleProperties.from_dict(obj.get("properties"))
        elif parse_type == DataModuleType.Database:
            parse_properties = DatabaseDataModuleProperties.from_dict(
                obj.get("properties")
            )
        elif parse_type == DataModuleType.Api:
            parse_properties = ApiDataModuleProperties.from_dict(obj.get("properties"))

        return DataModule(parse_type, parse_properties)

    def to_dict(self) -> dict:
        result: dict = {}
        result["type"] = to_class(DataModuleType, self.type)
        result["properties"] = to_class(DataModuleProperties, self.properties)
