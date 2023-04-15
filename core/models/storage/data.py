from dataclasses import dataclass
from typing import TypeVar

from core.common import to_class
from core.enums import DataType
from core.models.storage.properties import (
    DataProperties,
    LocalPathDataProperties,
    NfsDataProperties,
    S3DataProperties,
    SftpDataProperties,
    ImpalaDataProperties,
    ApiDataProperties,
    AzureBlobStorageDataProperties,
    AzureAdlsGen2StorageDataProperties,
    MongoDataProperties,
    MSSqlDataProperties,
)


T = TypeVar("T")


@dataclass
class Data:

    type: DataType
    properties: DataProperties

    def get_properties(self):
        if self.type == DataType.LocalPath:
            return LocalPathDataProperties.from_properties(self.properties)
        elif self.type == DataType.S3:
            return S3DataProperties.from_properties(self.properties)
        elif self.type == DataType.Nfs:
            return NfsDataProperties.from_properties(self.properties)
        elif self.type == DataType.Sftp:
            return SftpDataProperties.from_properties(self.properties)
        elif self.type == DataType.Impala:
            return ImpalaDataProperties.from_properties(self.properties)
        elif self.type == DataType.Api:
            return ApiDataProperties.from_properties(self.properties)
        elif self.type == DataType.MongoDB:
            return MongoDataProperties.from_properties(self.properties)
        elif self.type == DataType.AzureBlobStorage:
            return AzureBlobStorageDataProperties.from_properties(self.properties)
        elif self.type == DataType.AzureAdlsGen2Storage:
            return AzureAdlsGen2StorageDataProperties.from_properties(self.properties)
        elif self.type == DataType.MSSql:
            return MSSqlDataProperties.from_properties(self.properties)
        else:
            return self.properties

    @staticmethod
    def from_dict(obj: any) -> "Data":
        assert isinstance(obj, dict)
        parse_type = DataType(obj.get("type"))

        if parse_type == DataType.LocalPath:
            parse_properties = LocalPathDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.S3:
            parse_properties = S3DataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.Nfs:
            parse_properties = NfsDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.Sftp:
            parse_properties = SftpDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.Impala:
            parse_properties = ImpalaDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.Api:
            parse_properties = ApiDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.MongoDB:
            parse_properties = MongoDataProperties.from_dict(obj.get("properties"))
        elif parse_type == DataType.AzureBlobStorage:
            parse_properties = AzureBlobStorageDataProperties.from_dict(
                obj.get("properties")
            )
        elif parse_type == DataType.AzureAdlsGen2Storage:
            parse_properties = AzureAdlsGen2StorageDataProperties.from_dict(
                obj.get("properties")
            )
        elif parse_type == DataType.MSSql:
            parse_properties = MSSqlDataProperties.from_dict(obj.get("properties"))
        else:
            parse_properties = obj.get("properties")

        return Data(parse_type, parse_properties)

    def to_dict(self) -> dict:
        result: dict = {}
        result["type"] = to_class(DataType, self.type)
        result["properties"] = to_class(DataProperties, self.properties)
