from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class S3DataProperties(DataProperties):
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str

    def __init__(self, endpoint, access_key, secret_key, bucket_name):
        parsed_obj: dict = {}
        parsed_obj["endpoint"] = endpoint
        parsed_obj["access_key"] = access_key
        parsed_obj["secret_key"] = secret_key
        parsed_obj["bucket_name"] = bucket_name
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "S3DataProperties":
        assert isinstance(obj, dict)
        endpoint = from_str(obj.get("endpoint"))
        access_key = from_str(obj.get("accessKey"))
        secret_key = from_str(obj.get("secretKey"))
        bucket_name = from_str(obj.get("bucketName"))
        return S3DataProperties(endpoint, access_key, secret_key, bucket_name)

    def to_dict(self) -> dict:
        result: dict = {}
        result["endpoint"] = from_str(self.endpoint)
        result["accessKey"] = from_str(self.access_key)
        result["secretKey"] = from_str(self.secret_key)
        result["bucketName"] = from_str(self.bucket_name)
        return result

    def from_properties(self) -> "S3DataProperties":
        return S3DataProperties(
            self.endpoint, self.access_key, self.secret_key, self.bucket_name
        )
