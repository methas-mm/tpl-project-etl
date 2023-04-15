from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class SftpDataProperties(DataProperties):
    host: str
    username: str
    private_key: str
    path: str

    def __init__(self, host, username, private_key, path):
        parsed_obj: dict = {}
        parsed_obj["host"] = host
        parsed_obj["username"] = username
        parsed_obj["private_key"] = private_key
        parsed_obj["path"] = path
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "SftpDataProperties":
        assert isinstance(obj, dict)
        host = from_str(obj.get("host"))
        username = from_str(obj.get("username"))
        private_key = from_str(obj.get("privateKey"))
        path = from_str(obj.get("path"))
        return SftpDataProperties(host, username, private_key, path)

    def to_dict(self) -> dict:
        result: dict = {}
        result["host"] = from_str(self.host)
        result["username"] = from_str(self.username)
        result["privateKey"] = from_str(self.private_key)
        result["path"] = from_str(self.path)
        return result

    def from_properties(self) -> "SftpDataProperties":
        return SftpDataProperties(self.host, self.username, self.private_key, self.path)
