from dataclasses import dataclass
from typing import Any
from core.common import from_int, from_str

from core.models.storage.properties.data_properties import DataProperties


@dataclass
class ImpalaDataProperties(DataProperties):
    host: str
    port: int
    auth_mechanism: str
    user: str
    password: str
    table: str

    def __init__(self, host, port, auth_mechanism, user, password, table):
        parsed_obj: dict = {}
        parsed_obj["host"] = host
        parsed_obj["port"] = port
        parsed_obj["auth_mechanism"] = auth_mechanism
        parsed_obj["user"] = user
        parsed_obj["password"] = password
        parsed_obj["table"] = table
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "ImpalaDataProperties":
        assert isinstance(obj, dict)
        host = from_str(obj.get("host"))
        port = from_int(obj.get("port"))
        auth_mechanism = from_str(obj.get("auth_mechanism"))
        user = from_str(obj.get("user"))
        password = from_str(obj.get("password"))
        table = from_str(obj.get("table"))
        return ImpalaDataProperties(host, port, auth_mechanism, user, password, table)

    def to_dict(self) -> dict:
        result: dict = {}
        result["host"] = from_str(self.host)
        result["port"] = from_int(self.port)
        result["auth_mechanism"] = from_str(self.auth_mechanism)
        result["user"] = from_str(self.user)
        result["password"] = from_str(self.password)
        result["table"] = from_str(self.table)
        return result

    def from_properties(self) -> "ImpalaDataProperties":
        return ImpalaDataProperties(
            self.host,
            self.port,
            self.auth_mechanism,
            self.user,
            self.password,
            self.table,
        )
