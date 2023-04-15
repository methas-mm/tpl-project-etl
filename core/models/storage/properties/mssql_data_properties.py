from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class MSSqlDataProperties(DataProperties):
    url: str
    db: str

    def __init__(self, url, db):
        parsed_obj: dict = {}
        parsed_obj["url"] = url
        parsed_obj["db"] = db
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "MSSqlDataProperties":
        assert isinstance(obj, dict)
        url = from_str(obj.get("url"))
        db = from_str(obj.get("db"))
        return MSSqlDataProperties(url, db)

    def to_dict(self) -> dict:
        result: dict = {}
        result["url"] = from_str(self.url)
        result["db"] = from_str(self.db)
        return result

    def from_properties(self) -> "MSSqlDataProperties":
        return MSSqlDataProperties(self.url, self.db)
