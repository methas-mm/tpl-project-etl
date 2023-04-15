from dataclasses import dataclass
from typing import Any

from core.common import from_str
from core.models.storage.properties.data_properties import DataProperties


@dataclass
class MongoDataProperties(DataProperties):
    uri: str
    db: str

    def __init__(self, uri, db):
        parsed_obj: dict = {}
        parsed_obj["uri"] = uri
        parsed_obj["db"] = db
        self.__dict__ = parsed_obj

    @staticmethod
    def from_dict(obj: Any) -> "MongoDataProperties":
        assert isinstance(obj, dict)
        uri = from_str(obj.get("uri"))
        db = from_str(obj.get("db"))
        return MongoDataProperties(uri, db)

    def to_dict(self) -> dict:
        result: dict = {}
        result["uri"] = from_str(self.uri)
        result["db"] = from_str(self.db)
        return result

    def from_properties(self) -> "MongoDataProperties":
        return MongoDataProperties(self.uri, self.db)
