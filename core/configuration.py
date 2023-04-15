from dataclasses import dataclass
from typing import Any, Type, TypeVar

from core.common import to_class
from core.models.app import App
from core.models.engine import Engine

T = TypeVar("T")


@dataclass
class Configuration:
    engine: Engine
    app: App

    @staticmethod
    def from_dict(obj: Any) -> "Configuration":
        assert isinstance(obj, dict)
        print("Configuration")
        engine = Engine.from_dict(obj.get("engine"))
        app = App.from_dict(obj.get("app"))
        return Configuration(engine, app)

    def to_dict(self) -> dict:
        result: dict = {}
        result["engine"] = to_class(Engine, self.engine)
        result["app"] = to_class(App, self.app)
        return result


def configuration_from_dict(obj: Any) -> Configuration:
    return Configuration.from_dict(obj)
