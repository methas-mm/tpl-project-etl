from dataclasses import dataclass
from typing import Any, Dict
from core.common import from_str, to_class
from core.models.app_datas import AppDatas
from core.models.storage import Data
from core.models.execution import Execution


@dataclass
class App:
    name: str
    suffix: str
    execution: Execution

    datas: AppDatas
    storages: Dict[str, Data]

    @staticmethod
    def from_dict(obj: Any) -> "App":
        assert isinstance(obj, dict)
        name = from_str(obj.get("name"))
        suffix = from_str(obj.get("suffix"))
        execution = Execution.from_dict(obj.get("execution"))
        datas = AppDatas.from_dict(obj.get("datas"))
        raw_storages = obj.get("storages")
        storages: Dict[str, Data] = {}

        for key in raw_storages.keys():
            storages[key] = Data.from_dict(raw_storages.get(key))

        return App(name, suffix, execution, datas, storages)

    def to_dict(self) -> dict:
        result: dict = {}
        result["name"] = from_str(self.name)
        result["suffix"] = from_str(self.suffix)
        result["execution"] = to_class(Execution, self.execution)
        result["datas"] = to_class(AppDatas, self.datas)
        result["storages"] = self.storages
        return result
