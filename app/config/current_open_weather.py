from dataclasses import dataclass
from typing import Any

from core.enums import DataModuleType
from core.models import AppData
from core.models.module import DataModule


@dataclass
class CurrentOpenWeatherAppData(AppData):
    src: DataModule
    dest: DataModule

    def __init__(self):
        self.src = DataModule(DataModuleType.Default, {})
        self.dest = DataModule(DataModuleType.Default, {})

    def parse(self, obj: Any):
        assert isinstance(obj, dict)

        self.src = DataModule.from_dict(obj.get("src"))
        self.dest = DataModule.from_dict(obj.get("dest"))
