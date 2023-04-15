from abc import ABC, abstractmethod
from typing import Any

from core.enums import DataModuleType
from core.models.app_data_base import AppDataBase
from core.models.module import DataModule


class AppData(AppDataBase):
    src: DataModule
    dest: DataModule

    def __init__(self):
        self.src = DataModule(DataModuleType.Default, {})
        self.dest = DataModule(DataModuleType.Default, {})

    def parse(self, obj: Any):
        assert isinstance(obj, dict)

        self.src = DataModule.from_dict(obj.get("src"))
        self.dest = DataModule.from_dict(obj.get("dest"))
