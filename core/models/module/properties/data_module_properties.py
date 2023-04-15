from dataclasses import dataclass


@dataclass
class DataModuleProperties(dict):
    def __init__(self, obj):
        self.__dict__ = obj
