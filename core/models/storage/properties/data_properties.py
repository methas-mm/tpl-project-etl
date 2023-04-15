from dataclasses import dataclass


@dataclass
class DataProperties(dict):
    def __init__(self, obj):
        self.__dict__ = obj
