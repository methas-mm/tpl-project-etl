from dataclasses import dataclass
from typing import Any

from core.common import from_str


@dataclass
class Execution:

    input_path: str
    output_path: str

    @staticmethod
    def from_dict(obj: Any) -> 'Execution':
        assert isinstance(obj, dict)
        input_path = from_str(obj.get('inputPath'))
        output_path = from_str(obj.get('outputPath'))
        return Execution(input_path, output_path)

    def to_dict(self) -> dict:
        result: dict = {}
        result['inputPath'] = from_str(self.input_path)
        result['outputPath'] = from_str(self.output_path)
        return result
