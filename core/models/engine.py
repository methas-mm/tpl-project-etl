from dataclasses import dataclass
from typing import Any

from core.common import from_bool, from_int, from_str
from core.models.storage import Data


@dataclass
class Engine:
    storage: Data
    tmp_path: str

    is_dedicated_spark: bool
    spark_executor_core: int
    spark_executor_memory: str
    spark_endpoint: str

    is_dynamic_allocation: bool
    spark_executor_min_instances: int
    spark_executor_max_instances: int

    @staticmethod
    def from_dict(obj: Any) -> "Engine":
        assert isinstance(obj, dict)
        storage = Data.from_dict(obj.get("storage"))
        tmp_path = from_str(obj.get("tmpPath"))
        is_dediciated_spark = from_bool(obj.get("isDedicatedSpark"))
        spark_executor_core = from_int(obj.get("sparkExecutorCore"))
        spark_executor_memory = from_str(obj.get("sparkExecutorMemory"))
        spark_endpoint = from_str(obj.get("sparkEndpoint"))

        is_dynamic_allocation = from_bool(obj.get("isDynamicAllocation"))
        spark_executor_min_instances = from_int(obj.get("sparkExecutorMinInstances"))
        spark_executor_max_instances = from_int(obj.get("sparkExecutorMaxInstances"))

        return Engine(
            storage,
            tmp_path,
            is_dediciated_spark,
            spark_executor_core,
            spark_executor_memory,
            spark_endpoint,
            is_dynamic_allocation,
            spark_executor_min_instances,
            spark_executor_max_instances,
        )
