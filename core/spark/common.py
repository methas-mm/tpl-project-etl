from typing import Any, Dict, Iterable
from core.configuration import Configuration

from delta import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def build_options(host_ip: str, config: Configuration) -> Dict[str, Any]:

    # engine: object storage configuration
    engine_storage_props = config.engine.storage.get_properties()
    obj_storage_endpoint = engine_storage_props.endpoint

    # TODO: this must be revise. (security reason, recommended load as environment variables)
    obj_storage_access_key = engine_storage_props.access_key
    obj_storage_secret_key = engine_storage_props.secret_key

    spark_executor_core = config.engine.spark_executor_core
    spark_executor_memory = config.engine.spark_executor_memory

    is_dynamic_allocation = config.engine.is_dynamic_allocation
    spark_executor_min_instances = config.engine.spark_executor_min_instances
    spark_executor_max_instances = config.engine.spark_executor_max_instances

    spark_config = {
        "spark.master": (
            config.engine.spark_endpoint
            if config.engine.is_dedicated_spark
            else "k8s://https://kubernetes.default.svc.cluster.local"
        ),
        "spark.driver.blockManager.port": "7777",
        "spark.driver.port": "2222",
        "spark.driver.host": host_ip,
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": obj_storage_endpoint,
        # "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": obj_storage_access_key,
        "spark.hadoop.fs.s3a.secret.key": obj_storage_secret_key,
        "spark.jars.repositories": "https://pkgs.dev.azure.com/solytic/OpenSource/_packaging/releases/maven/v1",
    }

    if not config.engine.is_dedicated_spark:
        spark_config.update(
            {
                "spark.kubernetes.namespace": "data-driven-dd30",
                "spark.kubernetes.container.image": "xyzxyz442/spark-py:3.3.1-python-3.9-1",
                "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                "spark.dynamicAllocation.enabled": is_dynamic_allocation,
                "spark.dynamicAllocation.maxExecutors": spark_executor_max_instances,
                "spark.executor.instances": spark_executor_min_instances,
                "spark.executor.memory": spark_executor_memory,
                "spark.executor.cores": spark_executor_core,
            }
        )

    return spark_config


def get_session(
    app_name: str,
    config: dict,
    packages: Iterable[str] = [],
    options: dict = {},
) -> SparkSession:
    conf = SparkConf()

    for key, value in config.items():
        conf.set(key, value)

    for key, value in options.items():
        conf.set(key, value)

    builder = SparkSession.builder.appName(app_name).config(conf=conf)

    # wait for deltalake to update its version (https://github.com/delta-io/delta/issues/889)
    return configure_spark_with_delta_pip(builder, packages).getOrCreate()


def create_dataframe(
    spark: SparkSession, schema: StructType, datas: Iterable[Any] = []
) -> DataFrame:
    return spark.createDataFrame(datas, schema=schema)


def write_dataframe(df: DataFrame, path: str, options: dict):
    actual_options = {
        "mergeSchema": True,
    }

    write_df = df.write.format("delta").mode("append")

    actual_options.update(options)

    for key, value in actual_options.items():
        write_df.option(key, value)

    write_df.save(path)


def is_bucket_exist(context: SparkContext, bucket_name: str) -> bool:
    path = context._jvm.org.apache.hadoop.fs.FileSystem.get(
        context._jvm.java.net.URI.create(bucket_name),
        context._jsc.hadoopConfiguration(),
    )

    return path.exists(context._jvm.org.apache.hadoop.fs.Path(bucket_name))
