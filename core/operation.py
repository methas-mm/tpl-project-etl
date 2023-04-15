import logging
import os
import requests
from requests import Response
import socket
import sys
from typing import Any, Dict, Iterable
from urllib.parse import urlencode, urljoin

from delta import *
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from core.configuration import Configuration
from core.enums import DataModuleType, DataType
from core.models import AppData
from core.models.module import DataModule
from core.models.module.properties import (
    ApiDataModuleProperties,
    DatabaseDataModuleProperties,
    FileDataModuleProperties,
)
from core.models.storage import Data
from core.models.storage.properties import (
    ApiDataProperties,
    LocalPathDataProperties,
    S3DataProperties,
    NfsDataProperties,
    AzureBlobStorageDataProperties,
)
from core.spark.common import build_options, create_dataframe, get_session


class Operation:
    __logger: logging.Logger

    __config: Configuration

    __name: str

    __spark_session: SparkSession

    __storages: Dict[str, Data]

    __jupyter_hostname: str
    __jupyter_host_ip: str

    SPARK_S3_PATH_PREFIX = "s3a://"

    def __init__(self, config: Configuration, logger: logging.Logger) -> None:
        self.__config = config
        self.__logger = logger

        self.__jupyter_hostname = socket.gethostname()
        self.__jupyter_host_ip = socket.gethostbyname(self.__jupyter_hostname)

        self.__parse_config()

    def initialize_spark(self, name: str, packages: list, options: dict = {}):
        self.__name = name

        jupyter_hostname = socket.gethostname()
        jupyter_host_ip = socket.gethostbyname(jupyter_hostname)

        config = build_options(jupyter_host_ip, self.__config)

        spark = get_session(
            name,
            config,
            packages,
            options,
        )

        self.__spark_session = spark

    def get_current_spark_session(self) -> SparkSession:
        return self.__spark_session

    def print_debug_vars(self):
        self.__logger.info("jupyter hostname: " + self.__jupyter_hostname)
        self.__logger.info("jupyter ip: " + self.__jupyter_host_ip)

        self.__logger.info("**************** [Engine] ****************")

        if self.__config.engine.is_dedicated_spark:
            self.__logger.info(
                "spark master endpoint: " + self.__config.engine.spark_endpoint
            )

        engine_storage_props = self.__config.engine.storage.get_properties()

        obj_storage_endpoint = engine_storage_props.endpoint
        obj_storage_bucket_name = engine_storage_props.bucket_name

        bronze_bucket = obj_storage_bucket_name + "/bronze/"
        silver_bucket = obj_storage_bucket_name + "/silver/"
        gold_bucket = obj_storage_bucket_name + "/gold/"

        self.__logger.info("engine storage endpoint: " + obj_storage_endpoint)
        self.__logger.info("engine storage bucket name: " + obj_storage_bucket_name)

        self.__logger.info("engine storage bronze bucket: " + bronze_bucket)
        self.__logger.info("engine storage silver bucket: " + silver_bucket)
        self.__logger.info("engine storage gold bucket: " + gold_bucket)

        self.__logger.info("**************** [App] ****************")
        self.__logger.info("app name: " + self.__name)

    def ingest(
        self,
        src: DataModule,
        dest: DataModule,
        src_schema: StructType = None,
        src_options: dict = {},
        dest_options: dict = {},
    ) -> None:
        src_data_props = src.get_properties()
        src_storage = self.__storages.get(src_data_props.storage)

        dest_data_props = dest.get_properties()
        dest_storage = self.__storages.get(dest_data_props.storage)

        read_df = self.__read_source(
            schema=src_schema,
            module=src,
            storage=src_storage,
            options=src_options,
        )

        if read_df is not None:
            self.__write_dest(
                data=read_df, module=dest, storage=dest_storage, options=dest_options
            )

    def export(self, data: DataFrame, dest: DataModule, dest_options: dict = {}):
        if dest.type == DataModuleType.Database:
            data_props = dest.get_properties()
            assert isinstance(data_props, DatabaseDataModuleProperties)

            storage = self.__storages.get(data_props.storage)
            storage_props = storage.get_properties()

            path = ""

            if storage.type == DataType.S3:
                assert isinstance(storage_props, S3DataProperties)

                if data_props.storage == "engine":
                    path = self.get_engine_storage_path(data_props.table)
                else:
                    path = os.path.join(
                        self.SPARK_S3_PATH_PREFIX,
                        storage_props.bucket_name,
                        data_props.table,
                    )
            elif storage.type == DataType.AzureBlobStorage:
                assert isinstance(storage_props, AzureBlobStorageDataProperties)

                path = os.path.join(
                    storage_props.endpoint, storage_props.path, data_props.table
                )
            elif storage.type == DataType.MongoDB:
                pass

            if path == "":
                return None

            self.write_upsert_to(data, path, dest_options)

    def read_from_mongodb(
        self,
        schema: StructType,
        table: str,
        storage_props: dict,
        options: dict = {},
    ) -> DataFrame:
        spark_options = {
            "spark.mongodb.connection.uri": storage_props.get("uri"),
            "spark.mongodb.database": storage_props.get("db"),
            "spark.mongodb.collection": table,
        }

        options.update(spark_options)

        return self.read_from(schema=schema, data_type="mongodb", options=options)

    def read_from_mssql(
        self,
        schema: StructType,
        table: str,
        storage_props: dict,
        options: dict = {},
    ) -> DataFrame:
        if options is None:
            options = {}

        spark_options = {
            "dbtable": table,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        for key, value in spark_options.items():
            options[key] = value

        return self.read_from_sql(
            schema=schema, table=table, storage_props=storage_props, options=options
        )

    def read_from_sql(
        self,
        schema: StructType,
        table: str,
        storage_props: dict,
        options: dict = {},
    ) -> DataFrame:
        if options is None:
            options = {}

        spark_options = {
            "url": storage_props.get("url"),
        }

        for key, value in spark_options.items():
            options[key] = value

        return self.read_from(schema=schema, data_type="jdbc", options=options)

    def read_from_s3(
        self,
        schema: StructType,
        path: str,
        data_type: str,
        options: dict = {},
    ) -> DataFrame:
        return self.read_from(
            schema=schema, path=path, data_type=data_type, options=options
        )

    def read_from(
        self, schema: StructType, data_type: str, path: str = None, options: dict = {}
    ) -> DataFrame:
        spark = self.get_current_spark_session()
        spark_reader = spark.read

        for key in options:
            spark_reader = spark_reader.option(key, options[key])

        if data_type == "csv":
            if schema is not None:
                spark_reader.schema(schema=schema)

            return spark_reader.csv(path=path)
        elif data_type == "delta":
            return spark_reader.format("delta").load(path=path)
        elif data_type == "jdbc":
            return spark_reader.format("jdbc").load()
        elif data_type == "mongodb":
            return spark_reader.format("mongodb").load()

    def write_to_s3(
        self, data: DataFrame, mode: str, path: str, options: dict = None
    ) -> None:
        if options is None:
            options = {}

        path_prefix = self.SPARK_S3_PATH_PREFIX

        actual_options = {"mergeSchema": True}
        actual_path = os.path.join(path_prefix, path)

        for key, value in actual_options.items():
            options[key] = value

        if actual_path:
            self.write_to(
                data=data, format="delta", mode=mode, path=actual_path, options=options
            )

    def write_to_mongodb(
        self,
        data: DataFrame,
        mode: str,
        uri: str,
        db: str,
        table: str,
        options: dict = None,
    ) -> None:
        if options is None:
            options = {}

        spark_options = {
            "spark.mongodb.connection.uri": uri,
            "spark.mongodb.database": db,
            "spark.mongodb.collection": table,
        }

        for key, value in spark_options.items():
            options[key] = value

        self.write_to(data=data, format="mongodb", mode=mode, options=options)

    def write_to_mssql(
        self,
        data: DataFrame,
        mode: str,
        url: str,
        table: str,
        options: dict = None,
    ) -> None:
        if options is None:
            options = {}

        spark_options = {
            "url": url,
            "dbtable": table,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        for key, value in spark_options.items():
            options[key] = value

        self.write_to_sql(
            data=data,
            mode=mode,
            format="jdbc",
            options=options,
        )

    def write_to_sql(
        self,
        data: DataFrame,
        mode: str,
        format: str,
        options: dict = None,
    ) -> None:
        if options is None:
            options = {}

        self.write_to(
            data=data,
            format=format,
            mode=mode,
            options=options,
        )

    def write_to(
        self,
        data: DataFrame,
        format: str,
        mode: str,
        path: str = None,
        options: dict = {},
    ) -> None:
        writer = data.write

        modes = ["append", "overwrite"]

        parsed_mode = mode.lower()
        actual_mode = parsed_mode if parsed_mode in modes else "append"

        for key, value in options.items():
            writer = writer.option(key, value)

        writer.format(format).mode(actual_mode).save(path)

    def write_upsert_to(self, data: DataFrame, path: str, options: dict = {}):
        spark = self.get_current_spark_session()

        is_dt = DeltaTable.isDeltaTable(spark, path)

        if not is_dt:
            new_df = create_dataframe(spark=spark, schema=data.schema)
            self.write_to(data=new_df, format="delta", mode="append", path=path)

        dt = DeltaTable.forPath(spark, path)

        alias = "datas" if options["alias"] is None else options["alias"]

        dt.alias(alias).merge(data, options["condition"](data)).whenMatchedUpdate(
            set=options["matches"](data)
        ).whenNotMatchedInsertAll().execute()

    def get_engine_storage_path(self, path: str):
        storage = self.__storages["engine"]
        props = storage.get_properties()

        path_prefix = self.SPARK_S3_PATH_PREFIX

        return os.path.join(path_prefix, props.bucket_name, path)

    def get_storage(self, name: str) -> Data:
        return self.__storages[name]

    def __read_source(
        self,
        schema: StructType,
        module: DataModule,
        storage: Data,
        options: dict = {},
    ) -> DataFrame:
        data_props = module.get_properties()
        storage_props = storage.get_properties()

        if module.type == DataModuleType.Api and storage.type == DataType.Api:
            assert isinstance(data_props, ApiDataModuleProperties)
            assert isinstance(storage_props, ApiDataProperties)

            if "parameters" in options:
                data_props.parameters = options.get("parameters")

            if "body" in options:
                data_props.body = options.get("body")

            return self.__read_src_from_api(
                schema=schema,
                data_props=data_props,
                storage_props=storage_props,
                success_handler=options.get("successHandler")
                if "successHandler" in options
                else None,
                error_handler=options.get("errorHandler")
                if "errorHandler" in options
                else None,
            )

        elif module.type == DataModuleType.Database:
            assert isinstance(data_props, DatabaseDataModuleProperties)
            if storage.type == DataType.Impala:
                pass
            elif storage.type == DataType.MongoDB:
                return self.__read_src_from_database(
                    schema=schema,
                    data_props=data_props,
                    storage_props=storage_props,
                    options=options,
                )
            elif storage.type == DataType.AzureBlobStorage:
                pass

        elif module.type == DataModuleType.File:
            assert isinstance(data_props, FileDataModuleProperties)
            if storage.type == DataType.LocalPath:
                assert isinstance(storage_props, LocalPathDataProperties)
                return self.__read_src_from_path(
                    schema=schema,
                    data_props=data_props,
                    storage_props=storage_props,
                    options=options,
                )
            elif storage.type == DataType.Nfs:
                assert isinstance(storage_props, NfsDataProperties)
                return self.__read_src_from_nfs(
                    schema=schema,
                    data_props=data_props,
                    storage_props=storage_props,
                    options=options,
                )
            elif storage.type == DataType.S3:
                assert isinstance(storage_props, S3DataProperties)
                return self.__read_src_from_s3(
                    schema=schema,
                    data_props=data_props,
                    storage_props=storage_props,
                    options=options,
                )
            elif storage.type == DataType.Sftp:
                pass
            elif storage.type == DataType.Api:
                pass

    def __write_dest(
        self, data: DataFrame, module: DataModule, storage: Data, options: dict = {}
    ) -> DataFrame:
        if module.type == DataModuleType.Database:
            data_props = module.get_properties()
            assert isinstance(data_props, DatabaseDataModuleProperties)

            self.__write_dest_to_database(
                data=data, data_props=data_props, storage=storage, options=options
            )

        elif module.type == DataModuleType.File:
            pass

        elif module.type == DataModuleType.Api:
            pass

    def __write_dest_to_database(
        self,
        data: DataFrame,
        data_props: DatabaseDataModuleProperties,
        storage: Data,
        options: dict = {},
    ):
        props = storage.get_properties()
        if storage.type == DataType.S3:
            assert isinstance(props, S3DataProperties)

            path = os.path.join(props.endpoint, props.bucket_name, data_props.table)

            if data_props.storage == "engine":
                path = os.path.join(props.bucket_name, data_props.table)

            return self.write_to_s3(
                data=data, mode="append", path=path, options=options
            )
        elif storage.type == DataType.MongoDB:
            storage_props = storage.get_properties()

            uri = storage_props.get("uri")
            db = storage_props.get("db")

            return self.write_to_mongodb(
                data=data,
                mode="append",
                uri=uri,
                db=db,
                table=data_props.table,
                options=options,
            )
        elif storage.type == DataType.MSSql:
            storage_props = storage.get_properties()

            url = storage_props.get("url")
            db = storage_props.get("db")

            return self.write_to_mssql(
                data=data,
                mode="append",
                url=url,
                db=db,
                table=data_props.table,
                options=options,
            )

    def __read_src_from_api(
        self,
        schema: StructType,
        data_props: ApiDataModuleProperties,
        storage_props: ApiDataProperties,
        success_handler: Any = None,
        error_handler: Any = None,
    ):
        response = self.__request_api(
            self.__build_request_api_options(
                data_props=data_props, storage_props=storage_props
            )
        )
        if response.status_code == 200:
            datas = (
                response.json()
                if success_handler is None
                else success_handler(response.json())
            )

            return self.__process_read_src_from_api(schema=schema, datas=datas)
        else:
            if error_handler:
                error_handler(response.json())

    def __read_src_from_path(
        self,
        schema: StructType,
        data_props: FileDataModuleProperties,
        storage_props: LocalPathDataProperties,
        options: dict = {},
    ) -> DataFrame:
        parsed_type = str.lower(data_props.type)

        parsed_type = "csv" if not parsed_type else parsed_type

        path = os.path.join(storage_props.path, data_props.path)

        return self.read_from(
            schema=schema, path=path, data_type=parsed_type, options=options
        )

    def __read_src_from_nfs(
        self,
        schema: StructType,
        data_props: FileDataModuleProperties,
        storage_props: LocalPathDataProperties,
        options: dict = {},
    ) -> DataFrame:
        path = os.path.join(storage_props.path, data_props.path)

        return self.read_from(
            schema=schema, path=data_props.path, data_type="", options=options
        )

    def __read_src_from_s3(
        self,
        schema: StructType,
        data_props: FileDataModuleProperties,
        storage_props: S3DataProperties,
        options: dict = {},
    ) -> DataFrame:
        parsed_type = str.lower(data_props.type)

        if (
            storage_props.endpoint
            == self.__storages["engine"].get_properties().endpoint
        ):
            path = os.path.join(
                self.SPARK_S3_PATH_PREFIX, storage_props.bucket_name, data_props.path
            )
            return self.read_from_s3(
                schema=schema, path=path, data_type=parsed_type, options=options
            )

    def __read_src_from_database(
        self,
        schema: StructType,
        data_props: DatabaseDataModuleProperties,
        storage_props: dict,
        options: dict = {},
    ) -> DataFrame:
        if storage_props.type == DataType.MongoDB:
            return self.read_from_mongodb(
                schema=schema,
                table=data_props.table,
                storage_props=storage_props,
                options=options,
            )
        elif storage_props.type == DataType.MSSql:
            return self.read_from_mssql(
                schema=schema,
                table=data_props.table,
                storage_props=storage_props,
                options=options,
            )

    def __process_read_src_from_api(
        self, schema: StructType, datas: Iterable[Any]
    ) -> DataFrame:
        return create_dataframe(spark=self.__spark_session, schema=schema, datas=datas)

    def __build_request_api_options(
        self, data_props: ApiDataModuleProperties, storage_props: ApiDataProperties
    ) -> dict:
        options = {}
        headers = storage_props.headers
        method = str.lower(data_props.method)

        options["url"] = urljoin(
            storage_props.endpoint, storage_props.path + "/" + data_props.path
        )
        options["method"] = method
        options["parameters"] = data_props.parameters

        if not method:
            method = "get"

        if method == "post":
            if "Content-Type" not in headers:
                headers["Content-Type"] = "application/json"

            options["body"] = data_props.body

        options["headers"] = headers

        return options

    def __request_api(self, options: dict):
        url = options["url"]
        headers = options["headers"]
        method = options["method"]
        parameters = options["parameters"]

        if not method:
            method = "get"

        if method == "get":
            return requests.get(url, headers=headers, params=parameters)
        elif method == "post":
            body = options["body"]

            return requests.post(url, headers=headers, params=parameters, json=body)

    def __parse_config(self):
        self.__storages = self.__config.app.storages
        self.__storages.update({"engine": self.__config.engine.storage})
