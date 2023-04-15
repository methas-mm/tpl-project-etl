from enum import Enum


class DataType(Enum):
    Default = 0
    LocalPath = 1
    Nfs = 2
    S3 = 3
    Sftp = 4
    Impala = 5
    Api = 6
    MongoDB = 7
    AzureBlobStorage = 8
    AzureAdlsGen2Storage = 9
    MSSql = 10
