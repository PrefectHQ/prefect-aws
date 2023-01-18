from . import _version
from .credentials import AwsCredentials, MinIOCredentials
from .client_parameters import AwsClientParameters
from .s3 import S3Bucket
from .ecs import ECSTask

__all__ = [
    "AwsCredentials",
    "AwsClientParameters",
    "MinIOCredentials",
    "S3Bucket",
    "ECSTask",
]

__version__ = _version.get_versions()["version"]
