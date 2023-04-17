from . import _version
from .credentials import AwsCredentials, MinIOCredentials
from .client_parameters import AwsClientParameters
from .s3 import S3Bucket
from .ecs import ECSTask
from .secrets_manager import AwsSecret
from .workers import ECSWorker

__all__ = [
    "AwsCredentials",
    "AwsClientParameters",
    "MinIOCredentials",
    "S3Bucket",
    "ECSTask",
    "AwsSecret",
    "ECSWorker",
]

__version__ = _version.get_versions()["version"]
