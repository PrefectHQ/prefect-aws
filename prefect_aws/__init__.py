from . import _version
from .credentials import AwsCredentials, MinIOCredentials
from .client_parameters import AwsClientParameters
from .ecs import ECSTask, ECSTaskResult

__all__ = [
    "AwsCredentials",
    "AwsClientParameters",
    "MinIOCredentials",
    "ECSTask",
    "ECSTaskResult",
]

__version__ = _version.get_versions()["version"]
