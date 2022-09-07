from . import _version
from .credentials import AwsCredentials, MinIOCredentials
from .client_parameters import AwsClientParameters

__all__ = [
    "AwsCredentials",
    "AwsClientParameters",
    "MinIOCredentials",
]

__version__ = _version.get_versions()["version"]
