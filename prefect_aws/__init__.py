from . import _version
from .credentials import AwsCredentials
from .client_parameters import AwsClientParameters

__all__ = ["AwsCredentials", "AwsClientParameters"]

__version__ = _version.get_versions()["version"]
