from . import _version
from .credentials import AwsCredentials

__all__ = ["AwsCredentials"]

__version__ = _version.get_versions()["version"]
