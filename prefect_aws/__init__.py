from . import _version
from .credentials import AwsCredentials, MinIOCredentials
from .client_parameters import AwsClientParameters
from .s3bucket import S3Bucket

__all__ = ["AwsCredentials", "AwsClientParameters", "MinIOCredentials", "S3Bucket"]

__version__ = _version.get_versions()["version"]
