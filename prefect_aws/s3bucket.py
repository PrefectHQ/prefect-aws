import io
from prefect.filesystems import ReadableFileSystem, WritableFileSystem, RemoteFileSystem
from typing import Optional
import urllib3
from prefect_aws import AwsCredentials
from pydantic import HttpUrl, SecretStr
from prefect.blocks.core import Block
from typing import Union
import boto3

class S3Bucket(ReadableFileSystem, WritableFileSystem):

    """Info here."""

    bucket: str
    # make this secret
    credentials: Union[AwsCredentials, MinIOCredentials]
    basepath: Optional[str] = None
    endpoint_url: Optional[str] = None

    # change
    _logo_url: Optional[HttpUrl] = "https://miro.medium.com/max/1024/0*PGkPVIgj0M5hF92Y.png"  # noqa

    def read_path(self, path: str) -> bytes:
        s3_client = self.credentials.get_boto3_session().client('s3', endpoint_url=self.endpoint_url)
        with io.BytesIO() as stream:
            s3_client.download_fileobj(
                Bucket=self.bucket,
                Key=self.basepath,
                FileObj=stream
                )
            stream.seek(0)
            output = stream.read()
            return output

    def write_path(self, path: str, content: bytes) -> None:
        if self.endpoint_url:
            s3_client = self.credentials.get_boto3_session().client('s3', endpoint_url=self.endpoint_url)
        else:
            s3_client = self.credentials.get_boto3_session().client('s3')
        with io.BytesIO(content) as stream:
            s3_client.upload_fileobj(
                Fileobj=stream,
                Bucket=self.bucket,
                Key=self.basepath  # could fail if none?
                )
