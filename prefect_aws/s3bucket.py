import io
from uuid import uuid4
from anyio import to_thread
from prefect.filesystems import ReadableFileSystem, WritableFileSystem
from typing import Optional
from prefect_aws import AwsCredentials
from typing import Optional, Union

import boto3
from prefect_aws import MinIOCredentials


class S3Bucket(ReadableFileSystem, WritableFileSystem):

    """
    Block used to store data using S3-compatible object storage like MinIO.

    Args:
        bucket_name: Name of your bucket.
        credentials: A block containing your credentials (AwsCredentials or MinIOCredentials).
        basepath: Used when you don't want to read/write at base level.
        endpoint_url: Used for non-AWS configuration. When unspecified, defaults to AWS.

    Example:
        Load stored S3Bucket configuration:
        ```python
        from prefect_aws import S3Bucket

        s3bucket_block = S3Bucket.load("BLOCK_NAME")
        ```
    """

    # change
    _logo_url = "https://w7.pngwing.com/pngs/564/59/png-transparent-amazon-com-amazon-s3-amazon-web-services-amazon-simple-queue-service-amazon-glacier-bucket-miscellaneous-data-amazon-dynamodb.png"  # noqa
    _block_type_name = "S3 Bucket"

    bucket_name: str
    credentials: Union[MinIOCredentials, AwsCredentials]
    basepath: Optional[str] = None
    endpoint_url: Optional[str] = None

    def _get_s3_client(self) -> boto3.client:

        if self.endpoint_url:
            s3_client = boto3.client(
                service_name='s3',
                aws_access_key_id=self.credentials.minio_root_user,
                aws_secret_access_key=self.credentials.minio_root_password.get_secret_value(), #noqa
                endpoint_url=self.endpoint_url
            )
        else:
            s3_client = boto3.client(service_name='s3')

        return s3_client

    async def read_path(self, path: str) -> bytes:
        return await to_thread.run_sync(self._read_sync, path)

    def _read_sync(self, key: str) -> bytes:
        s3_client = self._get_s3_client()
        with io.BytesIO() as stream:
            s3_client.download_fileobj(
                Bucket=self.bucket_name,
                Key=key,
                Fileobj=stream
                )
            stream.seek(0)
            output = stream.read()
            return output

    async def write_path(self, path: str, content: bytes) -> str:
        path = str(uuid4())
        path = (self.basepath.rstrip("/") + "/" + path if self.basepath else path)
        await to_thread.run_sync(self._write_sync, path, content)
        return path

    def _write_sync(self, key: str, data: bytes) -> None:
        s3_client = self._get_s3_client()
        with io.BytesIO(data) as stream:
            s3_client.upload_fileobj(
                Fileobj=stream,
                Bucket=self.bucket_name,
                Key=key
                )
