"""Module for reading and writing from S3."""
import io
from pathlib import Path
from typing import Optional
from uuid import uuid4

import boto3
from anyio import to_thread
from prefect.filesystems import ReadableFileSystem, WritableFileSystem
from pydantic import validator

from prefect_aws import AwsCredentials, MinIOCredentials


class S3Bucket(ReadableFileSystem, WritableFileSystem):

    """
    Block used to store data using S3-compatible object storage like MinIO.

    Args:
        bucket_name: Name of your bucket.
        aws_credentials: A block containing your credentials (choose this
            or minio_credentials).
        minio_credentials: A block containing your credentials (choose this
            or aws_credentials).
        basepath: Used when you don't want to read/write at base level.
        endpoint_url: Used for non-AWS configuration. When unspecified,
            defaults to AWS.

    Example:
        Load stored S3Bucket configuration:
        ```python
        from prefect_aws import S3Bucket

        s3bucket_block = S3Bucket.load("BLOCK_NAME")
        ```
    """

    # change
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/uPezmBzEv4moXKdQJ3YyL/a1f029b423cf67f474d1eee33c1463d7/pngwing.com.png?h=250"  # noqa
    _block_type_name = "S3 Bucket"

    bucket_name: str
    minio_credentials: Optional[MinIOCredentials]
    aws_credentials: Optional[AwsCredentials]
    basepath: Optional[Path]
    endpoint_url: Optional[str]

    @validator("basepath", pre=True)
    def cast_pathlib(cls, value):

        """
        If basepath provided, it means we aren't writing to the root directory
        of the bucket. We need to ensure that it is a valid path. This is called
        when the S3Bucket block is instantiated.
        """

        if isinstance(value, Path):
            return str(value)
        return value

    def _resolve_path(self, path: str) -> Path:

        """
        A helper function used in write_path to join `self.basepath` and `path`.

        Args:

            path: Name of the key, e.g. "file1". Each object in your
                bucket has a unique key (or key name).

        """

        path = path or str(uuid4())

        # If basepath provided, it means we won't write to the root dir of
        # the bucket. So we need to add it on the front of the path.
        path = str(Path(self.basepath) / path) if self.basepath else path

        return path

    def _get_s3_client(self) -> boto3.client:

        """
        Authenticate MinIO credentials or AWS credentials and return an S3 client.
        This is a helper function called by read_path() or write_path().
        """

        s3_client_kwargs = {}

        if self.minio_credentials:

            aws_secret_access_key = self.minio_credentials.minio_root_password
            s3_client_kwargs.update(
                aws_access_key_id=self.minio_credentials.minio_root_user,
                aws_secret_access_key=aws_secret_access_key.get_secret_value(),
                endpoint_url=self.endpoint_url,
            )

        s3_client = boto3.client(service_name="s3", **s3_client_kwargs)

        return s3_client

    async def read_path(self, path: str) -> bytes:

        """
        Read specified path from S3 and return contents. Provide the entire
        path to the key in S3.

        Args:
            path: Entire path to (and including) the key.

        Example:
            Read "subfolder/file1" contents from an S3 bucket named "bucket":
            ```python
            s3_bucket_block = S3Bucket(
                bucket_name="bucket",
                aws_credentials=AwsCredentials,
                basepath="subfolder"
            )

            key_contents = s3_bucket_block.read_path(path="subfolder/file1")
        ```
        """

        return await to_thread.run_sync(self._read_sync, path)

    def _read_sync(self, key: str) -> bytes:

        """
        Called by read_path(). Creates an S3 client and retrieves the
        contents from  a specified path.
        """

        s3_client = self._get_s3_client()

        with io.BytesIO() as stream:

            s3_client.download_fileobj(Bucket=self.bucket_name, Key=key, Fileobj=stream)
            stream.seek(0)
            output = stream.read()
            return output

    async def write_path(self, path: str, content: bytes) -> str:

        """
        Writes to an S3 bucket.

        Args:

            path: The key name. Each object in your bucket has a unique
                key (or key name).
            content: What you are uploading to S3.

        Example:

            Write data to the path `dogs/small_dogs/havanese` in an S3 Bucket:
            ```python
            s3_bucket_block = S3Bucket(
                bucket_name="bucket",
                minio_credentials=MinIOCredentials,
                basepath="dogs/smalldogs",
                endpoint_url="http://localhost:9000",
            )
            s3_havanese_path = s3_bucket_block.write_path(path="havanese", content=data)
           ```
        """

        path = self._resolve_path(path)

        await to_thread.run_sync(self._write_sync, path, content)

        return path

    def _write_sync(self, key: str, data: bytes) -> None:

        """
        Called by write_path(). Creates an S3 client and uploads a file
        object.
        """

        s3_client = self._get_s3_client()

        with io.BytesIO(data) as stream:

            s3_client.upload_fileobj(Fileobj=stream, Bucket=self.bucket_name, Key=key)
