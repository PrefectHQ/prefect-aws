"""Tasks for interacting with AWS S3"""
import asyncio
import io
import os
import uuid
import warnings
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Union
from uuid import uuid4

import boto3
from botocore.paginate import PageIterator
from prefect import get_run_logger, task
from prefect.blocks.abstract import ObjectStorageBlock
from prefect.filesystems import WritableDeploymentStorage, WritableFileSystem
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from prefect.utilities.filesystem import filter_files
from pydantic import Field, root_validator, validator

from prefect_aws import AwsCredentials, MinIOCredentials
from prefect_aws.client_parameters import AwsClientParameters


@task
async def s3_download(
    bucket: str,
    key: str,
    aws_credentials: AwsCredentials,
    aws_client_parameters: AwsClientParameters = AwsClientParameters(),
) -> bytes:
    """
    Downloads an object with a given key from a given S3 bucket.

    Args:
        bucket: Name of bucket to download object from. Required if a default value was
            not supplied when creating the task.
        key: Key of object to download. Required if a default value was not supplied
            when creating the task.
        aws_credentials: Credentials to use for authentication with AWS.
        aws_client_parameters: Custom parameter for the boto3 client initialization.


    Returns:
        A `bytes` representation of the downloaded object.

    Example:
        Download a file from an S3 bucket:

        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.s3 import s3_download


        @flow
        async def example_s3_download_flow():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )
            data = await s3_download(
                bucket="bucket",
                key="key",
                aws_credentials=aws_credentials,
            )

        example_s3_download_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading object from bucket %s with key %s", bucket, key)

    s3_client = aws_credentials.get_boto3_session().client(
        "s3", **aws_client_parameters.get_params_override()
    )
    stream = io.BytesIO()
    await run_sync_in_worker_thread(
        s3_client.download_fileobj, Bucket=bucket, Key=key, Fileobj=stream
    )
    stream.seek(0)
    output = stream.read()

    return output


@task
async def s3_upload(
    data: bytes,
    bucket: str,
    aws_credentials: AwsCredentials,
    aws_client_parameters: AwsClientParameters = AwsClientParameters(),
    key: Optional[str] = None,
) -> str:
    """
    Uploads data to an S3 bucket.

    Args:
        data: Bytes representation of data to upload to S3.
        bucket: Name of bucket to upload data to. Required if a default value was not
            supplied when creating the task.
        aws_credentials: Credentials to use for authentication with AWS.
        aws_client_parameters: Custom parameter for the boto3 client initialization..
        key: Key of object to download. Defaults to a UUID string.

    Returns:
        The key of the uploaded object

    Example:
        Read and upload a file to an S3 bucket:

        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.s3 import s3_upload


        @flow
        async def example_s3_upload_flow():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )
            with open("data.csv", "rb") as file:
                key = await s3_upload(
                    bucket="bucket",
                    key="data.csv",
                    data=file.read(),
                    aws_credentials=aws_credentials,
                )

        example_s3_upload_flow()
        ```
    """
    logger = get_run_logger()

    key = key or str(uuid.uuid4())

    logger.info("Uploading object to bucket %s with key %s", bucket, key)

    s3_client = aws_credentials.get_boto3_session().client(
        "s3", **aws_client_parameters.get_params_override()
    )
    stream = io.BytesIO(data)
    await run_sync_in_worker_thread(
        s3_client.upload_fileobj, stream, Bucket=bucket, Key=key
    )

    return key


def _list_objects_sync(page_iterator: PageIterator):
    """
    Synchronous method to collect S3 objects into a list

    Args:
        page_iterator: AWS Paginator for S3 objects

    Returns:
        List[Dict]: List of object information
    """
    return [content for page in page_iterator for content in page.get("Contents", [])]


@task
async def s3_list_objects(
    bucket: str,
    aws_credentials: AwsCredentials,
    aws_client_parameters: AwsClientParameters = AwsClientParameters(),
    prefix: str = "",
    delimiter: str = "",
    page_size: Optional[int] = None,
    max_items: Optional[int] = None,
    jmespath_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Lists details of objects in a given S3 bucket.

    Args:
        bucket: Name of bucket to list items from. Required if a default value was not
            supplied when creating the task.
        aws_credentials: Credentials to use for authentication with AWS.
        aws_client_parameters: Custom parameter for the boto3 client initialization..
        prefix: Used to filter objects with keys starting with the specified prefix.
        delimiter: Character used to group keys of listed objects.
        page_size: Number of objects to return in each request to the AWS API.
        max_items: Maximum number of objects that to be returned by task.
        jmespath_query: Query used to filter objects based on object attributes refer to
            the [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#filtering-results-with-jmespath)
            for more information on how to construct queries.

    Returns:
        A list of dictionaries containing information about the objects retrieved. Refer
            to the boto3 docs for an example response.

    Example:
        List all objects in a bucket:

        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.s3 import s3_list_objects


        @flow
        async def example_s3_list_objects_flow():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )
            objects = await s3_list_objects(
                bucket="data_bucket",
                aws_credentials=aws_credentials
            )

        example_s3_list_objects_flow()
        ```
    """  # noqa E501
    logger = get_run_logger()
    logger.info("Listing objects in bucket %s with prefix %s", bucket, prefix)

    s3_client = aws_credentials.get_boto3_session().client(
        "s3", **aws_client_parameters.get_params_override()
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter=delimiter,
        PaginationConfig={"PageSize": page_size, "MaxItems": max_items},
    )
    if jmespath_query:
        page_iterator = page_iterator.search(f"{jmespath_query} | {{Contents: @}}")

    return await run_sync_in_worker_thread(_list_objects_sync, page_iterator)


class S3Bucket(WritableFileSystem, WritableDeploymentStorage, ObjectStorageBlock):

    """
    Block used to store data using AWS S3 or S3-compatible object storage like MinIO.

    Attributes:
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
        from prefect_aws.s3 import S3Bucket

        s3bucket_block = S3Bucket.load("BLOCK_NAME")
        ```
    """

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/1jbV4lceHOjGgunX15lUwT/db88e184d727f721575aeb054a37e277/aws.png?h=250"  # noqa
    _block_type_name = "S3 Bucket"

    bucket_name: str = Field(default=..., description="Name of your bucket")
    minio_credentials: Optional[MinIOCredentials] = Field(
        default=None,
        description=(
            "[DEPRECATED; use the aws_credentials field instead] "
            "A block containing your credentials (choose this or "
            "AWS Credentials)",
        ),
    )
    aws_credentials: Optional[Union[AwsCredentials, MinIOCredentials]] = Field(
        default=None,
        description="A block containing your credentials (choose this or "
        "MinIO Credentials).",
    )
    basepath: Optional[Union[str, Path]] = Field(
        default="",
        description=(
            "[DEPRECATED; use the bucket_folder field instead] "
            "A default location to write to and read from in the S3 bucket. Defaults "
            "to the root of the bucket."
        ),
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description=(
            "[DEPRECATED; pass AwsClientParameters in AwsCredentials instead] "
            "URL endpoint to use for S3 compatible storage. Defaults to "
            "standard AWS S3 endpoint."
        ),
    )
    bucket_folder: str = Field(
        default="",
        description=(
            "A default path to a folder within the S3 bucket to use "
            "for reading and writing objects."
        ),
    )

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

    @validator("basepath", pre=True)
    def deprecate_basepath(cls, value):
        if value:
            warnings.warn(
                "The basepath field is deprecated. Please use bucket_folder instead.",
                DeprecationWarning,
            )
        return value

    @validator("endpoint_url", pre=True)
    def deprecate_endpoint_url(cls, value):
        if value:
            warnings.warn(
                "The endpoint_url field is deprecated. Please pass it in "
                "AwsClientParameters and pass it into aws_credentials instead.",
                DeprecationWarning,
            )
        return value

    @root_validator(pre=True)
    def check_credentials(cls, values):

        """
        Ensure exactly 1 of 2 optional credentials fields has been provided by
        user.
        """

        minio_creds_exist = bool(values.get("minio_credentials"))
        aws_creds_exist = bool(values.get("aws_credentials"))

        # if both credentials fields provided
        if minio_creds_exist:
            # raise deprecationwarning
            warnings.warn(
                "The minio_credentials field is deprecated. Please use "
                "aws_credentials instead.",
                DeprecationWarning,
            )
            values["aws_credentials"] = values.get("minio_credentials")

        # if neither credentials fields provided
        if not minio_creds_exist and not aws_creds_exist:
            raise ValueError(
                "S3Bucket requires either a minio_credentials"
                "field or an aws_credentials field."
            )
        return values

    def _resolve_path(self, path: str) -> str:

        """
        A helper function used in write_path to join `self.basepath` and `path`.

        Args:

            path: Name of the key, e.g. "file1". Each object in your
                bucket has a unique key (or key name).

        """

        path = path or str(uuid4())

        bucket_folder = self.bucket_folder or self.basepath
        # If basepath provided, it means we won't write to the root dir of
        # the bucket. So we need to add it on the front of the path.
        path = str(Path(bucket_folder) / path) if bucket_folder else path

        return path

    def _get_s3_client(self) -> boto3.client:

        """
        Authenticate MinIO credentials or AWS credentials and return an S3 client.
        This is a helper function called by read_path() or write_path().
        """

        if self.minio_credentials:
            s3_client = self.minio_credentials.get_boto3_session().client(
                service_name="s3", endpoint_url=self.endpoint_url
            )

        elif self.aws_credentials:
            s3_client = self.aws_credentials.get_boto3_session().client(
                service_name="s3"
            )
        else:
            raise ValueError(
                "S3 Bucket requires either a minio_credentials"
                "field or an aws_credentials field."
            )
        return s3_client

    def _get_bucket_resource(self) -> boto3.resource:
        """
        Retrieves boto3 resource object for the configured bucket
        """
        if self.minio_credentials:
            bucket = (
                self.minio_credentials.get_boto3_session()
                .resource("s3", endpoint_url=self.endpoint_url)
                .Bucket(self.bucket_name)
            )

        elif self.aws_credentials:
            bucket = (
                self.aws_credentials.get_boto3_session()
                .resource("s3")
                .Bucket(self.bucket_name)
            )
        else:
            raise ValueError(
                "S3 Bucket requires either a minio_credentials"
                "field or an aws_credentials field."
            )
        return bucket

    @sync_compatible
    async def get_directory(
        self, from_path: Optional[str] = None, local_path: Optional[str] = None
    ) -> None:
        """
        Copies a folder from the configured S3 bucket to a local directory.

        Defaults to copying the entire contents of the block's basepath to the current
        working directory.

        Args:
            from_path: Path in S3 bucket to download from. Defaults to the block's
                configured basepath.
            local_path: Local path to download S3 contents to. Defaults to the current
                working directory.
        """
        bucket_folder = self.bucket_folder or self.basepath
        if from_path is None:
            from_path = str(bucket_folder) if bucket_folder else ""

        if local_path is None:
            local_path = str(Path(".").absolute())
        else:
            local_path = str(Path(local_path).expanduser())

        bucket = self._get_bucket_resource()
        for obj in bucket.objects.filter(Prefix=from_path):
            if obj.key[-1] == "/":
                # object is a folder and will be created if it contains any objects
                continue
            target = os.path.join(
                local_path,
                os.path.relpath(obj.key, from_path),
            )
            os.makedirs(os.path.dirname(target), exist_ok=True)
            bucket.download_file(obj.key, target)

    @sync_compatible
    async def put_directory(
        self,
        local_path: Optional[str] = None,
        to_path: Optional[str] = None,
        ignore_file: Optional[str] = None,
    ) -> int:
        """
        Uploads a directory from a given local path to the configured S3 bucket in a
        given folder.

        Defaults to uploading the entire contents the current working directory to the
        block's basepath.

        Args:
            local_path: Path to local directory to upload from.
            to_path: Path in S3 bucket to upload to. Defaults to block's configured
                basepath.
            ignore_file: Path to file containing gitignore style expressions for
                filepaths to ignore.

        """
        to_path = "" if to_path is None else to_path

        if local_path is None:
            local_path = "."

        included_files = None
        if ignore_file:
            with open(ignore_file, "r") as f:
                ignore_patterns = f.readlines()

            included_files = filter_files(local_path, ignore_patterns)

        uploaded_file_count = 0
        for local_file_path in Path(local_path).expanduser().rglob("*"):
            if (
                included_files is not None
                and str(local_file_path.relative_to(local_path)) not in included_files
            ):
                continue
            elif not local_file_path.is_dir():
                remote_file_path = Path(to_path) / local_file_path.relative_to(
                    local_path
                )
                with open(local_file_path, "rb") as local_file:
                    local_file_content = local_file.read()

                await self.write_path(str(remote_file_path), content=local_file_content)
                uploaded_file_count += 1

        return uploaded_file_count

    @sync_compatible
    async def read_path(self, path: str) -> bytes:

        """
        Read specified path from S3 and return contents. Provide the entire
        path to the key in S3.

        Args:
            path: Entire path to (and including) the key.

        Example:
            Read "subfolder/file1" contents from an S3 bucket named "bucket":
            ```python
            from prefect_aws import AwsCredentials
            from prefect_aws.s3 import S3Bucket

            aws_creds = AwsCredentials(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )

            s3_bucket_block = S3Bucket(
                bucket_name="bucket",
                aws_credentials=aws_creds,
                basepath="subfolder"
            )

            key_contents = s3_bucket_block.read_path(path="subfolder/file1")
            ```
        """
        path = self._resolve_path(path)

        return await run_sync_in_worker_thread(self._read_sync, path)

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

    @sync_compatible
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
            from prefect_aws import MinioCredentials
            from prefect_aws.s3 import S3Bucket

            minio_creds = MinIOCredentials(
                minio_root_user = "minioadmin",
                minio_root_password = "minioadmin",
            )

            s3_bucket_block = S3Bucket(
                bucket_name="bucket",
                minio_credentials=minio_creds,
                basepath="dogs/smalldogs",
                endpoint_url="http://localhost:9000",
            )
            s3_havanese_path = s3_bucket_block.write_path(path="havanese", content=data)
            ```
        """

        path = self._resolve_path(path)

        await run_sync_in_worker_thread(self._write_sync, path, content)

        return path

    def _write_sync(self, key: str, data: bytes) -> None:

        """
        Called by write_path(). Creates an S3 client and uploads a file
        object.
        """

        s3_client = self._get_s3_client()

        with io.BytesIO(data) as stream:

            s3_client.upload_fileobj(Fileobj=stream, Bucket=self.bucket_name, Key=key)

    def _list_objects_sync(self, page_iterator: PageIterator) -> List[Dict[str, Any]]:
        """
        Synchronous method to collect S3 objects into a list

        Args:
            page_iterator: AWS Paginator for S3 objects

        Returns:
            List[Dict]: List of object information
        """
        return [
            content for page in page_iterator for content in page.get("Contents", [])
        ]

    # NEW BLOCK INTERFACE METHODS BELOW
    def _join_bucket_folder(self, bucket_path: str = "") -> str:
        """
        Joins the base bucket folder to the bucket path.
        NOTE: If a method reuses another method in this class, be careful to not
        call this  twice because it'll join the bucket folder twice.
        See https://github.com/PrefectHQ/prefect-aws/issues/141 for a past issue.
        """
        bucket_path = str(bucket_path)
        if self.bucket_folder != "" and bucket_path.startswith(self.bucket_folder):
            self.logger.info(
                f"Bucket path {bucket_path!r} is already prefixed with "
                f"bucket folder {self.bucket_folder!r}; is this intentional?"
            )
        return str(Path(self.bucket_folder) / bucket_path)

    @sync_compatible
    async def list_objects(
        self,
        folder: str,
        delimiter: str = "",
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
        jmespath_query: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Args:
            bucket: Name of bucket to list items from. Required if a default value
                was not supplied when creating the task.
            aws_credentials: Credentials to use for authentication with AWS.
            aws_client_parameters: Custom parameter for the boto3 client initialization.
            prefix: Used to filter objects with keys starting with the specified prefix.
            delimiter: Character used to group keys of listed objects.
            page_size: Number of objects to return in each request to the AWS API.
            max_items: Maximum number of objects that to be returned by task.
            jmespath_query: Query used to filter objects based on object attributes refer to
                the [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#filtering-results-with-jmespath)
                for more information on how to construct queries.
        """  # noqa: E501
        bucket_path = self._join_bucket_folder(folder)
        client = self.aws_credentials.get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=bucket_path,
            Delimiter=delimiter,
            PaginationConfig={"PageSize": page_size, "MaxItems": max_items},
        )
        if jmespath_query:
            page_iterator = page_iterator.search(f"{jmespath_query} | {{Contents: @}}")

        self.logger.info(f"Listing objects in bucket {bucket_path}.")
        return await run_sync_in_worker_thread(self._list_objects_sync, page_iterator)

    @sync_compatible
    async def download_object_to_path(
        self,
        from_path: str,
        to_path: Optional[Union[str, Path]],
        **download_kwargs: Dict[str, Any],
    ) -> Path:
        """
        Downloads an object from the S3 bucket to a path.

        Args:
            from_path: The path to the object to download; this gets prefixed
                with the bucket_folder.
            to_path: The path to download the object to. If not provided, the
                object's name will be used.
            **download_kwargs: Additional keyword arguments to pass to
                `Client.download_file`.

        Returns:
            The absolute path that the object was downloaded to.
        """
        if to_path is None:
            to_path = Path(from_path).name

        # making path absolute, but converting back to str here
        # since !r looks nicer that way and filename arg expects str
        to_path = str(Path(to_path).absolute())
        bucket_path = self._join_bucket_folder(from_path)
        client = self.aws_credentials.get_s3_client()

        self.logger.debug(
            f"Preparing to download object from bucket {self.bucket_name!r} "
            f"path {bucket_path!r} to {to_path!r}."
        )
        await run_sync_in_worker_thread(
            client.download_file,
            Bucket=self.bucket_name,
            Key=from_path,
            Filename=to_path,
            **download_kwargs,
        )
        self.logger.info(
            f"Downloaded object from bucket {self.bucket_name!r} path {bucket_path!r}"
            f"to {to_path!r}."
        )
        return Path(to_path)

    @sync_compatible
    async def download_object_to_file_object(
        self,
        from_path: str,
        to_file_object: BinaryIO,
        **download_kwargs: Dict[str, Any],
    ) -> BinaryIO:
        """
        Downloads an object from the object storage service to a file-like object,
        which can be a BytesIO object or a BufferedWriter.

        Args:
            from_path: The path to the object to download from; this gets prefixed
                with the bucket_folder.
            to_file_object: The file-like object to download the object to.
            **download_kwargs: Additional keyword arguments to pass to
                `Client.download_fileobj`.

        Returns:
            The file-like object that the object was downloaded to.
        """
        client = self.aws_credentials.get_s3_client()
        bucket_path = self._join_bucket_folder(from_path)

        self.logger.debug(
            f"Preparing to download object from bucket {self.bucket_name!r} "
            f"path {bucket_path!r} to file object."
        )
        await run_sync_in_worker_thread(
            client.download_fileobj,
            Bucket=self.bucket_name,
            Key=bucket_path,
            Fileobj=to_file_object,
            **download_kwargs,
        )
        self.logger.info(
            f"Downloaded object from bucket {self.bucket_name!r} path {bucket_path!r}"
            f"to file object."
        )
        return to_file_object

    @sync_compatible
    async def download_folder_to_path(
        self,
        from_folder: str,
        to_folder: Optional[Union[str, Path]] = None,
        **download_kwargs: Dict[str, Any],
    ) -> Path:
        """
        Downloads objects *within* a folder (excluding the folder itself)
        from the S3 bucket to a folder.

        Args:
            from_folder: The path to the folder to download from.
            to_folder: The path to download the folder to.
            **download_kwargs: Additional keyword arguments to pass to
                `Client.download_file`.

        Returns:
            The absolute path that the folder was downloaded to.
        """
        if to_folder is None:
            to_folder = ""
        to_folder = Path(to_folder).absolute()

        client = self.aws_credentials.get_s3_client()
        objects = client.list_objects_v2(Bucket=self.bucket_name, Prefix=from_folder)
        if len(objects) == 0:
            self.logger.warning(
                f"No objects were downloaded from "
                f"bucket {self.bucket_name!r} path {from_folder!r}."
            )
            return

        # do not call self._join_bucket_folder for list_objects
        # because it's built-in to that method already!
        # however, we still need to do it because we're using relative_to
        bucket_folder = self._join_bucket_folder(from_folder)

        async_coros = []
        for object in objects["Contents"]:
            bucket_path = Path(object["Key"]).relative_to(bucket_folder)
            if bucket_path.is_dir():
                continue
            to_path = to_folder / bucket_path
            to_path.parent.mkdir(parents=True, exist_ok=True)
            self.logger.info(
                f"Downloading object from bucket {self.bucket_name!r} path "
                f"{str(bucket_path)!r} to {to_path}."
            )
            async_coros.append(
                run_sync_in_worker_thread(
                    client.download_file,
                    Bucket=self.bucket_name,
                    Key=object["Key"],
                    Filename=str(to_path),
                    **download_kwargs,
                )
            )
        await asyncio.gather(*async_coros)

        return Path(to_folder)

    @sync_compatible
    async def upload_from_path(
        self, from_path: Union[str, Path], to_path: str, **upload_kwargs: Dict[str, Any]
    ) -> str:
        """
        Uploads an object from a path to the S3 bucket.

        Args:
            from_path: The path to the file to upload from.
            to_path: The path to upload the file to.
            **upload_kwargs: Additional keyword arguments to pass to `Client.upload`.

        Returns:
            The path that the object was uploaded to.
        """
        if to_path is None:
            to_path = Path(from_path).name

        bucket_path = self._join_bucket_folder(to_path)
        client = self.aws_credentials.get_s3_client()
        self.logger.info(
            f"Uploading from {from_path!r} to the bucket "
            f"{self.bucket_name!r} path {bucket_path!r}."
        )

        await run_sync_in_worker_thread(
            client.upload_file,
            Filename=from_path,
            Bucket=self.bucket_name,
            Key=bucket_path,
            **upload_kwargs,
        )
        return bucket_path

    @sync_compatible
    async def upload_from_file_object(
        self, from_file_object: BinaryIO, to_path: str, **upload_kwargs
    ) -> str:
        """
        Uploads an object to the S3 bucket from a file-like object,
        which can be a BytesIO object or a BufferedReader.

        Args:
            from_file_object: The file-like object to upload from.
            to_path: The path to upload the object to.
            **upload_kwargs: Additional keyword arguments to pass to
                `Client.upload_fileobj`.

        Returns:
            The path that the object was uploaded to.
        """
        bucket_path = self._join_bucket_folder(to_path)
        client = self.aws_credentials.get_s3_client()
        self.logger.info(
            f"Uploading from file object to the bucket "
            f"{self.bucket_name!r} path {bucket_path!r}."
        )

        await run_sync_in_worker_thread(
            client.upload_fileobj,
            Fileobj=from_file_object,
            Bucket=self.bucket_name,
            Key=bucket_path,
            **upload_kwargs,
        )
        return bucket_path

    @sync_compatible
    async def upload_from_folder(
        self,
        from_folder: Union[str, Path],
        to_folder: str,
        **upload_kwargs: Dict[str, Any],
    ) -> str:
        """
        Uploads files *within* a folder (excluding the folder itself)
        to the object storage service folder.

        Args:
            from_folder: The path to the folder to upload from.
            to_folder: The path to upload the folder to.
            **upload_kwargs: Additional keyword arguments to pass to
                `Client.upload_fileobj`.

        Returns:
            The path that the folder was uploaded to.
        """
        from_folder = Path(from_folder)
        bucket_folder = self._join_bucket_folder(to_folder or "")

        num_uploaded = 0
        client = self.aws_credentials.get_s3_client()

        async_coros = []
        for from_path in from_folder.rglob("**/*"):
            if from_path.is_dir():
                continue
            bucket_path = str(Path(bucket_folder) / from_path.relative_to(from_folder))
            self.logger.info(
                f"Uploading from {str(from_path)!r} to the bucket "
                f"{self.bucket_name!r} path {bucket_path!r}."
            )
            async_coros.append(
                run_sync_in_worker_thread(
                    client.upload_file,
                    Filename=str(from_path),
                    Bucket=self.bucket_name,
                    Key=bucket_path,
                    **upload_kwargs,
                )
            )
            num_uploaded += 1
        await asyncio.gather(*async_coros)

        if num_uploaded == 0:
            self.logger.warning(f"No files were uploaded from {from_folder}.")

        return to_folder
