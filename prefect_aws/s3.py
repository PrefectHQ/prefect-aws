import io
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional

from prefect.tasks import task, Task
from prefect.utilities.logging import get_logger

from prefect_aws.schema import TaskArgs
from prefect_aws.utilities import (
    get_boto_client,
    verify_required_args_present,
)


@dataclass
class S3DownloadDefaultValues:
    """Dataclass that defines default values that can be supplied when creating an S3 download task"""

    bucket: Optional[str] = None
    key: Optional[str] = None
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)


def create_s3_download_task(
    default_values: Optional[S3DownloadDefaultValues] = None,
    task_args: Optional[TaskArgs] = None,
) -> Task:
    """
    Creates an S3 download task with the supplied default values and task configuration

    Args:
        default_values (Optional[S3DownloadDefaultValues]): Optional object to define default values
            supplied to the task on invocation. 
        task_args: Optional arguments to use in task creation. Refer to the [Prefect docs]
            (https://orion-docs.prefect.io/api-ref/prefect/tasks/#prefect.tasks.task) for more information.

    Returns:
        A callable `Task` object which, when called, will submit the task for execution 
        to perform an S3 download

    Examples:
        Create a task that by default will download from a given bucket with the supplied credentials

        ```python
        s3_download_task = create_s3_download_task(
            default_values=S3DownloadDefaultValues(
                bucket="my_bucket",
                boto_kwargs={
                    aws_access_key_id="access_key",
                    aws_secret_access_key="secret_access_key",
                }
            ),
            task_args=TaskArgs(name="Download from bucket"),
        )
        ```
    """
    default_values = default_values or S3DownloadDefaultValues()
    task_args = task_args or TaskArgs()

    @verify_required_args_present(arg_names=["bucket", "key"])
    def s3_download(
        bucket: Optional[str] = default_values.bucket,
        key: Optional[str] = default_values.key,
        boto_kwargs: Dict[str, Any] = default_values.boto_kwargs,
    ):
        """
        Downloads an object with a given key from a given S3 bucket. AWS authentication is handled via the `boto3`
        module. Refer to the [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
        for more info about the possible credential configurations.

        Args:
            bucket (str): Name of bucket to download object from. Required if a default value was not supplied
                when creating the task.
            key: Key of object to download. Required if a default value was not supplied
                when creating the task.
            boto_kwargs: Keyword arguments that are forwarded to the boto client used by the task. Defaults to
                an empty dictionary.

        Returns:
            A `bytes` representation of the downloaded object.

        """
        logger = get_logger()
        logger.info("Downloading object from bucket %s with key %s", bucket, key)

        s3_client = get_boto_client("s3", **boto_kwargs)
        stream = io.BytesIO()
        s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)
        stream.seek(0)
        output = stream.read()

        return output

    return task(s3_download, **asdict(task_args))


@dataclass
class S3UploadDefaultValues:
    bucket: Optional[str] = None
    key: str = field(default_factory=lambda: str(uuid.uuid4()))
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)


def create_s3_upload_task(
    default_values: Optional[S3UploadDefaultValues] = None,
    task_args: Optional[TaskArgs] = None,
):
    default_values = default_values or S3UploadDefaultValues()
    task_args = task_args or TaskArgs()

    @verify_required_args_present(arg_names=["bucket", "key", "data"])
    def s3_upload(
        data: bytes,
        bucket: Optional[str] = default_values.bucket,
        key: str = default_values.key,
        boto_kwargs: Dict[str, Any] = default_values.boto_kwargs,
    ):
        logger = get_logger()
        logger.info("Uploading object to bucket %s with key %s", bucket, key)

        s3_client = get_boto_client("s3", **boto_kwargs)
        stream = io.BytesIO(data)
        s3_client.upload_fileobj(stream, Bucket=bucket, Key=key)

        return key

    return task(s3_upload, **asdict(task_args))


@dataclass
class S3ListObjectsDefaultValues:
    bucket: Optional[str] = None
    prefix: str = ""
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)
    delimiter: Optional[str] = ""
    page_size: Optional[int] = None
    max_items: Optional[int] = None
    jmespath_query: Optional[str] = None


def create_s3_list_objects_task(
    default_values: Optional[S3ListObjectsDefaultValues] = None,
    task_args: Optional[TaskArgs] = None,
):
    default_values = default_values or S3ListObjectsDefaultValues()
    task_args = task_args or TaskArgs()

    @verify_required_args_present(arg_names=["bucket"])
    def s3_list_objects(
        bucket: Optional[str] = default_values.bucket,
        prefix: str = default_values.prefix,
        boto_kwargs: Dict[str, Any] = default_values.boto_kwargs,
        delimiter: Optional[str] = default_values.delimiter,
        page_size: Optional[int] = default_values.page_size,
        max_items: Optional[int] = default_values.max_items,
        jmespath_query: Optional[str] = default_values.jmespath_query,
    ):
        logger = get_logger()
        logger.info("Listing objects in bucket %s with prefix %s", bucket, prefix)

        s3_client = get_boto_client("s3", **boto_kwargs)
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig={"PageSize": page_size, "MaxItems": max_items},
        )
        if jmespath_query:
            page_iterator = page_iterator.search(f"{jmespath_query} | {{Contents: @}}")

        return [
            content for page in page_iterator for content in page.get("Contents", [])
        ]

    return s3_list_objects
