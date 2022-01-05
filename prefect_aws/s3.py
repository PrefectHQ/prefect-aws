import io
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from prefect.utilities.logging import get_logger

from prefect_aws.schema import DefaultValues
from prefect_aws.utilities import get_boto_client, task_factory


@dataclass
class S3DownloadDefaultValues(DefaultValues):
    """
    Dataclass that defines default values that can be supplied when creating an S3
    download task
    """

    bucket: Optional[str] = None
    key: Optional[str] = None
    boto_kwargs: Optional[Dict[str, Any]] = field(default_factory=dict)


@task_factory(
    default_values_cls=S3DownloadDefaultValues, required_args=["bucket", "key"]
)
def s3_download(
    bucket: str = None,
    key: str = None,
    boto_kwargs: Dict[str, Any] = None,
) -> bytes:
    """
    Downloads an object with a given key from a given S3 bucket. AWS authentication is
    handled via the `boto3` module. Refer to the [boto3 docs]
    (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.

    Args:
        bucket: Name of bucket to download object from. Required if a default value was
            not supplied when creating the task.
        key: Key of object to download. Required if a default value was not supplied
            when creating the task.
        boto_kwargs: Keyword arguments that are forwarded to the boto client used by the
            task. Defaults to an empty dictionary.

    Returns:
        A `bytes` representation of the downloaded object.

    Example:
        Download a file from an S3 bucket
        >>> boto_kwargs = dict(
        >>>     aws_access_key_id="acccess_key_id",
        >>>     aws_secret_access_key="secret_access_key",
        >>> )
        >>>
        >>> s3_download_task = s3_download(
        >>>    default_values=S3DownloadDefaultValues(
        >>>        bucket="data_bucket", boto_kwargs=boto_kwargs
        >>>    ),
        >>>    task_args=TaskArgs(retries=3, retry_delay_seconds=10)
        >>> )
        >>>
        >>> @flow
        >>> def example_s3_download_flow():
        >>>     data = s3_download_task(key="data.csv")


    """
    logger = get_logger()
    logger.info("Downloading object from bucket %s with key %s", bucket, key)

    s3_client = get_boto_client("s3", **boto_kwargs)
    stream = io.BytesIO()
    s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)
    stream.seek(0)
    output = stream.read()

    return output


@dataclass
class S3UploadDefaultValues(DefaultValues):
    """
    Dataclass that defines default values that can be supplied when creating an S3
    upload task
    """

    bucket: Optional[str] = None
    key: str = field(default_factory=lambda: str(uuid.uuid4()))
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)


@task_factory(
    default_values_cls=S3UploadDefaultValues, required_args=["bucket", "key", "data"]
)
def s3_upload(
    data: bytes,
    bucket: str = None,
    key: str = None,
    boto_kwargs: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Uploads data to an S3 bucket. AWS authentication is handled via the `boto3` module.
    Refer to the [boto3 docs]
    (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.

    Args:
        data: Bytes representation of data to upload to S3.
        bucket: Name of bucket to upload data to. Required if a default value was not
            supplied when creating the task.
        key: Key of object to download. Defaults to a UUID string.
        boto_kwargs: Keyword arguments that are forwarded to the boto client used by the
            task. Defaults to an empty dictionary.

    Returns:
        The key of the uploaded object

    Example:
        Read and upload a file to an S3 bucket
        >>> boto_kwargs = dict(
        >>>     aws_access_key_id="acccess_key_id",
        >>>     aws_secret_access_key="secret_access_key",
        >>> )
        >>>
        >>> s3_upload_task = s3_upload(
        >>>    default_values=S3UploadDefaultValues(
        >>>        bucket="data_bucket", boto_kwargs=boto_kwargs
        >>>    ),
        >>>    task_args=TaskArgs(retries=3, retry_delay_seconds=10)
        >>> )
        >>>
        >>> @flow
        >>> def example_s3_upload_flow():
        >>>     with open('data.csv', 'rb') as file:
        >>>         key = s3_upload_task(key="data.csv", data=file.read())


    """
    logger = get_logger()
    logger.info("Uploading object to bucket %s with key %s", bucket, key)

    s3_client = get_boto_client("s3", **boto_kwargs)
    stream = io.BytesIO(data)
    s3_client.upload_fileobj(stream, Bucket=bucket, Key=key)

    return key


@dataclass
class S3ListObjectsDefaultValues(DefaultValues):
    """
    Dataclass that defines default values that can be supplied when creating an S3 list
    objects task
    """

    bucket: Optional[str] = None
    prefix: Optional[str] = ""
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)
    delimiter: Optional[str] = ""
    page_size: Optional[int] = None
    max_items: Optional[int] = None
    jmespath_query: Optional[str] = None


@task_factory(
    default_values_cls=S3ListObjectsDefaultValues,
    required_args=["bucket"],
)
def s3_list_objects(
    bucket: str = None,
    prefix: Optional[str] = None,
    boto_kwargs: Optional[Dict[str, Any]] = None,
    delimiter: Optional[str] = None,
    page_size: Optional[int] = None,
    max_items: Optional[int] = None,
    jmespath_query: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Lists details of objects in a given S3 bucket. AWS authentication is handled via the
    `boto3` module. Refer to the [boto3 docs]
    (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.

    Args:
        bucket: Name of bucket to list items from. Required if a default value was not
            supplied when creating the task.
        prefix: Used to filter objects with keys starting with the specified prefix
        boto_kwargs: Keyword arguments that are forwarded to the boto client used by the
            task. Defaults to an empty dictionary.
        delimiter: Character used to group keys of listed objects
        page_size: Number of objects to return in each request to the AWS API
        max_items: Maximum number of objects that to be returned by task
        jmespath_query: Query used to filter objects based on object attributes refer to
            the [boto3 docs]
            (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#filtering-results-with-jmespath)
            for more information on how to construct queries.

    Returns:
        A list of dictionaries containing information about the objects retrieved. Refer
            to the boto3 docs for an example response.

    Example:
        List all objects in a bucket
        >>> boto_kwargs = dict(
        >>>     aws_access_key_id="acccess_key_id",
        >>>     aws_secret_access_key="secret_access_key",
        >>> )
        >>>
        >>> s3_list_objects_task = s3_list_objects(
        >>>    default_values=S3ListObjectsDefaultValues(
        >>>        bucket="data_bucket", boto_kwargs=boto_kwargs
        >>>    ),
        >>>    task_args=TaskArgs(retries=3, retry_delay_seconds=10)
        >>> )
        >>>
        >>> @flow
        >>> def example_s3_list_objects_flow():
        >>>     objects = s3_list_objects_task()

    """
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

    return [content for page in page_iterator for content in page.get("Contents", [])]
