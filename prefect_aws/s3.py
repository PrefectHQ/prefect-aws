"""Tasks for interacting with AWS S3"""
import io
import uuid
from functools import partial
from typing import Any, Dict, List, Optional

from anyio import to_thread
from botocore.paginate import PageIterator
from prefect import get_run_logger, task

from prefect_aws import AwsCredentials
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
        aws_client_parameters: Custom parameter for the boto3 client initialization..


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
    download = partial(
        s3_client.download_fileobj, Bucket=bucket, Key=key, Fileobj=stream
    )
    await to_thread.run_sync(download)
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
    upload = partial(s3_client.upload_fileobj, stream, Bucket=bucket, Key=key)
    await to_thread.run_sync(upload)

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

    return await to_thread.run_sync(_list_objects_sync, page_iterator)
