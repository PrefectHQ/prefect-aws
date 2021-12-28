import dataclasses
import io
from typing import Any, Dict, Optional
import uuid

from prefect.tasks import task
from prefect.utilities.logging import get_logger
from dataclasses import dataclass, field, asdict

from prefect_aws.exceptions import MissingRequiredArgument
from prefect_aws.schema import TaskArgs
from prefect_aws.utilities import get_boto_client


@dataclass
class S3DownloadDefaultValues:
    bucket: Optional[str] = None
    key: Optional[str] = None
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)


def create_s3_download_task(
    default_values: Optional[S3DownloadDefaultValues] = None,
    task_args: Optional[TaskArgs] = None,
):
    default_values = default_values or S3DownloadDefaultValues()
    task_args = task_args or TaskArgs()

    @task(**asdict(task_args))
    def s3_download(
        bucket: Optional[str] = default_values.bucket,
        key: Optional[str] = default_values.key,
        boto_kwargs: Dict[str, Any] = default_values.boto_kwargs,
    ):
        if bucket is None:
            raise MissingRequiredArgument("bucket")

        if key is None:
            raise MissingRequiredArgument("key")

        logger = get_logger()
        logger.info("Downloading file from bucket %s with key %s", bucket, key)

        s3_client = get_boto_client("s3", **boto_kwargs)
        stream = io.BytesIO()
        s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)
        stream.seek(0)
        output = stream.read()

        return output

    return s3_download


@dataclass
class S3UploadDefaultValues:
    bucket: Optional[str] = None
    key: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    boto_kwargs: Dict[str, Any] = field(default_factory=dict)


def create_s3_upload_task(
    default_values: Optional[S3UploadDefaultValues] = None,
    task_args: Optional[TaskArgs] = None,
):
    default_values = default_values or S3UploadDefaultValues()
    task_args = task_args or TaskArgs()

    @task(**asdict(task_args))
    def s3_upload(
        data: bytes,
        key: Optional[str] = default_values.key,
        bucket: Optional[str] = default_values.bucket,
        boto_kwargs: Dict[str, Any] = default_values.boto_kwargs,
    ):
        if bucket is None:
            raise MissingRequiredArgument("bucket")

        if key is None:
            raise MissingRequiredArgument("key")

        if data is None:
            raise MissingRequiredArgument("data")

        logger = get_logger()
        logger.info("Upload file to bucket %s with key %s", bucket, key)

        s3_client = get_boto_client("s3", **boto_kwargs)
        stream = io.BytesIO(data)
        s3_client.upload_fileobj(stream, Bucket=bucket, Key=key)

        return key

    return s3_upload
