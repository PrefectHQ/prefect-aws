import io
from typing import Dict, Optional

from prefect.tasks import Task

from prefect_aws.utilities import defaults_from_attrs, get_boto_client


class S3Download(Task):
    def __init__(
        self,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        boto_kwargs: Optional[Dict] = {},
        **kwargs,
    ):
        self.bucket = bucket
        self.boto_kwargs = boto_kwargs

        super().__init__(**kwargs)

    @defaults_from_attrs("bucket", "key", "boto_kwargs")
    def __call__(
        self,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        boto_kwargs: Optional[Dict] = {},
    ):
        s3_client = get_boto_client("s3", **boto_kwargs)

        stream = io.BytesIO()

        s3_client.download_fileobj(Bucket=bucket, Key=key, Fileobj=stream)

        stream.seek(0)
        output = stream.read()

        return output
