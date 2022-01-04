# prefect-aws

Collection of pre-built Prefect tasks and flows for interacting with AWS.

## Example

Download a file from S3

```python
from prefect import flow
from prefect_aws.s3 import (
    S3DownloadDefaultValues,
    s3_download,
)
from prefect_aws.schema import TaskArgs

boto_kwargs = dict(
    aws_access_key_id="access_key_id",
    aws_secret_access_key="secret_access_key",
)

s3_download_task = s3_download(
    default_values=S3DownloadDefaultValues(
        bucket="data_bucket", boto_kwargs=boto_kwargs
    ),
    task_args=TaskArgs(retries=3, retry_delay_seconds=10)
)


@flow
def test_s3_download():
    data = s3_download_task(key="data.csv")


state = test_s3_download()
print(state)

```

## Installation

```
git clone https://github.com/PrefectHQ/prefect-aws.git
pip install ./prefect-aws
```
