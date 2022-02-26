# prefect-aws

Collection of pre-built Prefect tasks and flows for interacting with AWS.

## Example

Download a file from S3

```python
from prefect import flow
from prefect_aws import AwsCredentials
from prefect_aws.s3 import s3_download

@flow
def example_s3_download():
    aws_credentials = AwsCredentials(
        aws_access_key_id="access_key_id",
        aws_secret_access_key="secret_access_key",
    )
    
    return s3_download(
        bucket="data_bucket",
        key="data.csv",
        aws_credentials=aws_credentials
    )


test_s3_download()
```

## Installation

```
git clone https://github.com/PrefectHQ/prefect-aws.git
pip install ./prefect-aws
```
