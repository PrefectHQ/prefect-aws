# prefect-aws

## Welcome!

`prefect-aws` is a collection of pre-built Prefect tasks that can be used to quickly construct Prefect flows that interact with Amazon Web Services.

## Getting Started

### Python setup

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-aws`

```bash
pip install prefect-aws
```

### AWS Authentication

You will need to obtain AWS credentials in order to use these tasks. Refer to the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) for authentication methods available.

### Write and run a flow with prefect-aws tasks
```python
from prefect import flow
from prefect_aws.s3 import s3_upload

@flow
def example_s3_upload_flow():
    aws_credentials = AwsCredentials(
        aws_access_key_id="acccess_key_id",
        aws_secret_access_key="secret_access_key"
    )
    with open("data.csv", "rb") as file:
        key = s3_upload(
            bucket="bucket",
            key="data.csv",
            data=file.read(),
            aws_credentials=aws_credentials,
        )

example_s3_upload_flow()
```

### Write and run a flow with AwsCredentials and S3Bucket

```python
from prefect import flow
from prefect_aws import AwsCredential, S3Bucket

BUCKET_NAME = "dev_bucket"

@flow
def aws_s3_bucket_roundtrip():
    aws_creds = AwsCredentials(
        aws_access_key_id="acccess_key_id",
        aws_secret_access_key="secret_access_key"
    )

    s3_bucket = S3Bucket(
        bucket=BUCKET_NAME,
        credentials=aws_creds,
        basepath="bucket-subfolder" # optional
    )

    key = s3_bucket.write_path("data.csv", content=b"hello")

    contents = s3_bucket.read_path(key)

aws_s3_bucket_roundtrip()
```

### Write and run a flow with MinIOCredentials and S3Bucket

```python
from prefect import flow
from prefect_aws import MinIOCredentials, S3Bucket

BUCKET_NAME = "dev_bucket"

@flow
def minio_s3_bucket_roundtrip():
    minio_creds = MinIOCredentials(
        minio_root_user="acccess_key_id",
        minio_root_password="secret_access_key"
    )

    s3_bucket = S3Bucket(
        bucket=BUCKET_NAME,
        credentials=miniocreds,
        endpoint="http://localhost:9000"
    )

    key = s3_bucket.write_path("data.csv", content=b"hello")

    contents = s3_bucket.read_path(key)

minio_s3_bucket_roundtrip()
```


## Next steps

Refer to the API documentation in the side menu to explore all the capabilities of Prefect AWS!

## Resources

If you encounter and bugs while using `prefect-aws`, feel free to open an issue in the [prefect-aws](https://github.com/PrefectHQ/prefect-aws) repository.

If you have any questions or issues while using `prefect-aws`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack)

## Development

If you'd like to install a version of `prefect-aws` for development, first clone the repository and then perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-aws.git

cd prefect-aws/

pip install -e ".[dev]"
```
