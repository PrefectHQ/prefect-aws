# prefect-aws

## Welcome!

Prefect AWS is a collections of prebuilt Prefect tasks that can be used to quickly construct Prefect flows that interact with Amazon Web Services.

## Getting Started

### Python setup

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).
### Installation

Install Prefect and Prefect AWS

```bash
pip install "prefect>=2.0a12" prefect-aws
```

### Write and run a flow

```python
from prefect import flow
from prefect_aws import s3_upload

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

## Next steps

Refer to the API documentation in the side menu to explore all the capabilities of Prefect AWS!

## Development

If you'd like to install a version of Prefect AWS for development, first clone the Prefect AWS repository and then install in editable mode with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-aws.git 
cd prefect-aws/
pip install -e ".[dev]"
```