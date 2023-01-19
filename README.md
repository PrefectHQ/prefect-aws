# prefect-aws

Visit the full docs [here](https://PrefectHQ.github.io/prefect-aws) to see additional examples and the API reference.

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-aws/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-aws?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/prefecthq/prefect-aws/" alt="Stars">
        <img src="https://img.shields.io/github/stars/prefecthq/prefect-aws?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-aws/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-aws?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/prefecthq/prefect-aws/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/prefecthq/prefect-aws?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

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

A list of available blocks in `prefect-aws` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-aws/#blocks-catalog).

### AWS Authentication

You will need to obtain AWS credentials in order to use these tasks. Refer to the [AWS documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html) for authentication methods available.

### Write and run a flow

#### Upload and download from S3Bucket
```python
from prefect import flow
from prefect_aws.s3 import S3Bucket

@flow
def example_flow():
    with open("hello.py", "w") as f:
        f.write("print('Hello world!')")

    s3_bucket = S3Bucket.load("my-bucket-test")
    s3_bucket.upload_from_path("hello.py")
    s3_bucket.download_object_to_path("hello.py", "downloaded_hello.py")

example_flow()
```

#### Write, read, and delete secret from AWS Secrets Manager
```python
from prefect import flow
from prefect_aws import AwsCredentials, SecretsManager

@flow
def example_flow():
    secrets_manager = SecretsManager.load("my-block")
    secrets_manager.write_secret("my-secret-value")
    secret = secrets_manager.read_secret()
    print(secret)
    secrets_manager.delete_secret()

example_flow()
```

#### Use `with_options` to customize options on any existing task or flow

```python
custom_example_flow = example_flow.with_options(
    name="My custom task name",
    retries=2,
    retry_delay_seconds=10,
) 
```

## Next steps

Refer to the API documentation in the side menu to explore all the capabilities of Prefect AWS!

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you have any questions or issues while using `prefect-aws`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).
 
Feel free to star or watch [`prefect-aws`](https://github.com/PrefectHQ/prefect-aws) for updates too!

## Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-aws`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-aws/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
