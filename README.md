# Coordinate and incorporate AWS in your dataflow with prefect-aws

<p align="center">
    <img src="https://user-images.githubusercontent.com/15331990/214123296-4cfa69ed-d105-4ca2-a351-4c21917086c7.png">
    <br>
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

Visit the full docs [here](https://PrefectHQ.github.io/prefect-aws) to see additional examples and the API reference.

## Welcome!

`prefect-aws` is a collection of pre-built Prefect tasks that can be used to quickly construct Prefect flows that interact with Amazon Web Services.

## Getting Started

### Saving credentials to a block

You will need to obtain AWS credentials in order to use `prefect-aws`.

1. Refer to the [AWS Configuration documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds) on how to retrieve your access key ID and secret access key
2. Copy the access key ID and secret access key
3. Create a short script, replacing the placeholders (or do so in the UI)

```python
from prefect_aws import AwsCredentials
AwsCredentials(
    aws_access_key_id="PLACEHOLDER",
    aws_secret_access_key="PLACEHOLDER",
    aws_session_token=None,  # replace this with token if necessary
).save("BLOCK_NAME_PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:
 
```python
from prefect_aws import AwsCredentials
AwsCredentials.load("BLOCK_NAME_PLACEHOLDER")
```

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
from prefect_aws import AwsCredentials, AwsSecret

@flow
def example_flow():
    secrets_manager = AwsSecret.load("my-block")
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

## Resources

Refer to the API documentation on the sidebar to explore all the capabilities of Prefect AWS!

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Recipes

A tutorial on `ECSTask` can be found [here](https://towardsdatascience.com/prefect-aws-ecs-fargate-github-actions-make-serverless-dataflows-as-easy-as-py-f6025335effc).

For additional recipes and examples, check out [`prefect-recipes`](https://github.com/PrefectHQ/prefect-recipes).

### Feedback

If you have any questions or issues while using `prefect-aws`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).
 
Feel free to star or watch [`prefect-aws`](https://github.com/PrefectHQ/prefect-aws) for updates too!

### Installation

Install `prefect-aws`

```bash
pip install prefect-aws
```

A list of available blocks in `prefect-aws` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-aws/#blocks-catalog).

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Contributing

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
