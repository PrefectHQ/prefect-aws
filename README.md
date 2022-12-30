# prefect-aws

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
Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_aws
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

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

## Next steps

Refer to the API documentation in the side menu to explore all the capabilities of Prefect AWS!

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you encounter and bugs while using `prefect-aws`, feel free to open an issue in the [prefect-aws](https://github.com/PrefectHQ/prefect-aws) repository.

If you have any questions or issues while using `prefect-aws`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack)

Feel free to ⭐️ or watch [`prefect-aws`](https://github.com/PrefectHQ/prefect-aws) for updates too!

## Development

If you'd like to install a version of `prefect-aws` for development, first clone the repository and then perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-aws.git

cd prefect-aws/

pip install -e ".[dev]"
```
