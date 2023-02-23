# Coordinate and incorporate AWS in your dataflow with `prefect-aws`

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

## Welcome!

The `prefect-aws` collection makes it easy to leverage the capabilities of AWS in your flows, featuring support for ECSTask, S3, Secrets Manager, Batch Job, and Client Waiter.

Visit the full docs [here](https://PrefectHQ.github.io/prefect-aws).

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
).save("BLOCK-NAME-PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:
 
```python
from prefect_aws import AwsCredentials
AwsCredentials.load("BLOCK-NAME-PLACEHOLDER")
```

!!! info "Registering blocks"

    Register blocks in this module to
    [view and edit them](https://docs.prefect.io/ui/blocks/)
    on Prefect Cloud:

    ```bash
    prefect block register -m prefect_aws
    ```

### Using Prefect with Amazon ECS

`prefect_aws` allows you to interact with Amazon ECS Tasks with Prefect flows.

The snippets below show how you can use `prefect_aws` to run a task on ECS. It uses the `ECSTask` block as [Prefect infrastructure](https://docs.prefect.io/concepts/infrastructure/) or simply within a flow.

#### As Infrastructure

You can also use ECS Tasks as infrastructure to execute your deployed flows.

##### Set variables

To expedite copy/paste without the needing to update placeholders manually, update and execute the following.

```bash
export CREDENTIALS_BLOCK_NAME="BLOCK-NAME-PLACEHOLDER"
export ECS_TASK_BLOCK_NAME="ecs-task-example"
export ECS_TASK_REGION="us-east-1"
export GCS_BUCKET_BLOCK_NAME="ecs-task-bucket-example"
```

##### Save an infrastructure and storage block

Save a custom infrastructure and storage block by executing the following snippet.

```python
import os
from prefect_aws import AwsCredentials, ECSTask, S3Bucket

aws_credentials = AwsCredentials.load(os.environ["CREDENTIALS_BLOCK_NAME"])

ecs_task = ECSTask(
    image="",
    credentials=aws_credentials,
    region=os.environ["ECS_TASK_REGION"],
)
ecs_task.save(os.environ["ECS_TASK_BLOCK_NAME"], overwrite=True)

bucket_name = "ecs-task-bucket"
cloud_storage_client = aws_credentials.get_cloud_storage_client()
cloud_storage_client.create_bucket(bucket_name)
s3_bucket = S3Bucket(
    bucket=bucket_name,
    aws_credentials=aws_credentials,
)
s3_bucket.save(os.environ["GCS_BUCKET_BLOCK_NAME"], overwrite=True)
```

##### Write a flow

Then, use an existing flow to create a deployment with, or use the flow below if you don't have an existing flow handy.

```python
from prefect import flow

@flow(log_prints=True)
def ecs_task_flow():
    print("Hello, Prefect!")

if __name__ == "__main__":
    ecs_task_flow()
```

##### Create a deployment

If the script was named "ecs_task_script.py", build a deployment manifest with the following command.

```bash
prefect deployment build ecs_task_script.py:ecs_task_flow \
    -n ecs-task-deployment \
    -ib ecs-task/${ECS_TASK_BLOCK_NAME} \
    -sb s3-bucket/${GCS_BUCKET_BLOCK_NAME}
```

Now apply the deployment!

```bash
prefect deployment apply ecs_task_flow-deployment.yaml
```

##### Test the deployment

Start up an agent in a separate terminal. The agent will poll the Prefect API for scheduled flow runs that are ready to run.

```bash
prefect agent start -q 'default'
```

Run the deployment once to test.

```bash
prefect deployment run ecs-task-flow/ecs-task-deployment
```

Once the flow run has completed, you will see `Hello, Prefect!` logged in the Prefect UI.

!!! info "No class found for dispatch key"

    If you encounter an error message like `KeyError: "No class found for dispatch key 'ecs-task' in registry for type 'Block'."`,
    ensure `prefect-aws` is installed in the environment that your agent is running!

#### Within Flow

You can execute commands through ECS Task directly within a Prefect flow.

```python
from prefect import flow
from prefect_aws import AwsCredentials
from prefect_aws.ecs import ECSTask

@flow
def ecs_task_flow():
    ecs_task = ECSTask(
        image="",
        credentials=AwsCredentials.load("BLOCK-NAME-PLACEHOLDER"),
        region="us-east-1",
        command=["echo", "Hello, Prefect!"],
    )
    return ecs_task.run()
```

### Using Prefect with AWS S3

`prefect_aws` allows you to read and write objects with AWS S3 within your Prefect flows.

The provided code snippet shows how you can use `prefect_aws` to upload a file to a AWS S3 bucket and download the same file under a different file name.

```python
from pathlib import Path
from prefect import flow
from prefect_aws import AwsCredentials, S3Bucket

@flow
def s3_flow():
    # create a dummy file to upload
    file_path = Path("test-example.txt")
    file_path.write_text("Hello, Prefect!")

    aws_credentials = AwsCredentials.load("BLOCK-NAME-PLACEHOLDER")
    s3_bucket = S3Bucket(
        bucket="BUCKET-NAME-PLACEHOLDER",
        aws_credentials=aws_credentials
    )

    s3_bucket_path = s3_bucket.upload_from_path(file_path)
    downloaded_file_path = s3_bucket.download_object_to_path(
        s3_bucket_path, "downloaded-test-example.txt"
    )
    return downloaded_file_path.read_text()

s3_flow()
```

### Using Prefect with AWS Secrets Manager

`prefect_aws` allows you to read and write secrets with AWS Secrets Manager within your Prefect flows.

The provided code snippet shows how you can use `prefect_aws` to write a secret to the Secret Manager, read the secret data, delete the secret, and finally return the secret data.

```python
from prefect import flow
from prefect_aws import AwsCredentials, AwsSecret

@flow
def secrets_manager_flow():
    aws_credentials = AwsCredentials.load("BLOCK-NAME-PLACEHOLDER")
    aws_secret = AwsSecret(secret_name="test-example", aws_credentials=aws_credentials)
    aws_secret.write_secret(secret_data=b"Hello, Prefect!")
    secret_data = aws_secret.read_secret()
    aws_secret.delete_secret()
    return secret_data

secrets_manager_flow()
```

## Resources

Refer to the API documentation on the sidebar to explore all the capabilities of Prefect AWS!

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Recipes

A tutorial on `ECSTaskTask` can be found [here](https://towardsdatascience.com/prefect-aws-ecs-fargate-github-actions-make-serverless-dataflows-as-easy-as-py-f6025335effc).

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
