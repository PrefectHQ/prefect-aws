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


## Getting Started

### Saving credentials to a block

You will need an AWS account and credentials in order to use `prefect-aws`.

1. Refer to the [AWS Configuration documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds) on how to retrieve your access key ID and secret access key
2. Copy the access key ID and secret access key
3. Create a short script and replace the placeholders with your credential information and desired block name:

```python
from prefect_aws import AwsCredentials
AwsCredentials(
    aws_access_key_id="PLACEHOLDER",
    aws_secret_access_key="PLACEHOLDER",
    aws_session_token=None,  # replace this with token if necessary
    region_name="us-east-2"
).save("BLOCK-NAME-PLACEHOLDER")
```

Congrats! You can now load the saved block to use your credentials in your Python code:
 
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

### Using Prefect with AWS ECS

`prefect_aws` allows you to use [AWS ECS](https://aws.amazon.com/ecs/) as infrastructure for your deployments. Using ECS for scheduled flow runs enables the dynamic provisioning of infrastructure for containers and unlocks greater scalability.

The snippets below show how you can use `prefect_aws` to run a task on ECS. The first example uses the `ECSTask` block as [infrastructure](https://docs.prefect.io/concepts/infrastructure/) and the second example shows using ECS within a flow.

#### As deployment Infrastructure


##### Set variables

To expedite copy/paste without the needing to update placeholders manually, update and execute the following.

```bash
export CREDENTIALS_BLOCK_NAME="aws-credentials"
export VPC_ID="vpc-id"
export ECS_TASK_BLOCK_NAME="ecs-task-example"
export S3_BUCKET_BLOCK_NAME="ecs-task-bucket-example"
```

##### Save an infrastructure and storage block

Save a custom infrastructure and storage block by executing the following snippet.

```python
import os
from prefect_aws import AwsCredentials, ECSTask, S3Bucket

aws_credentials = AwsCredentials.load(os.environ["CREDENTIALS_BLOCK_NAME"])

ecs_task = ECSTask(
    image="prefecthq/prefect:2-python3.10",
    aws_credentials=aws_credentials,
    vpc_id=os.environ["VPC_ID"],
)
ecs_task.save(os.environ["ECS_TASK_BLOCK_NAME"], overwrite=True)

bucket_name = "ecs-task-bucket-example"
s3_client = aws_credentials.get_s3_client()
s3_client.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={"LocationConstraint": aws_credentials.region_name}
)
s3_bucket = S3Bucket(
    bucket_name=bucket_name,
    credentials=aws_credentials,
)
s3_bucket.save(os.environ["S3_BUCKET_BLOCK_NAME"], overwrite=True)
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
    -sb s3-bucket/${S3_BUCKET_BLOCK_NAME} \
    --override env.EXTRA_PIP_PACKAGES=prefect-aws
```

Now apply the deployment!

```bash
prefect deployment apply ecs_task_flow-deployment.yaml
```

##### Test the deployment

Start an [agent](https://docs.prefect.io/latest/concepts/work-pools/) in a separate terminal. The agent will poll the Prefect API's work pool for scheduled flow runs.

```bash
prefect agent start -q 'default'
```

Run the deployment once to test it:

```bash
prefect deployment run ecs-task-flow/ecs-task-deployment
```

Once the flow run has completed, you will see `Hello, Prefect!` logged in the CLI and the Prefect UI.

!!! info "No class found for dispatch key"

    If you encounter an error message like `KeyError: "No class found for dispatch key 'ecs-task' in registry for type 'Block'."`,
    ensure `prefect-aws` is installed in the environment in which your agent is running!

Another tutorial on `ECSTask` can be found [here](https://towardsdatascience.com/prefect-aws-ecs-fargate-github-actions-make-serverless-dataflows-as-easy-as-py-f6025335effc).

#### Within Flow

You can also execute commands with an `ECSTask` block directly within a Prefect flow. Running containers via ECS in your flows is useful for executing non-Python code in a distributed manner while using Prefect.

```python
from prefect import flow
from prefect_aws import AwsCredentials
from prefect_aws.ecs import ECSTask

@flow
def ecs_task_flow():
    ecs_task = ECSTask(
        image="prefecthq/prefect:2-python3.10",
        credentials=AwsCredentials.load("BLOCK-NAME-PLACEHOLDER"),
        region="us-east-2",
        command=["echo", "Hello, Prefect!"],
    )
    return ecs_task.run()
```

This setup gives you all of the observation and orchestration benefits of Prefect, while also providing you the scalability of ECS.

### Using Prefect with AWS S3

`prefect_aws` allows you to read and write objects with AWS S3 within your Prefect flows.

The provided code snippet shows how you can use `prefect_aws` to upload a file to a AWS S3 bucket and download the same file under a different file name.

Note, the following code assumes that the bucket already exists.

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
        bucket_name="BUCKET-NAME-PLACEHOLDER",
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

For additional recipes and examples, check out [`prefect-recipes`](https://github.com/PrefectHQ/prefect-recipes).

### Installation

Install `prefect-aws`

```bash
pip install prefect-aws
```

A list of available blocks in `prefect-aws` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-aws/#blocks-catalog).

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Feedback

If you encounter any bugs while using `prefect-aws`, feel free to open an issue in the [`prefect-aws`](https://github.com/PrefectHQ/prefect-aws) repository.

If you have any questions or issues while using `prefect-aws`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).
 
Feel free to star or watch [`prefect-aws`](https://github.com/PrefectHQ/prefect-aws) for updates too!
