import json
from functools import partial
from typing import Callable, Dict, List

import anyio
import pytest
import yaml
from moto import mock_ec2, mock_ecs, mock_logs
from moto.ec2.utils import generate_instance_identity_document
from prefect.logging.configuration import setup_logging
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import ValidationError

from prefect_aws.ecs import ECSTask

setup_logging()


BASE_TASK_DEFINITION_YAML = """
containerDefinitions:
- cpu: 1024
  image: prefecthq/prefect:2.1.0-python3.8
  memory: 2048
  name: prefect
family: prefect
"""

BASE_TASK_DEFINITION = yaml.safe_load(BASE_TASK_DEFINITION_YAML)


def inject_moto_patches(moto_mock, patches: Dict[str, List[Callable]]):
    def injected_call(method, patch_list, *args, **kwargs):
        for patch in patch_list:
            result = patch(method, *args, **kwargs)
        return result

    for account in moto_mock.backends:
        for region in moto_mock.backends[account]:
            backend = moto_mock.backends[account][region]

            for attr, attr_patches in patches.items():
                original_method = getattr(backend, attr)
                setattr(
                    backend, attr, partial(injected_call, original_method, attr_patches)
                )


def add_prefect_container(describe_tasks, *args, **kwargs):
    """
    Adds the minimal prefect container to moto's task description.
    """
    result = describe_tasks(*args, **kwargs)
    for task in result:
        task.containers = [{"name": "prefect"}]
    return result


def fix_resource_calculations_for_fargate(
    _calculate_task_resource_requirements, task_definition
):
    """
    Adds support for non-EC2 execution modes to moto's calculation of task definition.
    """
    for container_definition in task_definition.container_definitions:
        container_definition.setdefault("memory", 0)
    return _calculate_task_resource_requirements(task_definition)


def add_ec2_instance_to_ecs_cluster(session, cluster_name):
    ecs_client = session.client("ecs")
    ec2_client = session.client("ec2")
    ec2_resource = session.resource("ec2")

    ecs_client.create_cluster(clusterName=cluster_name)

    images = ec2_client.describe_images()
    image_id = images["Images"][0]["ImageId"]

    test_instance = ec2_resource.create_instances(
        ImageId=image_id, MinCount=1, MaxCount=1
    )[0]

    ecs_client.register_container_instance(
        cluster=cluster_name,
        instanceIdentityDocument=json.dumps(
            generate_instance_identity_document(test_instance)
        ),
    )


def create_test_ecs_cluster(ecs_client, cluster_name):
    return ecs_client.create_cluster(clusterName=cluster_name)


def describe_task(ecs_client, task_arn, **kwargs) -> dict:
    return ecs_client.describe_tasks(tasks=[task_arn], **kwargs)["tasks"][0]


# def describe_task_definition(ecs_client, task_definition_arn, **kwargs) -> dict:
#     return ecs_client.describe_task_definitions(tasks=[task_definition_arn], **kwargs)[
#         "taskDefinitions"
#     ][0]


async def stop_task(task_arn, ecs_client):
    await run_sync_in_worker_thread(ecs_client.stop_task, task=task_arn)


def retrieve_task_definition_for_task(ecs_client, task_arn):
    task = describe_task(ecs_client, task_arn)
    return ecs_client.describe_task_definition(
        taskDefinition=task["taskDefinitionArn"]
    )["taskDefinition"]


async def run_then_stop_task(task: ECSTask) -> str:
    session = task.aws_credentials.get_boto3_session()

    async with anyio.create_task_group() as tg:
        task_arn = await tg.start(task.run)
        # Stop the task after it starts to prevent the test from running forever
        tg.start_soon(stop_task, task_arn, session.client("ecs"))

    return task_arn


@pytest.fixture
def ecs_mocks(aws_credentials):
    with mock_ecs() as ecs:
        with mock_ec2():
            inject_moto_patches(
                ecs,
                {
                    "describe_tasks": [add_prefect_container],
                    "_calculate_task_resource_requirements": [
                        fix_resource_calculations_for_fargate
                    ],
                },
            )

            session = aws_credentials.get_boto3_session()
            create_test_ecs_cluster(session.client("ecs"), "default")

            # NOTE: Even when using FARGATE, moto requires container instances to be
            #       registered. This differs from AWS behavior.
            add_ec2_instance_to_ecs_cluster(session, "default")

            yield ecs


@pytest.mark.usefixtures("ecs_mocks")
async def test_ec2_task_run(aws_credentials):
    task = ECSTask(
        aws_credentials=aws_credentials,
        command=["prefect", "version"],
        launch_type="EC2",
    )

    session = aws_credentials.get_boto3_session()
    print(task.preview())

    task_arn = await run_then_stop_task(task)


@pytest.mark.usefixtures("ecs_mocks")
async def test_fargate_task_run(aws_credentials):
    task = ECSTask(
        aws_credentials=aws_credentials,
        command=["prefect", "version"],
        launch_type="FARGATE",
    )

    session = aws_credentials.get_boto3_session()
    print(task.preview())

    task_arn = await run_then_stop_task(task)


@pytest.mark.usefixtures("ecs_mocks")
async def test_fargate_spot_task_run(aws_credentials):
    task = ECSTask(
        aws_credentials=aws_credentials,
        command=["prefect", "version"],
        launch_type="FARGATE_SPOT",
    )

    session = aws_credentials.get_boto3_session()

    print(task.preview())

    task_arn = await run_then_stop_task(task)


@pytest.mark.usefixtures("ecs_mocks")
async def test_logging_requires_execution_role_arn(aws_credentials):
    with pytest.raises(ValidationError, match="`execution_role_arn` must be provided"):
        ECSTask(
            aws_credentials=aws_credentials,
            command=["prefect", "version"],
            configure_cloudwatch_logs=True,
        )


@pytest.mark.usefixtures("ecs_mocks")
async def test_logging_requires_execution_role_arn_at_runtime(aws_credentials):
    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")
    task_definition_arn = ecs_client.register_task_definition(**BASE_TASK_DEFINITION)[
        "taskDefinition"
    ]["taskDefinitionArn"]

    task = ECSTask(
        aws_credentials=aws_credentials,
        command=["prefect", "version"],
        configure_cloudwatch_logs=True,
        task_definition_arn=task_definition_arn,
    )
    with pytest.raises(ValueError, match="An execution role arn must be set"):
        await task.run()


@pytest.mark.usefixtures("ecs_mocks")
async def test_configure_cloudwatch_logging(aws_credentials):
    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    with mock_logs():
        task = ECSTask(
            aws_credentials=aws_credentials,
            command=["prefect", "version"],
            configure_cloudwatch_logs=True,
            execution_role_arn="test",
        )

    task_arn = await run_then_stop_task(task)
    task_definition = retrieve_task_definition_for_task(ecs_client, task_arn)

    for container in task_definition["containerDefinitions"]:
        if container["name"] == "prefect":
            # Assert that the 'prefect' container has logging configured
            assert container["logConfiguration"] == {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-create-group": "true",
                    "awslogs-group": "prefect",
                    "awslogs-region": "us-east-1",
                    "awslogs-stream-prefix": "prefect",
                },
            }
        else:
            # Other containers should not be modifed
            assert "logConfiguration" not in container
