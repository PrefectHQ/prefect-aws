import json
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional
from uuid import uuid4

import anyio
import pytest
from moto import mock_ec2, mock_ecs, mock_logs
from moto.ec2.utils import generate_instance_identity_document
from prefect.server.schemas.core import FlowRun
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from prefect_aws.workers.ecs_worker import (
    ECS_DEFAULT_CONTAINER_NAME,
    ECS_DEFAULT_CPU,
    ECS_DEFAULT_MEMORY,
    AwsCredentials,
    ECSJobConfiguration,
    ECSVariables,
    ECSWorker,
    _default_task_definition_template,
    _default_task_run_request_template,
    _get_container,
    parse_identifier,
)


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid4(), deployment_id=uuid4())


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


def patch_describe_tasks_add_prefect_container(describe_tasks, *args, **kwargs):
    """
    Adds the minimal prefect container to moto's task description.
    """
    result = describe_tasks(*args, **kwargs)
    for task in result:
        task.containers = [{"name": ECS_DEFAULT_CONTAINER_NAME}]
    return result


def patch_run_task(mock, run_task, *args, **kwargs):
    """
    Track calls to `run_task` by calling a mock as well.
    """
    mock(*args, **kwargs)
    return run_task(*args, **kwargs)


def patch_calculate_task_resource_requirements(
    _calculate_task_resource_requirements, task_definition
):
    """
    Adds support for non-EC2 execution modes to moto's calculation of task definition.
    """
    for container_definition in task_definition.container_definitions:
        container_definition.setdefault("memory", 0)
    return _calculate_task_resource_requirements(task_definition)


def create_log_stream(session, run_task, *args, **kwargs):
    """
    When running a task, create the log group and stream if logging is configured on
    containers.

    See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html
    """
    tasks = run_task(*args, **kwargs)
    if not tasks:
        return tasks
    task = tasks[0]

    ecs_client = session.client("ecs")
    logs_client = session.client("logs")

    task_definition = ecs_client.describe_task_definition(
        taskDefinition=task.task_definition_arn
    )["taskDefinition"]

    for container in task_definition.get("containerDefinitions", []):
        log_config = container.get("logConfiguration", {})
        if log_config:
            if log_config.get("logDriver") != "awslogs":
                continue

            options = log_config.get("options", {})
            if not options:
                raise ValueError("logConfiguration does not include options.")

            group_name = options.get("awslogs-group")
            if not group_name:
                raise ValueError(
                    "logConfiguration.options does not include awslogs-group"
                )

            if options.get("awslogs-create-group") == "true":
                logs_client.create_log_group(logGroupName=group_name)

            stream_prefix = options.get("awslogs-stream-prefix")
            if not stream_prefix:
                raise ValueError(
                    "logConfiguration.options does not include awslogs-stream-prefix"
                )

            logs_client.create_log_stream(
                logGroupName=group_name,
                logStreamName=f"{stream_prefix}/{container['name']}/{task.id}",
            )

    return tasks


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


def create_test_ecs_cluster(ecs_client, cluster_name) -> str:
    """
    Create an ECS cluster and return its ARN
    """
    return ecs_client.create_cluster(clusterName=cluster_name)["cluster"]["clusterArn"]


def describe_task(ecs_client, task_arn, **kwargs) -> dict:
    """
    Describe a single ECS task
    """
    return ecs_client.describe_tasks(tasks=[task_arn], **kwargs)["tasks"][0]


async def stop_task(ecs_client, task_arn, **kwargs):
    """
    Stop an ECS task.

    Additional keyword arguments are passed to `ECSClient.stop_task`.
    """
    task = await run_sync_in_worker_thread(describe_task, ecs_client, task_arn)
    # Check that the task started successfully
    assert task["lastStatus"] == "RUNNING", "Task should be RUNNING before stopping"
    print("Stopping task...")
    await run_sync_in_worker_thread(ecs_client.stop_task, task=task_arn, **kwargs)


def describe_task_definition(ecs_client, task):
    return ecs_client.describe_task_definition(
        taskDefinition=task["taskDefinitionArn"]
    )["taskDefinition"]


@pytest.fixture
def ecs_mocks(aws_credentials):
    with mock_ecs() as ecs:
        with mock_ec2():
            with mock_logs():
                session = aws_credentials.get_boto3_session()

                inject_moto_patches(
                    ecs,
                    {
                        # Add a container when describing any task
                        "describe_tasks": [patch_describe_tasks_add_prefect_container],
                        # Fix moto internal resource requirement calculations
                        "_calculate_task_resource_requirements": [
                            patch_calculate_task_resource_requirements
                        ],
                        # Add log group creation
                        "run_task": [partial(create_log_stream, session)],
                    },
                )

                create_test_ecs_cluster(session.client("ecs"), "default")

                # NOTE: Even when using FARGATE, moto requires container instances to be
                #       registered. This differs from AWS behavior.
                add_ec2_instance_to_ecs_cluster(session, "default")

                yield ecs


async def construct_configuration(**options):
    extras = {
        "task_definition": _default_task_definition_template(),
        "task_run_request": _default_task_run_request_template(),
    }
    variables = ECSVariables(**options)
    print(f"Using variables: {variables.json(indent=2)}")

    configuration = await ECSJobConfiguration.from_template_and_values(
        base_job_template=ECSWorker.get_default_base_job_template(),
        values={**extras, **variables.dict(exclude_none=True)},
    )
    print(f"Constructed test configuration: {configuration.json(indent=2)}")

    return configuration


async def run_then_stop_task(
    worker: ECSWorker,
    configuration: ECSJobConfiguration,
    flow_run: FlowRun,
    after_start: Optional[Callable[[str], Awaitable[Any]]] = None,
) -> str:
    """
    Run an ECS Task then stop it.

    Moto will not advance the state of tasks, so `ECSTask.run` would hang forever if
    the run is created successfully and not stopped.

    `after_start` can be used to run something after the task starts but before it is
    stopped. It will be passed the task arn.
    """
    session = configuration.aws_credentials.get_boto3_session()
    result = None

    async def run(task_status):
        nonlocal result
        result = await worker.run(flow_run, configuration, task_status=task_status)
        return

    with anyio.fail_after(20):
        async with anyio.create_task_group() as tg:
            identifier = await tg.start(run)
            cluster, task_arn = parse_identifier(identifier)

            if after_start:
                await after_start(task_arn)

            # Stop the task after it starts to prevent the test from running forever
            tg.start_soon(
                partial(stop_task, session.client("ecs"), task_arn, cluster=cluster)
            )

    return result


@pytest.mark.usefixtures("ecs_mocks")
async def test_default(aws_credentials: AwsCredentials, flow_run: FlowRun):
    configuration = await construct_configuration(aws_credentials=aws_credentials)

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)
    task = describe_task(ecs_client, task_arn)
    assert task["lastStatus"] == "STOPPED"


@pytest.mark.usefixtures("ecs_mocks")
@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE", "FARGATE_SPOT"])
async def test_launch_types(
    aws_credentials: AwsCredentials, launch_type: str, flow_run: FlowRun
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials, launch_type=launch_type
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)

    task = describe_task(ecs_client, task_arn)
    task_definition = describe_task_definition(ecs_client, task)

    if launch_type != "FARGATE_SPOT":
        assert launch_type in task_definition["compatibilities"]
        assert task["launchType"] == launch_type
    else:
        assert "FARGATE" in task_definition["compatibilities"]
        # FARGATE SPOT requires a null launch type
        assert not task.get("launchType")
        # Instead, it requires a capacity provider strategy but this is not supported
        # by moto and is not present on the task even when provided
        # assert task["capacityProviderStrategy"] == [
        #     {"capacityProvider": "FARGATE_SPOT", "weight": 1}
        # ]

    requires_capabilities = task_definition.get("requiresCompatibilities", [])
    if launch_type != "EC2":
        assert "FARGATE" in requires_capabilities
    else:
        assert not requires_capabilities


@pytest.mark.usefixtures("ecs_mocks")
@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE", "FARGATE_SPOT"])
@pytest.mark.parametrize(
    "cpu,memory", [(None, None), (1024, None), (None, 2048), (2048, 4096)]
)
async def test_cpu_and_memory(
    aws_credentials: AwsCredentials,
    launch_type: str,
    flow_run: FlowRun,
    cpu: int,
    memory: int,
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials, launch_type=launch_type, cpu=cpu, memory=memory
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)
    task = describe_task(ecs_client, task_arn)
    task_definition = describe_task_definition(ecs_client, task)
    container_definition = _get_container(
        task_definition["containerDefinitions"], ECS_DEFAULT_CONTAINER_NAME
    )
    overrides = task["overrides"]
    container_overrides = _get_container(
        overrides["containerOverrides"], ECS_DEFAULT_CONTAINER_NAME
    )

    if launch_type == "EC2":
        # EC2 requires CPU and memory to be defined at the container level
        assert container_definition["cpu"] == cpu or ECS_DEFAULT_CPU
        assert container_definition["memory"] == memory or ECS_DEFAULT_MEMORY
    else:
        # Fargate requires CPU and memory to be defined at the task definition level
        assert task_definition["cpu"] == str(cpu or ECS_DEFAULT_CPU)
        assert task_definition["memory"] == str(memory or ECS_DEFAULT_MEMORY)

    # We always provide non-null values as overrides on the task run
    assert overrides.get("cpu") == (str(cpu) if cpu else None)
    assert overrides.get("memory") == (str(memory) if memory else None)
    # And as overrides for the Prefect container
    assert container_overrides.get("cpu") == cpu
    assert container_overrides.get("memory") == memory


@pytest.mark.usefixtures("ecs_mocks")
@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE", "FARGATE_SPOT"])
async def test_network_mode_default(
    aws_credentials: AwsCredentials,
    launch_type: str,
    flow_run: FlowRun,
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials, launch_type=launch_type
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)

    task = describe_task(ecs_client, task_arn)
    task_definition = describe_task_definition(ecs_client, task)

    if launch_type == "EC2":
        assert task_definition["networkMode"] == "bridge"
    else:
        assert task_definition["networkMode"] == "awsvpc"


@pytest.mark.usefixtures("ecs_mocks")
@pytest.mark.parametrize("launch_type", ["EC2", "FARGATE", "FARGATE_SPOT"])
async def test_container_command(
    aws_credentials: AwsCredentials,
    launch_type: str,
    flow_run: FlowRun,
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials,
        launch_type=launch_type,
        command="prefect version",
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)

    task = describe_task(ecs_client, task_arn)

    container_overrides = _get_container(
        task["overrides"]["containerOverrides"], ECS_DEFAULT_CONTAINER_NAME
    )
    assert container_overrides["command"] == ["prefect", "version"]


@pytest.mark.usefixtures("ecs_mocks")
async def test_environment_variables(
    aws_credentials: AwsCredentials,
    flow_run: FlowRun,
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials,
        env={"FOO": "BAR"},
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)

    task = describe_task(ecs_client, task_arn)
    task_definition = describe_task_definition(ecs_client, task)
    prefect_container_definition = _get_container(
        task_definition["containerDefinitions"], ECS_DEFAULT_CONTAINER_NAME
    )
    assert not prefect_container_definition[
        "environment"
    ], "Variables should not be passed until runtime"

    prefect_container_overrides = _get_container(
        task["overrides"]["containerOverrides"], ECS_DEFAULT_CONTAINER_NAME
    )
    expected = [{"name": "FOO", "value": "BAR"}]
    assert prefect_container_overrides.get("environment") == expected


@pytest.mark.usefixtures("ecs_mocks")
async def test_labels_aka_tags(
    aws_credentials: AwsCredentials,
    flow_run: FlowRun,
):
    configuration = await construct_configuration(
        aws_credentials=aws_credentials,
        labels={"foo": "bar"},
    )

    session = aws_credentials.get_boto3_session()
    ecs_client = session.client("ecs")

    async with ECSWorker(work_pool_name="test") as worker:
        result = await run_then_stop_task(worker, configuration, flow_run)

    assert result.status_code == 0
    _, task_arn = parse_identifier(result.identifier)

    task = describe_task(ecs_client, task_arn)
    task_definition = describe_task_definition(ecs_client, task)
    assert not task_definition.get("tags"), "Labels should not be passed until runtime"

    assert task.get("tags") == [{"key": "foo", "value": "bar"}]
