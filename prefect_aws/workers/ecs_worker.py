import copy
import json
import shlex
import warnings
from copy import deepcopy
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Tuple
from uuid import UUID

import anyio
import anyio.abc
import boto3
import yaml
from prefect.server.schemas.core import FlowRun
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from pydantic import Field, root_validator
from slugify import slugify

from prefect_aws import AwsCredentials

# Internal type alias for ECS clients which are generated dynamically in botocore
_ECSClient = Any

ECS_DEFAULT_CONTAINER_NAME = "prefect"
ECS_DEFAULT_CPU = 1024
ECS_DEFAULT_MEMORY = 2048
ECS_DEFAULT_LAUNCH_TYPE = "FARGATE"
ECS_DEFAULT_FAMILY = "prefect"
ECS_POST_REGISTRATION_FIELDS = [
    "compatibilities",
    "taskDefinitionArn",
    "revision",
    "status",
    "requiresAttributes",
    "registeredAt",
    "registeredBy",
    "deregisteredAt",
]


DEFAULT_TASK_DEFINITION_TEMPLATE = """
containerDefinitions:
- image: "{{ image }}"
  name: "{{ container_name }}"
cpu: "{{ cpu }}"
family: "{{ family }}"
memory: "{{ memory }}"
"""

DEFAULT_TASK_RUN_REQUEST_TEMPLATE = """
launchType: "{{ launch_type }}"
overrides:
  containerOverrides:
    - name: "{{ container_name }}"
      command: "{{ command }}"      
      environment: "{{ env }}"
      cpu: "{{ cpu }}"
      memory: "{{ memory }}"
  cpu: "{{ cpu }}"
  memory: "{{ memory }}"
tags: "{{ labels }}"
taskDefinition: "{{ task_definition_arn }}"
"""


_TASK_DEFINITION_CACHE: Dict[UUID, str] = {}


class ECSIdentifier(NamedTuple):
    cluster: str
    task_arn: str


def _default_task_definition_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_DEFINITION_TEMPLATE)


def _default_task_run_request_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_RUN_REQUEST_TEMPLATE)


def _get_container(containers: List[dict], name: str) -> Optional[dict]:
    """
    Extract a container from a list of containers or container definitions.
    If not found, `None` is returned.
    """
    for container in containers:
        if container.get("name") == name:
            return container
    return None


def _container_name_from_task_definition(task_definition: dict) -> Optional[str]:
    # Attempt to infer the container name from the task definition
    if task_definition:
        container_definitions = task_definition.get("containerDefinitions", [])
    else:
        container_definitions = []

    if _get_container(container_definitions, ECS_DEFAULT_CONTAINER_NAME):
        # Use the default container name if present
        return ECS_DEFAULT_CONTAINER_NAME
    elif container_definitions:
        # Otherwise, if there's at least one container definition try to get the
        # name from that
        return container_definitions[0].get("name")

    return None


def parse_identifier(identifier: str) -> ECSIdentifier:
    """
    Splits identifier into its cluster and task components, e.g.
    input "cluster_name::task_arn" outputs ("cluster_name", "task_arn").
    """
    cluster, task = identifier.split("::", maxsplit=1)
    return ECSIdentifier(cluster, task)


class ECSJobConfiguration(BaseJobConfiguration):
    aws_credentials: Optional[AwsCredentials] = Field(default=None)
    task_definition: Optional[Dict[str, Any]] = Field(
        template=_default_task_definition_template()
    )
    task_run_request: Dict[str, Any] = Field(
        template=_default_task_run_request_template()
    )
    configure_cloudwatch_logs: bool = Field(
        default=None,
        description=(
            "If `True`, the Prefect container will be configured to send its output "
            "to the AWS CloudWatch logs service. This functionality requires an "
            "execution role with logs:CreateLogStream, logs:CreateLogGroup, and "
            "logs:PutLogEvents permissions. The default for this field is `False` "
            "unless `stream_output` is set."
        ),
    )
    cloudwatch_logs_options: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "When `configure_cloudwatch_logs` is enabled, this setting may be used to "
            "pass additional options to the CloudWatch logs configuration or override "
            "the default options. See the AWS documentation for available options. "
            "https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html#create_awslogs_logdriver_options."  # noqa
        ),
    )
    stream_output: bool = Field(
        default=None,
        description=(
            "If `True`, logs will be streamed from the Prefect container to the local "
            "console. Unless you have configured AWS CloudWatch logs manually on your "
            "task definition, this requires the same prerequisites outlined in "
            "`configure_cloudwatch_logs`."
        ),
    )

    vpc_id: Optional[str] = Field(
        title="VPC ID",
        default=None,
        description=(
            "The AWS VPC to link the task run to. This is only applicable when using "
            "the 'awsvpc' network mode for your task. FARGATE tasks require this "
            "network  mode, but for EC2 tasks the default network mode is 'bridge'. "
            "If using the 'awsvpc' network mode and this field is null, your default "
            "VPC will be used. If no default VPC can be found, the task run will fail."
        ),
    )
    cloudwatch_logs_options: dict = Field(default_factory=dict)
    container_name: Optional[str] = Field(default=None, template="{{ container_name }}")
    cluster: Optional[str] = Field(default=None, template="{{ cluster }}")

    @root_validator
    def task_run_request_requires_arn_if_no_task_definition_given(cls, values):
        if not values.get("task_run_request", {}).get(
            "taskDefinition"
        ) and not values.get("task_definition"):
            raise ValueError(
                "A task definition must be provided if a task definition ARN is not present on the task run request."
            )
        return values

    @root_validator
    def container_name_default_from_task_definition(cls, values):
        if values.get("container_name") is None:
            values["container_name"] = _container_name_from_task_definition(
                values.get("task_definition")
            )

            # We may not have a name here still; for example if someone is using a task
            # definition arn. In that case, we'll perform similar logic later to find
            # the name to treat as the "orchestration" container.

        return values

    @root_validator(pre=True)
    def set_default_configure_cloudwatch_logs(cls, values: dict) -> dict:
        """
        Streaming output generally requires CloudWatch logs to be configured.

        To avoid entangled arguments in the simple case, `configure_cloudwatch_logs`
        defaults to matching the value of `stream_output`.
        """
        configure_cloudwatch_logs = values.get("configure_cloudwatch_logs")
        if configure_cloudwatch_logs is None:
            values["configure_cloudwatch_logs"] = values.get("stream_output")
        return values

    @root_validator
    def configure_cloudwatch_logs_requires_execution_role_arn(
        cls, values: dict
    ) -> dict:
        """
        Enforces that an execution role arn is provided (or could be provided by a
        runtime task definition) when configuring logging.
        """
        if (
            values.get("configure_cloudwatch_logs")
            and not values.get("execution_role_arn")
            # TODO: Does not match
            # Do not raise if they've linked to another task definition or provided
            # it without using our shortcuts
            and not values.get("task_definition_arn")
            and not (values.get("task_definition") or {}).get("executionRoleArn")
        ):
            raise ValueError(
                "An `execution_role_arn` must be provided to use "
                "`configure_cloudwatch_logs` or `stream_logs`."
            )
        return values

    @root_validator
    def cloudwatch_logs_options_requires_configure_cloudwatch_logs(
        cls, values: dict
    ) -> dict:
        """
        Enforces that an execution role arn is provided (or could be provided by a
        runtime task definition) when configuring logging.
        """
        if values.get("cloudwatch_logs_options") and not values.get(
            "configure_cloudwatch_logs"
        ):
            raise ValueError(
                "`configure_cloudwatch_log` must be enabled to use "
                "`cloudwatch_logs_options`."
            )
        return values


class ECSVariables(BaseVariables):
    task_definition_arn: Optional[str] = Field(default=None)
    aws_credentials: AwsCredentials = Field(
        title="AWS Credentials",
        default_factory=AwsCredentials,
        description=(
            "The AWS credentials to use to connect to ECS. If not provided, credentials"
            " will be inferred from the local environment following AWS's boto client's"
            " rules."
        ),
    )
    cluster: Optional[str] = Field(
        default=None,
        description=(
            "The ECS cluster to run the task in. The ARN or name may be provided. If "
            "not provided, the default cluster will be used."
        ),
    )
    family: Optional[str] = Field(
        default=None,
        description=(
            "A family for the task definition. If not provided, it will be inferred "
            "from the task definition. If the task definition does not have a family, "
            "the name will be generated. When flow and deployment metadata is "
            "available, the generated name will include their names. Values for this "
            "field will be slugified to match AWS character requirements."
        ),
    )
    launch_type: Optional[
        Literal["FARGATE", "EC2", "EXTERNAL", "FARGATE_SPOT"]
    ] = Field(
        default=ECS_DEFAULT_LAUNCH_TYPE,
        description=(
            "The type of ECS task run infrastructure that should be used. Note that "
            "'FARGATE_SPOT' is not a formal ECS launch type, but we will configure the "
            "proper capacity provider stategy if set here."
        ),
    )
    image: Optional[str] = Field(
        default=None,
        description=(
            "The image to use for the Prefect container in the task. If this value is "
            "not null, it will override the value in the task definition. This value "
            "defaults to a Prefect base image matching your local versions."
        ),
    )
    cpu: int = Field(
        title="CPU",
        default=None,
        description=(
            "The amount of CPU to provide to the ECS task. Valid amounts are "
            "specified in the AWS documentation. If not provided, a default value of "
            f"{ECS_DEFAULT_CPU} will be used unless present on the task definition."
        ),
    )
    memory: int = Field(
        default=None,
        description=(
            "The amount of memory to provide to the ECS task. Valid amounts are "
            "specified in the AWS documentation. If not provided, a default value of "
            f"{ECS_DEFAULT_MEMORY} will be used unless present on the task definition."
        ),
    )
    container_name: str = Field(
        default=None,
        description=(
            "The name of the container flow run orchestration will occur in. If not "
            f"specified, a default value of {ECS_DEFAULT_CONTAINER_NAME} will be used "
            "and if that is not found in the task definition the first container will "
            "be used."
        ),
    )


class ECSWorkerResult(BaseWorkerResult):
    pass


class ECSWorker(BaseWorker):
    type = "ecs"
    job_configuration = ECSJobConfiguration

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: ECSJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> BaseWorkerResult:
        """
        Runs a given flow run on the current worker.
        """
        session, ecs_client = await run_sync_in_worker_thread(
            self._get_session_and_client, configuration
        )

        cached_task_definition_arn = _TASK_DEFINITION_CACHE.get(flow_run.deployment_id)

        # TODO: Pull the task definition from the ARN in the task run request if present
        task_definition = self._prepare_task_definition(
            configuration, region=ecs_client.meta.region_name
        )

        if cached_task_definition_arn:
            # Read the task definition to see if the cached task definition is valid
            try:
                task_definition = self._retrieve_task_definition(
                    ecs_client, cached_task_definition_arn
                )
            except Exception as exc:
                print(
                    f"Failed to retrieve cached task definition {cached_task_definition_arn!r}: {exc!r}"
                )
                # Clear from cache
                _TASK_DEFINITION_CACHE.pop(cached_task_definition_arn, None)
                cached_task_definition_arn = None
            else:
                if not self._task_definitions_equal(task_definition, task_definition):
                    # Cached task definition is not valid
                    cached_task_definition_arn = None

        if not cached_task_definition_arn:
            print(
                f"Registering task definition {json.dumps(task_definition, indent=2)}"
            )
            task_definition_arn = self._register_task_definition(
                ecs_client, task_definition
            )
        else:
            task_definition_arn = cached_task_definition_arn

        launch_type = configuration.task_run_request.get(
            "launchType", ECS_DEFAULT_LAUNCH_TYPE
        )
        if (
            launch_type != "EC2"
            and "FARGATE" not in task_definition["requiresCompatibilities"]
        ):
            raise RuntimeError(
                f"Task definition does not have 'FARGATE' in 'requiresCompatibilities' and cannot be used with launch type {launch_type!r}"
            )

        # Update the cached task definition ARN to avoid re-registering the task
        # definition on this worker unless necessary; registration is agressively
        # rate limited by AWS
        _TASK_DEFINITION_CACHE[flow_run.deployment_id] = task_definition_arn

        # Prepare the task run request
        task_run_request = self._prepare_task_run_request(
            session,
            configuration,
            task_definition,
            task_definition_arn,
        )

        print(
            f"Creating task run with request {json.dumps(task_run_request, indent=2)}"
        )
        task = self._create_task_run(ecs_client, task_run_request)

        try:
            task_arn = task["taskArn"]
            cluster_arn = task["clusterArn"]
        except Exception as exc:
            # self._report_task_run_creation_failure(task_run_request, exc)
            raise

        # The task identifier is "{cluster}::{task}" where we use the configured cluster
        # if set to preserve matching by name rather than arn
        # Note "::" is used despite the Prefect standard being ":" because ARNs contain
        # single colons.
        identifier = (
            (configuration.cluster if configuration.cluster else cluster_arn)
            + "::"
            + task_arn
        )

        if task_status:
            task_status.started(identifier)

        return ECSWorkerResult(identifier=identifier, status_code=0)

    def _get_session_and_client(
        self,
        configuration: ECSJobConfiguration,
    ) -> Tuple[boto3.Session, _ECSClient]:
        """
        Retrieve a boto3 session and ECS client
        """
        boto_session = configuration.aws_credentials.get_boto3_session()
        ecs_client = boto_session.client("ecs")
        return boto_session, ecs_client

    def _register_task_definition(
        self, ecs_client: _ECSClient, task_definition: dict
    ) -> str:
        """
        Register a new task definition with AWS.

        Returns the ARN.
        """
        response = ecs_client.register_task_definition(**task_definition)
        return response["taskDefinition"]["taskDefinitionArn"]

    def _retrieve_task_definition(
        self, ecs_client: _ECSClient, task_definition_arn: str
    ):
        """
        Retrieve an existing task definition from AWS.
        """
        print(f"Retrieving task definition {task_definition_arn!r}...")
        response = ecs_client.describe_task_definition(
            taskDefinition=task_definition_arn
        )
        return response["taskDefinition"]

    def _prepare_task_definition(
        self,
        configuration: ECSJobConfiguration,
        region: str,
    ) -> dict:
        """
        Prepare a task definition by inferring any defaults and merging overrides.
        """
        task_definition = copy.deepcopy(configuration.task_definition)

        # Configure the Prefect runtime container
        task_definition.setdefault("containerDefinitions", [])
        container_name = configuration.container_name
        if not container_name:
            container_name = (
                _container_name_from_task_definition(task_definition)
                or ECS_DEFAULT_CONTAINER_NAME
            )

        container = _get_container(
            task_definition["containerDefinitions"], container_name
        )
        if container is None:
            container = {"name": container_name}
            task_definition["containerDefinitions"].append(container)

        # Remove any keys that have been explicitly "unset"
        unset_keys = {key for key, value in configuration.env.items() if value is None}
        for item in tuple(container.get("environment", [])):
            if item["name"] in unset_keys:
                container["environment"].remove(item)

        if configuration.configure_cloudwatch_logs:
            container["logConfiguration"] = {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-create-group": "true",
                    "awslogs-group": "prefect",
                    "awslogs-region": region,
                    "awslogs-stream-prefix": configuration.name or "prefect",
                    **configuration.cloudwatch_logs_options,
                },
            }

        family = task_definition.get("family") or ECS_DEFAULT_FAMILY
        task_definition["family"] = slugify(
            family,
            max_length=255,
            regex_pattern=r"[^a-zA-Z0-9-_]+",
        )

        # CPU and memory are required in some cases, retrieve the value to use
        cpu = task_definition.get("cpu") or ECS_DEFAULT_CPU
        memory = task_definition.get("memory") or ECS_DEFAULT_MEMORY

        launch_type = configuration.task_run_request.get(
            "launchType", ECS_DEFAULT_LAUNCH_TYPE
        )

        if launch_type == "FARGATE" or launch_type == "FARGATE_SPOT":
            # Task level memory and cpu are required when using fargate
            task_definition["cpu"] = str(cpu)
            task_definition["memory"] = str(memory)

            # The FARGATE compatibility is required if it will be used as as launch type
            requires_compatibilities = task_definition.setdefault(
                "requiresCompatibilities", []
            )
            if "FARGATE" not in requires_compatibilities:
                task_definition["requiresCompatibilities"].append("FARGATE")

            # Only the 'awsvpc' network mode is supported when using FARGATE
            # However, we will not enforce that here if the user has set it
            network_mode = task_definition.setdefault("networkMode", "awsvpc")

            if network_mode != "awsvpc":
                warnings.warn(
                    f"Found network mode {network_mode!r} which is not compatible with "
                    f"launch type {launch_type!r}. Use either the 'EC2' launch "
                    "type or the 'awsvpc' network mode."
                )

        elif launch_type == "EC2":
            # Container level memory and cpu are required when using ec2
            container.setdefault("cpu", cpu)
            container.setdefault("memory", memory)

            # Ensure set values are cast to integers
            container["cpu"] = int(container["cpu"])
            container["memory"] = int(container["memory"])

        # Ensure set values are cast to strings
        if task_definition.get("cpu"):
            task_definition["cpu"] = str(task_definition["cpu"])
        if task_definition.get("memory"):
            task_definition["memory"] = str(task_definition["memory"])

        if configuration.configure_cloudwatch_logs and not task_definition.get(
            "executionRoleArn"
        ):
            raise ValueError(
                "An execution role arn must be set on the task definition to use "
                "`configure_cloudwatch_logs` or `stream_logs` but no execution role "
                "was found on the task definition."
            )

        return task_definition

    def _load_vpc_network_config(
        self, vpc_id: Optional[str], boto_session: boto3.Session
    ) -> dict:
        """
        Load settings from a specific VPC or the default VPC and generate a task
        run request's network configuration.
        """
        ec2_client = boto_session.client("ec2")
        vpc_message = "the default VPC" if not vpc_id else f"VPC with ID {vpc_id}"

        if not vpc_id:
            # Retrieve the default VPC
            describe = {"Filters": [{"Name": "isDefault", "Values": ["true"]}]}
        else:
            describe = {"VpcIds": [vpc_id]}

        vpcs = ec2_client.describe_vpcs(**describe)["Vpcs"]
        if not vpcs:
            help_message = (
                "Pass an explicit `vpc_id` or configure a default VPC."
                if not vpc_id
                else "Check that the VPC exists in the current region."
            )
            raise ValueError(
                f"Failed to find {vpc_message}. "
                "Network configuration cannot be inferred. " + help_message
            )

        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2_client.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )["Subnets"]
        if not subnets:
            raise ValueError(
                f"Failed to find subnets for {vpc_message}. "
                "Network configuration cannot be inferred."
            )

        return {
            "awsvpcConfiguration": {
                "subnets": [s["SubnetId"] for s in subnets],
                "assignPublicIp": "ENABLED",
                "securityGroups": [],
            }
        }

    def _prepare_task_run_request(
        self,
        boto_session: boto3.Session,
        configuration: ECSJobConfiguration,
        task_definition: dict,
        task_definition_arn: str,
    ) -> dict:
        """
        Prepare a task run request payload.
        """
        task_run_request = deepcopy(configuration.task_run_request)

        task_run_request.setdefault("taskDefinition", task_definition_arn)
        assert task_run_request["taskDefinition"] == task_definition_arn

        if task_run_request.get("launchType") == "FARGATE_SPOT":
            # Should not be provided at all for FARGATE SPOT
            task_run_request.pop("launchType", None)

        overrides = task_run_request.get("overrides", {})
        container_overrides = overrides.get("containerOverrides", [])

        # Ensure the network configuration is present if using awsvpc for network mode

        if task_definition.get("networkMode") == "awsvpc" and not task_run_request.get(
            "networkConfiguration"
        ):
            task_run_request["networkConfiguration"] = self._load_vpc_network_config(
                configuration.vpc_id, boto_session
            )

        # Ensure the container name is set if not provided at template time

        if container_overrides and not container_overrides[0].get("name"):
            container_overrides[0]["name"] = _container_name_from_task_definition(
                task_definition
            )

        # Clean up templated variable formatting

        for container in container_overrides:
            if isinstance(container.get("command"), str):
                container["command"] = shlex.split(container["command"])
            if isinstance(container.get("environment"), dict):
                container["environment"] = [
                    {"name": k, "value": v} for k, v in container["environment"].items()
                ]

        if isinstance(task_run_request.get("tags"), dict):
            task_run_request["tags"] = [
                {"key": k, "value": v} for k, v in task_run_request["tags"].items()
            ]

        if overrides.get("cpu"):
            overrides["cpu"] = str(overrides["cpu"])

        if overrides.get("memory"):
            overrides["memory"] = str(overrides["memory"])

        return task_run_request

    def _create_task_run(self, ecs_client: _ECSClient, task_run_request: dict) -> str:
        """
        Create a run of a task definition.

        Returns the task run ARN.
        """
        return ecs_client.run_task(**task_run_request)["tasks"][0]

    def _task_definitions_equal(self, taskdef_1, taskdef_2) -> bool:
        """
        Compare two task definitions.

        Since one may come from the AWS API and have populated defaults, we do our best
        to homogenize the definitions without changing their meaning.
        """
        if taskdef_1 == taskdef_2:
            return True

        if taskdef_1 is None or taskdef_2 is None:
            return False

        taskdef_1 = copy.deepcopy(taskdef_1)
        taskdef_2 = copy.deepcopy(taskdef_2)

        def _set_aws_defaults(taskdef):
            """Set defaults that AWS would set after registration"""
            container_definitions = taskdef.get("containerDefinitions", [])
            essential = any(
                container.get("essential") for container in container_definitions
            )
            if not essential:
                container_definitions[0].setdefault("essential", True)

            taskdef.setdefault("networkMode", "bridge")

        _set_aws_defaults(taskdef_1)
        _set_aws_defaults(taskdef_2)

        def _drop_empty_keys(dict_):
            """Recursively drop keys with 'empty' values"""
            for key, value in tuple(dict_.items()):
                if not value:
                    dict_.pop(key)
                if isinstance(value, dict):
                    _drop_empty_keys(value)
                if isinstance(value, list):
                    for v in value:
                        if isinstance(v, dict):
                            _drop_empty_keys(v)

        _drop_empty_keys(taskdef_1)
        _drop_empty_keys(taskdef_2)

        # Clear fields that change on registration for comparison
        for field in ECS_POST_REGISTRATION_FIELDS:
            taskdef_1.pop(field, None)
            taskdef_2.pop(field, None)

        return taskdef_1 == taskdef_2
