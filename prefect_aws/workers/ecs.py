import copy
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import anyio
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
from pydantic import Field

from prefect_aws import AwsCredentials

# Internal type alias for ECS clients which are generated dynamically in botocore
_ECSClient = Any

ECS_DEFAULT_CONTAINER_NAME = "prefect"
ECS_DEFAULT_CPU = 1024
ECS_DEFAULT_MEMORY = 2048
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
cpu: '{{1024}}'
family: prefect
memory: '2048'
networkMode: awsvpc
requiresCompatibilities:
- FARGATE
"""

DEFAULT_TASK_RUN_REQUEST_TEMPLATE = """
launchType: FARGATE
overrides:
  containerOverrides:
    - name: "{{ container_name }}"
      command: "{{ command }}"
      environment: "{{ env }}"
tags: "{{ labels }}"
taskDefinition: "{{ task_definition_arn }}"
"""


_TASK_DEFINITION_CACHE: Dict[UUID, str] = {}


def _default_task_definition_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_DEFINITION_TEMPLATE)


def _default_task_run_request_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_RUN_REQUEST_TEMPLATE)


class ECSJobConfiguration(BaseJobConfiguration):
    aws_credentials: Optional[AwsCredentials] = Field(default=None)
    task_definition: dict = Field(template=_default_task_definition_template())
    task_run_request: dict = Field(template=_default_task_run_request_template())


class ECSVariables(BaseVariables):
    task_definition_arn: Optional[str] = Field(default=None)


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

        if cached_task_definition_arn:
            # Read the task definition to see if the cached task definition is valid
            task_definition = self._retrieve_task_definition(
                ecs_client, task_definition_arn
            )

            if not self._task_definitions_equal(
                task_definition, configuration.task_definition
            ):
                # Cached task definition is not valid
                cached_task_definition_arn = None

        if not cached_task_definition_arn:
            task_definition_arn = self._register_task_definition(
                ecs_client, configuration.task_definition
            )

        # Update the cached task definition ARN to avoid re-registering the task
        # definition on this worker unless necessary; registration is agressively
        # rate limited by AWS
        _TASK_DEFINITION_CACHE[flow_run.deployment_id] = task_definition_arn

        # Prepare the task run request
        task_run_request = deepcopy(configuration.task_run_request)
        task_run_request["taskDefinition"] = task_definition_arn

        task_run = self._create_task_run(ecs_client, configuration.task_run_request)
        return ECSWorkerResult(identifier=task_run)

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
        self.logger.info(
            f"{self._log_prefix}: Retrieving task definition {task_definition_arn!r}..."
        )
        response = ecs_client.describe_task_definition(
            taskDefinition=task_definition_arn
        )
        return response["taskDefinition"]

    def _create_task_run(self, ecs_client: _ECSClient, task_run_request: dict):
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
