from copy import deepcopy
from typing import Any, Optional, Tuple

import anyio
import boto3
import yaml
from prefect.server.schemas.core import FlowRun
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect.workers.base import BaseJobConfiguration, BaseWorker, BaseWorkerResult
from pydantic import Field

from prefect_aws import AwsCredentials

# Internal type alias for ECS clients which are generated dynamically in botocore
_ECSClient = Any


DEFAULT_TASK_DEFINITION_TEMPLATE = """
containerDefinitions:
- image: "{{ image }}"
  name: "{{ container_name }}"
cpu: '1024'
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


def _default_task_definition_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_DEFINITION_TEMPLATE)


def _default_task_run_request_template() -> dict:
    return yaml.safe_load(DEFAULT_TASK_RUN_REQUEST_TEMPLATE)


class ECSJobConfiguration(BaseJobConfiguration):
    aws_credentials: Optional[AwsCredentials] = Field(default=None)
    task_definition: dict = Field(template=_default_task_definition_template())
    task_run_request: dict = Field(template=_default_task_run_request_template())


class ECSWorkerResult(BaseWorkerResult):
    pass


class ECSWorker(BaseWorker):
    type = "ecs"

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

        task_definition_arn = self._register_task_definition(
            ecs_client, configuration.task_definition
        )

        task_run_request = deepcopy.copy(configuration.task_run_request)
        task_run_request["taskDefinition"] = task_definition_arn

        task_run = self._run_task(ecs_client, configuration.task_run_request)
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

    def _create_task_run(self, ecs_client: _ECSClient, task_run_request: dict):
        """
        Create a run of a task definition.

        Returns the task run ARN.
        """
        return ecs_client.run_task(**task_run_request)["tasks"][0]
