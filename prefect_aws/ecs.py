import copy
import warnings
from typing import Dict, List, Literal, Optional, Tuple, Union

import yaml
from prefect.docker import get_prefect_image_name
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from pydantic import BaseModel, Field, root_validator

from prefect_aws import AwsCredentials


class ECSTaskResult(InfrastructureResult):
    """The result of a run of an ECS task"""


ECS_TASK_RUN_CONTAINER_NAME = "prefect"


class ECSTask(Infrastructure):
    """ """

    type: Literal["ecs-task"] = "ecs-task"

    aws_credentials: AwsCredentials = Field(default_factory=AwsCredentials)

    # Task definition settings
    task_definition_arn: Optional[str] = None
    task_definition: Optional[dict] = None
    image: str = Field(default_factory=get_prefect_image_name)

    # TODO: Determine if these should be included
    # task_family: Optional[str] = None
    # network_mode: Optional[str] = None

    # Task run settings
    launch_type: Optional[Literal["FARGATE", "EC2", "EXTERNAL"]] = None
    cluster: Optional[str] = None
    env: Dict[str, str] = Field(default_factory=dict)
    cpu: Union[int, str] = None
    memory: Union[int, str] = None
    task_role_arn: str = None
    execution_role_arn: str = None
    capacity_provider_strategy: List[dict] = Field(default_factory=list)

    @root_validator
    def set_default_launch_type(cls, values):
        """
        To use FARGATE_SPOT, the launch type must be left empty. If a launch type is
        not providing and the user isn't using FARGATE_SPOT, the launch type defaults
        to FARGATE.
        """
        strategies = values.get("capacity_provider_strategy")
        launch_type = values.get("launch_type")
        using_fargate_spot = False

        if strategies:
            providers = {strategy.get("capacityProvider") for strategy in strategies}
            using_fargate_spot = "FARGATE_SPOT" in providers

        FARGATE_SPOT_WARNING = (
            "'FARGATE_SPOT' capacity provider found in 'capacity_provider_strategy'"
            "but 'launch_type' is set to {launch_type!r}. "
            "Set the launch type to `None` to allow 'FARGATE_SPOT' to be used."
        )
        if using_fargate_spot and launch_type:
            warnings.warn(FARGATE_SPOT_WARNING.format(launch_type), stacklevel=3)

        if not launch_type and not using_fargate_spot:
            values["launch_type"] = "FARGATE"

        return values

    # @root_validator
    # def set_default_network_mode(cls, values):
    #     """
    #     If a task definition arn is not linked or the network mode is not set in the
    #     given task definition the network mode should default to 'awsvpc'
    #     """
    #     if not values.get("task_definition_arn") and not values.get(
    #         "task_definition", {}
    #     ).get("networkMode"):
    #         values.setdefault("network_mode", "awsvpc")
    #     return values

    # @root_validator
    # def set_default_task_family(cls, values):
    #     """
    #     If a task definition arn is not linked or the family is not set in the
    #     given task definition the family should default to 'prefect'
    #     """
    #     if not values.get("task_definition_arn") and not values.get(
    #         "task_definition", {}
    #     ).get("family"):
    #         values.setdefault("family", "prefect")
    #     return values

    async def run(self):
        ecs_client = self.get_ecs_client()

        task_definition = (
            self.retrieve_task_definition(ecs_client, self.task_definition_arn)
            if self.task_definition_arn
            else self.task_definition
        ) or {}
        task_definition_arn = task_definition.get("taskDefinitionArn", None)

        prepared_task_definition = self.prepare_task_definition(task_definition)

        # We must register the task definition if the arn is null or changes were made
        if prepared_task_definition != task_definition or not task_definition_arn:
            task_definition_arn = self.register_task_definition(
                ecs_client, prepared_task_definition
            )

        task_run = self.prepare_task_run(prepared_task_definition, task_definition_arn)
        return ecs_client.run_task(**task_run)

    def preview(self) -> str:
        preview = ""

        task_definition_arn = self.task_definition_arn or "<registered-at-runtime>"

        if self.task_definition or not self.task_definition_arn:
            task_definition = self.prepare_task_definition(self.task_definition or {})
            preview += "----- Task definition -----\n"
            preview += yaml.dump(task_definition)
            preview += "\n"

        task_run = self.prepare_task_run(task_definition or {}, task_definition_arn)
        preview += "----- Task run -----\n"
        preview += yaml.dump(task_run)

        return preview

    def get_ecs_client(self):
        return self.aws_credentials.get_boto3_session().client("ecs")

    def retrieve_task_definition(self, ecs_client, task_definition_arn: str):
        response = ecs_client.describe_task_definition(
            taskDefinition=task_definition_arn
        )
        return response["taskDefinition"]

    def register_task_definition(self, ecs_client, task_definition: dict) -> str:
        # TODO: Consider including a global cache for this task definition since
        #       registration of task definitions is frequently rate limited
        response = ecs_client.register_task_definition(**task_definition)
        return response["taskDefinition"]["taskDefinitionArn"]

    def get_prefect_container_definition(
        self, container_definitions: List[dict]
    ) -> Optional[dict]:
        for container in container_definitions:
            if container.get("name") == ECS_TASK_RUN_CONTAINER_NAME:
                return container
        return None

    def prepare_task_definition(self, task_definition: dict) -> dict:
        # Prepare the task definition by inferring any defaults and merging overrides
        task_definition = copy.deepcopy(task_definition)

        # Configure the Prefect runtime container
        task_definition.setdefault(
            "containerDefinitions", [{"name": ECS_TASK_RUN_CONTAINER_NAME}]
        )
        container = self.get_prefect_container_definition(
            task_definition["containerDefinitions"]
        )
        container["image"] = self.image

        # task_definition.setdefault("networkMode", "awsvpc")
        task_definition.setdefault("family", "prefect")

        # CPU and memory are required in some cases, retrieve the value to use
        cpu = self.cpu or task_definition.get("cpu") or 1024
        memory = self.memory or task_definition.get("memory") or 2048

        if self.launch_type == "FARGATE":
            # Task level memory and cpu are required when using fargate
            task_definition["cpu"] = str(cpu)
            task_definition["memory"] = str(memory)
        elif self.launch_type == "EC2":
            # Container level memory and cpu are required when using ec2
            container.setdefault("cpu", int(cpu))
            container.setdefault("memory", int(memory))

        return task_definition

    def prepare_task_run_overrides(self) -> dict:
        overrides = {
            "containerOverrides": [
                {
                    "name": ECS_TASK_RUN_CONTAINER_NAME,
                    "environment": [
                        {"name": key, "value": value} for key, value in self.env.items()
                    ],
                }
            ],
        }

        if self.command:
            overrides["containerOverrides"][0]["command"] = self.command

        if self.execution_role_arn:
            overrides["executionRoleArn"] = self.execution_role_arn

        if self.task_role_arn:
            overrides["taskRoleArn"] = self.task_role_arn

        if self.cluster:
            overrides["cluster"] = self.cluster

        if self.memory:
            overrides["memory"] = str(self.memory)

        if self.cpu:
            overrides["cpu"] = str(self.cpu)

        return overrides

    def infer_network_configuration(self, network_mode: Optional[str]) -> dict:
        if network_mode == "awsvpc":
            # Retrieve the default VPC
            ec2_client = self.aws_credentials.get_boto3_session().client("ec2")
            vpcs = ec2_client.describe_vpcs(
                Filters=[{"Name": "isDefault", "Values": ["true"]}]
            )["Vpcs"]
            if vpcs:
                vpc_id = vpcs[0]["VpcId"]
                subnets = ec2_client.describe_subnets(
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
                )["Subnets"]
                if subnets:
                    config = {
                        "awsvpcConfiguration": {
                            "subnets": [s["SubnetId"] for s in subnets],
                            "assignPublicIp": "ENABLED",
                        }
                    }
                    self.logger.debug(
                        "ECSTask %r: Using networkConfiguration=%r", self.name, config
                    )
                    return config

        return {}

    def prepare_task_run(self, task_definition: dict, task_definition_arn: str) -> dict:
        task_run = {
            "overrides": self.prepare_task_run_overrides(),
            "capacityProviderStrategy": self.capacity_provider_strategy,
            "networkConfiguration": self.infer_network_configuration(
                task_definition.get("networkMode")
            ),
            "tags": [
                {"name": key, "value": value} for key, value in self.labels.items()
            ],
            "taskDefinition": task_definition_arn,
        }

        if self.launch_type:
            task_run["launchType"] = self.launch_type

        return task_run
