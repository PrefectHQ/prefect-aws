import copy
import time
import warnings
from typing import Dict, List, Literal, Optional, Union

import yaml
from prefect.docker import get_prefect_image_name
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import sync_compatible
from pydantic import Field, root_validator

from prefect_aws import AwsCredentials


class ECSTaskResult(InfrastructureResult):
    """The result of a run of an ECS task"""


ECS_TASK_RUN_CONTAINER_NAME = "prefect"


class ECSTask(Infrastructure):
    """
    Run a command as an ECS task
    """

    type: Literal["ecs-task"] = "ecs-task"

    aws_credentials: AwsCredentials = Field(default_factory=AwsCredentials)

    # Task definition settings
    task_definition_arn: Optional[str] = None
    task_definition: Optional[dict] = None
    image: str = Field(default_factory=get_prefect_image_name)
    family_prefix: str = "prefect"

    # Mixed task definition / run settings
    cpu: Union[int, str] = None
    memory: Union[int, str] = None

    # Task run settings
    launch_type: Optional[Literal["FARGATE", "EC2", "EXTERNAL"]] = None
    vpc_id: Optional[str] = None
    cluster: Optional[str] = None
    env: Dict[str, str] = Field(default_factory=dict)
    task_role_arn: str = None
    execution_role_arn: str = None
    capacity_provider_strategy: List[dict] = Field(default_factory=list)

    @root_validator
    def set_default_launch_type(cls, values):
        """
        To use FARGATE_SPOT, the launch type must be left empty. If a launch type is
        not provided and the user isn't using FARGATE_SPOT, the launch type defaults
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

    @sync_compatible
    async def run(self):
        """
        Run the configured task on ECS.
        """
        ecs_client = self._get_ecs_client()

        requested_task_definition = (
            self._retrieve_task_definition(ecs_client, self.task_definition_arn)
            if self.task_definition_arn
            else self.task_definition
        ) or {}
        task_definition_arn = requested_task_definition.get("taskDefinitionArn", None)

        task_definition = self._prepare_task_definition(requested_task_definition)

        # We must register the task definition if the arn is null or changes were made
        if task_definition != requested_task_definition or not task_definition_arn:
            self.logger.info(f"ECSTask {self.name!r}: Registering task definition...")
            self.logger.debug("\n" + yaml.dump(task_definition))
            task_definition_arn = self._register_task_definition(
                ecs_client, task_definition
            )

        if task_definition.get("networkMode") == "awsvpc":
            network_config = self._load_vpc_network_config(self.vpc_id)
        else:
            network_config = None

        task_run = self._prepare_task_run(network_config, task_definition_arn)
        self.logger.info(f"ECSTask {self.name!r}: Creating ECS task run...")
        self.logger.debug("\n" + yaml.dump(task_run))

        try:
            task_arn = ecs_client.run_task(**task_run)["tasks"][0]["taskArn"]
        except Exception as exc:
            self._report_task_run_creation_failure(task_run, exc)

        task = self._watch_task(task_arn, ecs_client)

        return ECSTaskResult(
            identifier=task_arn,
            status_code=self._get_prefect_container(task["containers"])["exitCode"],
        )

    def preview(self) -> str:
        """
        Generate a preview of the task definition and task run that will be sent to AWS.
        """
        preview = ""

        task_definition_arn = self.task_definition_arn or "<registered at runtime>"

        if self.task_definition or not self.task_definition_arn:
            task_definition = self._prepare_task_definition(self.task_definition or {})
            preview += "---\n# Task definition\n"
            preview += yaml.dump(task_definition)
            preview += "\n"
        else:
            task_definition = None

        if task_definition and task_definition.get("networkMode") == "awsvpc":
            vpc = "the default VPC" if not self.vpc_id else self.vpc_id
            network_config = {"awsvpcConfiguration": f"<loaded from {vpc} at runtime>"}
        else:
            network_config = None

        task_run = self._prepare_task_run(network_config, task_definition_arn)
        preview += "---\n# Task run request\n"
        preview += yaml.dump(task_run)

        return preview

    def _report_task_run_creation_failure(self, task_run, exc: Exception) -> None:
        """
        Wrap common AWS task run creation failures with nicer user-facing messages.
        """
        # AWS generates exception types at runtime so they must be captured a bit
        # differently than normal.
        if "ClusterNotFoundException" in str(exc):
            cluster = task_run.get("cluster", "default")
            raise RuntimeError(
                f"Failed to run ECS task, cluster {cluster!r} not found. "
                "Confirm that the cluster is configured in your region."
            ) from exc
        else:
            raise

    def _watch_task(self, task_arn: str, ecs_client, poll_interval: int = 5):
        """
        Watch a task until it reaches a STOPPED status.

        Returns a description of the task on completion.
        """
        last_status = status = "UNKNOWN"

        while status != "STOPPED":
            task = ecs_client.describe_tasks(tasks=[task_arn])["tasks"][0]

            status = task["lastStatus"]
            if status != last_status:
                self.logger.info(
                    f"ECSTask {self.name!r}: Entered new state {status!r}."
                )

            last_status = status
            time.sleep(poll_interval)

        return task

    def _get_ecs_client(self):
        """
        Get an AWS ECS client
        """
        return self.aws_credentials.get_boto3_session().client("ecs")

    def _retrieve_task_definition(self, ecs_client, task_definition_arn: str):
        """
        Retrieve an existing task definition from AWS.
        """
        self.logger.info(
            f"ECSTask {self.name!r}: "
            "Retrieving task definition {task_definition_arn!r}..."
        )
        response = ecs_client.describe_task_definition(
            taskDefinition=task_definition_arn
        )
        return response["taskDefinition"]

    def _register_task_definition(self, ecs_client, task_definition: dict) -> str:
        """
        Register a new task definition with AWS.
        """
        # TODO: Consider including a global cache for this task definition since
        #       registration of task definitions is frequently rate limited
        response = ecs_client.register_task_definition(**task_definition)
        return response["taskDefinition"]["taskDefinitionArn"]

    def _get_prefect_container(self, containers: List[dict]) -> Optional[dict]:
        """
        Extract the Prefect container from a list of containers or container definitions
        If not found, `None` is returned.
        """
        for container in containers:
            if container.get("name") == ECS_TASK_RUN_CONTAINER_NAME:
                return container
        return None

    def _prepare_task_definition(self, task_definition: dict) -> dict:
        """
        Prepare a task definition by inferring any defaults and merging overrides.
        """
        task_definition = copy.deepcopy(task_definition)

        # Configure the Prefect runtime container
        task_definition.setdefault(
            "containerDefinitions", [{"name": ECS_TASK_RUN_CONTAINER_NAME}]
        )
        container = self._get_prefect_container(task_definition["containerDefinitions"])
        container["image"] = self.image

        task_definition.setdefault("family", "prefect")

        # CPU and memory are required in some cases, retrieve the value to use
        cpu = self.cpu or task_definition.get("cpu") or 1024
        memory = self.memory or task_definition.get("memory") or 2048

        if self.launch_type == "FARGATE":
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
                    "launch type 'FARGATE'. Either use the 'EC2' launch type or the "
                    "'awsvpc' network mode."
                )

        elif self.launch_type == "EC2":
            # Container level memory and cpu are required when using ec2
            container.setdefault("cpu", int(cpu))
            container.setdefault("memory", int(memory))

        return task_definition

    def _prepare_task_run_overrides(self) -> dict:
        """
        Prepare the 'overrides' payload for a task run request.
        """
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

    def _load_vpc_network_config(self, vpc_id: Optional[str]) -> dict:
        """
        Load settings from a specific VPC or the default VPC and generate a task
        run request's network configuration.
        """
        ec2_client = self.aws_credentials.get_boto3_session().client("ec2")
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
            }
        }

    def _prepare_task_run(
        self, network_config: Optional[dict], task_definition_arn: str
    ) -> dict:
        """
        Prepare a task run request payload.
        """
        task_run = {
            "overrides": self._prepare_task_run_overrides(),
            "capacityProviderStrategy": self.capacity_provider_strategy,
            "tags": [
                {"name": key, "value": value} for key, value in self.labels.items()
            ],
            "taskDefinition": task_definition_arn,
        }

        if self.launch_type:
            task_run["launchType"] = self.launch_type

        if network_config:
            task_run["networkConfiguration"] = network_config

        return task_run
