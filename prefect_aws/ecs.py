"""
Examples:

    Run a task using ECS Fargate
    >>> ECSTask(command=["echo", "hello world"]).run()

    Run a task using ECS Fargate with a spot container instance
    >>> ECSTask(command=["echo", "hello world"], launch_type="FARGATE_SPOT").run()

    Run a task using ECS with an EC2 container instance
    >>> ECSTask(command=["echo", "hello world"], launch_type="EC2").run()

    Run a task on a specific VPC using ECS Fargate
    >>> ECSTask(command=["echo", "hello world"], vpc_id="vpc-01abcdf123456789a").run()

    Run a task and stream the container's output to the local terminal
    >>> ECSTask(command=["echo", "hello world"], stream_output=True)

    Run a task using an existing task definition as a base
    >>> ECSTask(command=["echo", "hello world"], task_definition_arn="arn:aws:ecs:...")

    Run a task with a specific image
    >>> ECSTask(command=["echo", "hello world"], image="alpine:latest")

    Run a task with custom memory and CPU requirements
    >>> ECSTask(command=["echo", "hello world"], memory=4096, cpu=2048)

    Run a task with custom environment variables
    >>> ECSTask(command=["echo", "hello $PLANET"], env={"PLANET": "earth"})

    Run a task in a specific ECS cluster
    >>> ECSTask(command=["echo", "hello world"], cluster="my-cluster-name")
"""
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
    execution_role_arn: str = None
    configure_cloudwatch_logs: bool = None
    stream_output: bool = False

    # Task run settings
    launch_type: Optional[
        Literal["FARGATE", "EC2", "EXTERNAL", "FARGATE_SPOT"]
    ] = "FARGATE"
    vpc_id: Optional[str] = None
    cluster: Optional[str] = None
    env: Dict[str, str] = Field(default_factory=dict)
    task_role_arn: str = None

    @root_validator(pre=True)
    def set_default_configure_cloudwatch_logs(cls, values):
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
    def configure_cloudwatch_logs_requires_execution_role_arn(cls, values):
        if values.get("configure_cloudwatch_logs") and not values.get(
            "execution_role_arn"
        ):
            raise ValueError(
                "An `execution_role_arn` must be provided to use "
                "`configure_cloudwatch_logs` or `stream_logs`."
            )
        return values

    @sync_compatible
    async def run(self):
        """
        Run the configured task on ECS.
        """
        boto_session = self.aws_credentials.get_boto3_session()
        ecs_client = boto_session.client("ecs")

        requested_task_definition = (
            self._retrieve_task_definition(ecs_client, self.task_definition_arn)
            if self.task_definition_arn
            else self.task_definition
        ) or {}
        task_definition_arn = requested_task_definition.get("taskDefinitionArn", None)

        task_definition = self._prepare_task_definition(
            requested_task_definition, region=ecs_client.meta.region_name
        )

        # We must register the task definition if the arn is null or changes were made
        if task_definition != requested_task_definition or not task_definition_arn:
            self.logger.info(f"ECSTask {self.name!r}: Registering task definition...")
            self.logger.debug("Task definition payload\n" + yaml.dump(task_definition))
            task_definition_arn = self._register_task_definition(
                ecs_client, task_definition
            )

        if task_definition.get("networkMode") == "awsvpc":
            network_config = self._load_vpc_network_config(self.vpc_id, boto_session)
        else:
            network_config = None

        task_run = self._prepare_task_run(
            network_config=network_config,
            task_definition_arn=task_definition_arn,
        )
        self.logger.info(f"ECSTask {self.name!r}: Creating ECS task run...")
        self.logger.debug("Task run payload\n" + yaml.dump(task_run))

        try:
            task_arn = ecs_client.run_task(**task_run)["tasks"][0]["taskArn"]
        except Exception as exc:
            self._report_task_run_creation_failure(task_run, exc)

        task = self._watch_task(task_arn, task_definition, ecs_client, boto_session)

        status_code = self._get_prefect_container(task["containers"]).get("exitCode")

        return ECSTaskResult(
            identifier=task_arn,
            # If the container does not start the exit code can be null but we must
            # still report a status code. We use a -1 to indicate a special code.
            status_code=status_code or -1,
        )

    def preview(self) -> str:
        """
        Generate a preview of the task definition and task run that will be sent to AWS.
        """
        preview = ""

        task_definition_arn = self.task_definition_arn or "<registered at runtime>"

        if self.task_definition or not self.task_definition_arn:
            task_definition = self._prepare_task_definition(
                self.task_definition or {},
                region=self.aws_credentials.region_name
                or "<loaded from client at runtime>",
            )
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

    def _report_container_status_code(self, status_code: Optional[int]) -> None:
        if status_code is None:
            self.logger.error(
                f"ECSTask {self.name!r}: Task exited without reporting a container "
                "exit status."
            )
        elif status_code == 0:
            self.logger.info(f"ECSTask {self.name!r}: Container exited successfully.")
        else:
            self.logger.warning(
                f"ECSTask {self.name!r}: Container exited with non-zero exit code "
                f"{status_code}."
            )

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
        elif "No Container Instances" in str(exc) and self.launch_type == "EC2":
            cluster = task_run.get("cluster", "default")
            raise RuntimeError(
                f"Failed to run ECS task, cluster {cluster!r} does not appear to "
                "have any container instances associated with it. Confirm that you "
                "have EC2 container instances available."
            ) from exc
        else:
            raise

    def _watch_task(
        self,
        task_arn: str,
        task_definition: dict,
        ecs_client,
        boto_session,
        poll_interval: int = 5,
    ):
        """
        Watch a task until it reaches a STOPPED status.

        Returns a description of the task on completion.
        """
        last_status = status = "UNKNOWN"

        if self.stream_output:
            log_driver = task_definition.get("logConfiguration", {}).get("logDriver")
            if not task_definition.get("logConfiguration"):
                self.logger.warning(
                    f"ECSTask {self.name!r}: Logging configuration not found on task. "
                    "Logs cannot be streamed."
                )
            elif not log_driver == "awslogs":
                self.logger.warning(
                    f"ECSTask {self.name!r}: Logging configuration uses unsupported "
                    " driver {log_driver!r}. Logs cannot be streamed."
                )
            else:
                log_config = task_definition["logConfiguration"]["options"]
                logs_client = boto_session.client("logs")

        while status != "STOPPED":
            task = ecs_client.describe_tasks(tasks=[task_arn])["tasks"][0]

            status = task["lastStatus"]
            if status != last_status:
                self.logger.info(
                    f"ECSTask {self.name!r}: Entered new state {status!r}."
                )

            last_status = status

            if self.stream_output and log_config:
                response = logs_client.get_log_events(
                    logGroupName=log_config["awslogs-group"],
                    logStreamName=log_config["awslogs-stream-prefix"],
                )
                log_events = response["events"]
                for log_event in log_events:
                    self.logger.info(log_event)

            time.sleep(poll_interval)

        return task

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

    def _prepare_task_definition(self, task_definition: dict, region: str) -> dict:
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

        if self.configure_cloudwatch_logs:
            container["logConfiguration"] = {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-create-group": "true",
                    "awslogs-group": "prefect",
                    "awslogs-region": region,
                    "awslogs-stream-prefix": self.name or "prefect",
                },
            }

        task_definition.setdefault("family", "prefect")

        # CPU and memory are required in some cases, retrieve the value to use
        cpu = self.cpu or task_definition.get("cpu") or 1024
        memory = self.memory or task_definition.get("memory") or 2048

        if self.launch_type == "FARGATE" or self.launch_type == "FARGATE_SPOT":
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

        if self.execution_role_arn and not self.task_definition_arn:
            task_definition["executionRoleArn"] = self.execution_role_arn

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

        prefect_container_overrides = overrides["containerOverrides"][0]

        if self.command:
            prefect_container_overrides["command"] = self.command

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

    def _load_vpc_network_config(self, vpc_id: Optional[str], boto_session) -> dict:
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
            }
        }

    def _prepare_task_run(
        self,
        network_config: Optional[dict],
        task_definition_arn: str,
    ) -> dict:
        """
        Prepare a task run request payload.
        """
        task_run = {
            "overrides": self._prepare_task_run_overrides(),
            "tags": [
                {"name": key, "value": value} for key, value in self.labels.items()
            ],
            "taskDefinition": task_definition_arn,
        }

        if self.launch_type:
            if self.launch_type == "FARGATE_SPOT":
                task_run["capacityProviderStrategy"] = [
                    {"capacityProvider": "FARGATE_SPOT", "weight": 1}
                ]
            else:
                task_run["launchType"] = self.launch_type

        if network_config:
            task_run["networkConfiguration"] = network_config

        return task_run
