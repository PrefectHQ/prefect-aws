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

    Run a task and stream the container's output to the local terminal. Note an
    execution role must be provided with permissions: logs:CreateLogStream,
    logs:CreateLogGroup, and logs:PutLogEvents.
    >>> ECSTask(
    >>>     command=["echo", "hello world"],
    >>>     stream_output=True,
    >>>     execution_role_arn="..."
    >>> )

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
import sys
import time
import warnings
from typing import Dict, Generator, List, Literal, Optional, Union

import yaml
from anyio.abc import TaskStatus
from prefect.docker import get_prefect_image_name
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field, root_validator

from prefect_aws import AwsCredentials


class ECSTaskResult(InfrastructureResult):
    """The result of a run of an ECS task"""


PREFECT_ECS_CONTAINER_NAME = "prefect"
ECS_DEFAULT_CPU = 1024
ECS_DEFAULT_MEMORY = 2048


def get_prefect_container(containers: List[dict]) -> Optional[dict]:
    """
    Extract the Prefect container from a list of containers or container definitions
    If not found, `None` is returned.
    """
    return get_container(containers, PREFECT_ECS_CONTAINER_NAME)


def get_container(containers: List[dict], name: str) -> Optional[dict]:
    """
    Extract the Prefect container from a list of containers or container definitions
    If not found, `None` is returned.
    """
    for container in containers:
        if container.get("name") == name:
            return container
    return None


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

    # Execution settings
    task_start_timeout_seconds: int = 30
    task_watch_poll_interval: float = 5.0

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
        """
        Enforces that an execution role arn is provided (or could be provided by a
        runtime task definition) when configuring logging.
        """
        if (
            values.get("configure_cloudwatch_logs")
            and not values.get("execution_role_arn")
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

    @sync_compatible
    async def run(self, task_status: Optional[TaskStatus] = None):
        """
        Run the configured task on ECS.
        """
        boto_session, ecs_client = await run_sync_in_worker_thread(
            self._get_session_and_client
        )

        task_arn, cluster_arn, task_definition = await run_sync_in_worker_thread(
            self._create_task_and_wait_for_start, boto_session, ecs_client
        )

        # Display a nice message indicating the command and image
        self.logger.info(
            f"{self.log_prefix}: Running command {' '.join(self.command)!r} "
            f"in container {PREFECT_ECS_CONTAINER_NAME!r} ({self.image})..."
        )

        if task_status:
            task_status.started(task_arn)

        return await run_sync_in_worker_thread(
            self._watch_task_and_get_result,
            task_arn,
            cluster_arn,
            task_definition,
            boto_session,
            ecs_client,
        )

    @property
    def log_prefix(self):
        """
        Internal property for generating a prefix for logs where `name` may be null
        """
        if self.name is not None:
            return f"ECSTask {self.name!r}"
        else:
            return "ECSTask"

    def _get_session_and_client(self):
        """
        Retrieve a boto3 session and ECS client
        """
        boto_session = self.aws_credentials.get_boto3_session()
        ecs_client = boto_session.client("ecs")
        return boto_session, ecs_client

    def _create_task_and_wait_for_start(self, boto_session, ecs_client):
        """
        Register the task definition, create the task run, and wait for it to start.
        """
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
            self.logger.info(f"{self.log_prefix}: Registering task definition...")
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
        self.logger.info(f"{self.log_prefix}: Creating task run...")
        self.logger.debug("Task run payload\n" + yaml.dump(task_run))

        try:
            task = self._run_task(ecs_client, task_run)
            task_arn = task["taskArn"]
            cluster_arn = task["clusterArn"]
        except Exception as exc:
            self._report_task_run_creation_failure(task_run, exc)

        # Raises an exception if the task does not start
        self.logger.info(f"{self.log_prefix}: Waiting for task run to start...")
        self._wait_for_task_start(
            task_arn, cluster_arn, ecs_client, timeout=self.task_start_timeout_seconds
        )

        return task_arn, cluster_arn, task_definition

    def _watch_task_and_get_result(
        self,
        task_arn: str,
        cluster_arn: str,
        task_definition: dict,
        boto_session,
        ecs_client,
    ):
        """
        Wait for the task run to complete and retrieve the exit code of the Prefect
        container.
        """

        # Wait for completion and stream logs
        task = self._wait_for_task_finish(
            task_arn, cluster_arn, task_definition, ecs_client, boto_session
        )

        # TODO: Consider deactivating the task definition

        # Check the status code of the Prefect container
        prefect_container = get_prefect_container(task["containers"])
        assert (
            prefect_container is not None
        ), f"'prefect' container missing from task: {task}"
        status_code = prefect_container.get("exitCode")
        self._report_container_status_code(PREFECT_ECS_CONTAINER_NAME, status_code)

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

    def _report_container_status_code(
        self, name: str, status_code: Optional[int]
    ) -> None:
        """
        Display a log for the given container status code.
        """
        if status_code is None:
            self.logger.error(
                f"{self.log_prefix}: Task exited without reporting an exit status "
                f"for container {name!r}."
            )
        elif status_code == 0:
            self.logger.info(
                f"{self.log_prefix}: Container {name!r} exited successfully."
            )
        else:
            self.logger.warning(
                f"{self.log_prefix}: Container {name!r} exited with non-zero exit code "
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
        elif (
            "failed to validate logger args" in str(exc)
            and "AccessDeniedException" in str(exc)
            and self.configure_cloudwatch_logs
        ):

            raise RuntimeError(
                f"Failed to run ECS task, the attached execution role does not appear "
                "to have sufficient permissions. Ensure that the execution role "
                f"{self.execution_role!r} has permissions logs:CreateLogStream, "
                "logs:CreateLogGroup, and logs:PutLogEvents."
            )
        else:
            raise

    def _watch_task_run(
        self,
        task_arn: str,
        cluster_arn: str,
        ecs_client,
        current_status: str = "UNKNOWN",
        until_status: str = None,
        timeout: int = None,
    ) -> Generator[None, None, dict]:
        """
        Watches an ECS task run by querying every `poll_interval` seconds. After each
        query, the retrieved task is yielded. This function returns when the task run
        reaches a STOPPED status or the provided `until_status`.

        Emits a log each time the status changes.
        """
        last_status = status = current_status
        t0 = time.time()
        while status != until_status:
            task = ecs_client.describe_tasks(tasks=[task_arn], cluster=cluster_arn)[
                "tasks"
            ][0]

            status = task["lastStatus"]
            if status != last_status:
                self.logger.info(f"{self.log_prefix}: Status is {status}.")

            yield task

            # No point in continuing if the status is final
            if status == "STOPPED":
                break

            last_status = status
            elapsed_time = time.time() - t0
            if timeout is not None and elapsed_time > timeout:
                raise TimeoutError(
                    f"Timed out after {elapsed_time}s while watching task for status "
                    "{until_status or 'STOPPED'}"
                )
            time.sleep(self.task_watch_poll_interval)

    def _wait_for_task_start(
        self, task_arn: str, cluster_arn: str, ecs_client, timeout: int
    ) -> dict:
        """
        Waits for an ECS task run to reach a RUNNING status.

        If a STOPPED status is reached instead, an exception is raised indicating the
        reason that the task run did not start.
        """
        for task in self._watch_task_run(
            task_arn, cluster_arn, ecs_client, until_status="RUNNING", timeout=timeout
        ):
            # TODO: It is possible that the task has passed _through_ a RUNNING
            #       status during the polling interval. In this case, there is not an
            #       exception to raise.
            if task["lastStatus"] == "STOPPED":
                code = task.get("stopCode")
                reason = task.get("stoppedReason")
                # Generate a dynamic exception type from the AWS name
                raise type(code, (RuntimeError,), {})(reason)

        return task

    def _wait_for_task_finish(
        self,
        task_arn: str,
        cluster_arn: str,
        task_definition: dict,
        ecs_client,
        boto_session,
    ):
        """
        Watch an ECS task until it reaches a STOPPED status.

        If configured, logs from the Prefect container are streamed to stderr.

        Returns a description of the task on completion.
        """
        can_stream_output = False

        if self.stream_output:
            container_def = get_prefect_container(
                task_definition["containerDefinitions"]
            )
            if not container_def:
                self.logger.warning(
                    f"{self.log_prefix}: Prefect container definition not found in "
                    "task definition. Output cannot be streamed."
                )
            elif not container_def.get("logConfiguration"):
                self.logger.warning(
                    f"{self.log_prefix}: Logging configuration not found on task. "
                    "Output cannot be streamed."
                )
            elif not container_def["logConfiguration"].get("logDriver") == "awslogs":
                self.logger.warning(
                    f"{self.log_prefix}: Logging configuration uses unsupported "
                    " driver {container_def['logConfiguration'].get('logDriver')!r}. "
                    "Output cannot be streamed."
                )
            else:
                # Prepare to stream the output
                log_config = container_def["logConfiguration"]["options"]
                logs_client = boto_session.client("logs")
                can_stream_output = True
                # Track the last log timestamp to prevent double display
                last_log_timestamp: Optional[int] = None
                # Determine the name of the stream as "prefix/family/run"
                stream_name = "/".join(
                    [
                        log_config["awslogs-stream-prefix"],
                        task_definition["family"],
                        task_arn.rsplit("/")[-1],
                    ]
                )
                self.logger.info(
                    f"{self.log_prefix}: Streaming output from container "
                    f"{PREFECT_ECS_CONTAINER_NAME!r}..."
                )

        for task in self._watch_task_run(
            task_arn, cluster_arn, ecs_client, current_status="RUNNING"
        ):
            if self.stream_output and can_stream_output:
                # On each poll for task run status, also retrieve available logs
                last_log_timestamp = self._stream_available_logs(
                    logs_client,
                    log_group=log_config["awslogs-group"],
                    log_stream=stream_name,
                    last_log_timestamp=last_log_timestamp,
                )

        return task

    def _stream_available_logs(
        self,
        logs_client,
        log_group: str,
        log_stream: str,
        last_log_timestamp: Optional[int] = None,
    ) -> Optional[int]:
        """
        Stream logs from the given log group and stream since the last log timestamp.

        Will continue on paginated responses until all logs are returned.

        Returns the last log timestamp which can be used to call this method in the
        future.
        """
        last_log_stream_token = "NO-TOKEN"
        next_log_stream_token = None

        # AWS will return the same token that we send once the end of the paginated
        # response is reached
        while last_log_stream_token != next_log_stream_token:
            last_log_stream_token = next_log_stream_token

            request = {
                "logGroupName": log_group,
                "logStreamName": log_stream,
            }

            if last_log_stream_token is not None:
                request["nextToken"] = last_log_stream_token

            if last_log_timestamp is not None:
                # Bump the timestamp by one ms to avoid retrieving the last log again
                request["startTime"] = last_log_timestamp + 1

            response = logs_client.get_log_events(**request)

            log_events = response["events"]
            for log_event in log_events:
                # TODO: This doesn't forward to the local logger, which can be
                #       bad for customizing handling and understanding where the
                #       log is coming from, but it avoid nesting logger information
                #       when the content is output from a Prefect logger on the
                #       running infrastructure
                print(log_event["message"], file=sys.stderr)

                if (
                    last_log_timestamp is None
                    or log_event["timestamp"] > last_log_timestamp
                ):
                    last_log_timestamp = log_event["timestamp"]

            next_log_stream_token = response.get("nextForwardToken")

        return last_log_timestamp

    def _retrieve_task_definition(self, ecs_client, task_definition_arn: str):
        """
        Retrieve an existing task definition from AWS.
        """
        self.logger.info(
            f"{self.log_prefix}: "
            f"Retrieving task definition {task_definition_arn!r}..."
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

    def _prepare_task_definition(self, task_definition: dict, region: str) -> dict:
        """
        Prepare a task definition by inferring any defaults and merging overrides.
        """
        task_definition = copy.deepcopy(task_definition)

        # Configure the Prefect runtime container
        task_definition.setdefault("containerDefinitions", [])
        container = get_prefect_container(task_definition["containerDefinitions"])
        if container is None:
            container = {"name": PREFECT_ECS_CONTAINER_NAME}
            task_definition["containerDefinitions"].append(container)
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
        cpu = self.cpu or task_definition.get("cpu") or ECS_DEFAULT_CPU
        memory = self.memory or task_definition.get("memory") or ECS_DEFAULT_MEMORY

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
                    f"launch type {self.launch_type}. Either use the 'EC2' launch type "
                    "or the 'awsvpc' network mode."
                )

        elif self.launch_type == "EC2":
            # Container level memory and cpu are required when using ec2
            container.setdefault("cpu", int(cpu))
            container.setdefault("memory", int(memory))

        if self.execution_role_arn and not self.task_definition_arn:
            task_definition["executionRoleArn"] = self.execution_role_arn

        if self.configure_cloudwatch_logs and not task_definition.get(
            "executionRoleArn"
        ):
            raise ValueError(
                "An execution role arn must be set on the task definition to use "
                "`configure_cloudwatch_logs` or `stream_logs` but no execution role "
                "was found on the task definition."
            )

        return task_definition

    def _prepare_task_run_overrides(self) -> dict:
        """
        Prepare the 'overrides' payload for a task run request.
        """
        overrides = {
            "containerOverrides": [
                {
                    "name": PREFECT_ECS_CONTAINER_NAME,
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

        if self.memory:
            overrides["memory"] = str(self.memory)
            prefect_container_overrides.setdefault("memory", self.memory)

        if self.cpu:
            overrides["cpu"] = str(self.cpu)
            prefect_container_overrides.setdefault("cpu", self.cpu)

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
                {"key": key, "value": value} for key, value in self.labels.items()
            ],
            "taskDefinition": task_definition_arn,
        }

        if self.cluster:
            task_run["cluster"] = self.cluster

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

    def _run_task(self, ecs_client, task_run):
        """
        Run the task using the ECS client.

        This is isolated as a separate method for testing purposes.
        """
        return ecs_client.run_task(**task_run)["tasks"][0]
