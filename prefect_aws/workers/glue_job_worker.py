import logging
import time
from typing import Any, Optional

import anyio
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

_GlueJobClient = Any


class GlueJobWorkerConfiguration(BaseJobConfiguration):
    """
    Job configuration for a Glue Job.
    """

    job_name: str = Field(
        ...,
        title="AWS Glue Job Name",
        description="The name of the job definition to use.",
    )
    arguments: Optional[dict] = Field(
        default=None,
        title="AWS Glue Job Arguments",
        description="The job arguments associated with this run.",
    )
    job_watch_poll_interval: float = Field(
        default=60.0,
        description=(
            "The amount of time to wait between AWS API calls while monitoring the "
            "state of an Glue Job."
        ),
    )
    aws_credentials: Optional[AwsCredentials] = Field(default_factory=AwsCredentials)
    error_states = ["FAILED", "STOPPED", "ERROR", "TIMEOUT"]


class GlueJobWorkerResult(BaseWorkerResult):
    """
    The result of Glue job.
    """


class GlueJobWorker(BaseWorker):
    type = "glue-job"
    job_configuration = GlueJobWorkerConfiguration
    job_configuration_variables = BaseVariables
    _description = "Execute flow runs Glue Job."
    _display_name = "AWS Glue Job"
    _documentation_url = "https://prefecthq.github.io/prefect-aws/glue_job/"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/1jbV4lceHOjGgunX15lUwT/db88e184d727f721575aeb054a37e277/aws.png?h=250"  # noqa

    async def run(
        self,
        flow_run: FlowRun,
        configuration: GlueJobWorkerConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> GlueJobWorkerResult:
        """Run the Glue Job."""
        glue_job_client = await run_sync_in_worker_thread(
            self._get_client, configuration
        )
        return await run_sync_in_worker_thread(
            self.run_with_client, glue_job_client, configuration
        )

    async def run_with_client(
        self,
        flow_run: FlowRun,
        glue_job_client: _GlueJobClient,
        configuration: GlueJobWorkerConfiguration,
    ) -> GlueJobWorkerResult:
        """Run the Glue Job with Glue Client."""
        logger = self.get_flow_run_logger(flow_run)
        run_job_id = await run_sync_in_worker_thread(
            self._start_job, logger, glue_job_client, configuration
        )
        exit_code = await run_sync_in_worker_thread(
            self._watch_job_and_get_exit_code,
            logger,
            glue_job_client,
            run_job_id,
            configuration,
        )
        return GlueJobWorkerResult(identifier=run_job_id, status_code=exit_code)

    @staticmethod
    def _get_client(configuration: GlueJobWorkerConfiguration) -> _GlueJobClient:
        """
        Retrieve a Glue Job Client
        """
        boto_session = configuration.aws_credentials.get_boto3_session()
        return boto_session.client("glue")

    @staticmethod
    def _start_job(
        logger: logging.Logger,
        glue_job_client: _GlueJobClient,
        configuration: GlueJobWorkerConfiguration,
    ) -> str:
        """
        Start the AWS Glue Job
        [doc](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/start_job_run.html)
        """
        logger.info(
            f"starting job {configuration.job_name} with arguments"
            f" {configuration.arguments}"
        )
        try:
            response = glue_job_client.start_job_run(
                JobName=configuration.job_name,
                Arguments=configuration.arguments,
            )
            job_run_id = str(response["JobRunId"])
            logger.info(f"job started with job run id: {job_run_id}")
            return job_run_id
        except Exception as e:
            logger.error(f"failed to start job: {e}")
            raise RuntimeError

    @staticmethod
    def _watch_job_and_get_exit_code(
        logger: logging.Logger,
        glue_job_client: _GlueJobClient,
        job_run_id: str,
        configuration: GlueJobWorkerConfiguration,
    ) -> Optional[int]:
        """
        Wait for the job run to complete and get exit code
        """
        logger.info(f"watching job {configuration.job_name} with run id {job_run_id}")
        exit_code = 0
        while True:
            job = glue_job_client.get_job_run(
                JobName=configuration.job_name, RunId=job_run_id
            )
            job_state = job["JobRun"]["JobRunState"]
            if job_state in configuration.error_states:
                # Generate a dynamic exception type from the AWS name
                logger.error(f"job failed: {job['JobRun']['ErrorMessage']}")
                raise RuntimeError(job["JobRun"]["ErrorMessage"])
            elif job_state == "SUCCEEDED":
                logger.info(f"job succeeded: {job_run_id}")
                break

            time.sleep(configuration.job_watch_poll_interval)
        return exit_code
