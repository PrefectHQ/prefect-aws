"""
Integrations with the AWS Glue Job.

"""
import time
from typing import Any, Optional

from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field

from typing_extensions import Literal

from prefect_aws import AwsCredentials

_GlueJobClient = Any


class GlueJobResult(InfrastructureResult):
    """The result of a run of a Glue Job"""


class GlueJob(Infrastructure):
    """
    Execute a job to the AWS Glue Job service.

    Attributes:
        job_name: The name of the job definition to use.
        arguments: The job arguments associated with this run.
            For this job run, they replace the default arguments set in the job
            definition itself.
            You can specify arguments here that your own job-execution script consumes,
            as well as arguments that Glue itself consumes.
            Job arguments may be logged. Do not pass plaintext secrets as arguments.
            Retrieve secrets from a Glue Connection, Secrets Manager or other secret
            management mechanism if you intend to keep them within the Job.
            [doc](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)
        job_watch_poll_interval: The amount of time to wait between AWS API
            calls while monitoring the state of a Glue Job.
            default is 60s because of jobs that use AWS Glue versions 2.0 and later
            have a 1-minute minimum.
            [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/?nc1=h_ls)

    Example:
        Start a job to AWS Glue Job.

        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.glue_job import GlueJob


        @flow
        def example_run_glue_job():
            aws_credentials = AwsCredentials(
                aws_access_key_id="your_access_key_id",
                aws_secret_access_key="your_secret_access_key"
            )
            glue_job = GlueJob(
                job_name="your_glue_job_name",
                arguments={"--YOUR_EXTRA_ARGUMENT": "YOUR_EXTRA_ARGUMENT_VALUE"},
            )
            return glue_job.run()

        example_run_glue_job()
        ```"""

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

    _error_states = ["FAILED", "STOPPED", "ERROR", "TIMEOUT"]

    type: Literal["glue-job"] = Field(
        "glue-job", description="The slug for this task type."
    )

    aws_credentials: AwsCredentials = Field(
        title="AWS Credentials",
        default_factory=AwsCredentials,
        description="The AWS credentials to use to connect to Glue.",
    )

    @sync_compatible
    async def run(self) -> GlueJobResult:
        """Run the Glue Job."""
        glue_client = await run_sync_in_worker_thread(self._get_client)

        return await run_sync_in_worker_thread(self.run_with_client, glue_client)

    @sync_compatible
    async def run_with_client(self, glue_job_client: _GlueJobClient) -> GlueJobResult:
        """Run the Glue Job with Glue Client."""
        run_job_id = await run_sync_in_worker_thread(self._start_job, glue_job_client)
        exit_code = await run_sync_in_worker_thread(
            self._watch_job_and_get_exit_code, glue_job_client, run_job_id
        )

        return GlueJobResult(identifier=run_job_id, status_code=exit_code)

    def preview(self) -> str:
        """
        Generate a preview of the job information that will be sent to AWS.
        """
        preview = "---\n# Glue Job\n"

        preview += f"Target Glue Job Name: {self.job_name}\n"
        if self.arguments is not None:
            argument_text = ""
            for key, value in self.arguments.items():
                argument_text += f"  - {key}: {value}\n"
            preview += f"Job Arguments: \n{argument_text}"
        preview += f"Job Watch Interval: {self.job_watch_poll_interval}s\n"
        return preview

    def _start_job(self, glue_job_client: _GlueJobClient) -> str:
        """
        Start the AWS Glue Job
        [doc](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue/client/start_job_run.html)
        """
        self.logger.info(
            f"starting job {self.job_name} with arguments {self.arguments}"
        )
        try:
            response = self.__start_job_run(glue_job_client)
            job_run_id = str(response["JobRunId"])
            self.logger.info(f"job started with job run id: {job_run_id}")
            return job_run_id
        except Exception as e:
            self.logger.error(f"failed to start job: {e}")
            raise RuntimeError

    def _watch_job_and_get_exit_code(
        self, glue_job_client: _GlueJobClient, job_run_id: str
    ) -> Optional[int]:
        """
        Wait for the job run to complete and get exit code
        """
        self.logger.info(f"watching job {self.job_name} with run id {job_run_id}")
        exit_code = 0
        while True:
            job = self.__get_job_run(glue_job_client, job_run_id)
            job_state = job["JobRun"]["JobRunState"]
            if job_state in self._error_states:
                # Generate a dynamic exception type from the AWS name
                self.logger.error(f"job failed: {job['JobRun']['ErrorMessage']}")
                raise RuntimeError(job["JobRun"]["ErrorMessage"])
            elif job_state == "SUCCEEDED":
                self.logger.info(f"job succeeded: {job_run_id}")
                break

            time.sleep(self.job_watch_poll_interval)
        return exit_code

    def _get_client(self) -> _GlueJobClient:
        """
        Retrieve a Glue Job Client
        """
        boto_session = self.aws_credentials.get_boto3_session()
        return boto_session.client("glue")

    def __start_job_run(self, glue_job_client: _GlueJobClient):
        return glue_job_client.start_job_run(
            JobName=self.job_name,
            Arguments=self.arguments,
        )

    def __get_job_run(self, glue_job_client: _GlueJobClient, job_run_id: str):
        return glue_job_client.get_job_run(JobName=self.job_name, RunId=job_run_id)
