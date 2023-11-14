from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from moto import mock_glue
from prefect.server.schemas.core import FlowRun

from prefect_aws.workers.glue_job_worker import (
    GlueJobWorker,
    GlueJobWorkerConfiguration,
    GlueJobWorkerResult,
)


@pytest.fixture(scope="function")
def glue_job_client(aws_credentials):
    with mock_glue():
        boto_session = aws_credentials.get_boto3_session()
        yield boto_session.client("glue")


@pytest.fixture
def flow_run():
    return FlowRun(flow_id=uuid4(), deployment_id=uuid4())


def test_get_client(aws_credentials):
    with mock_glue():
        job_worker_configuration = GlueJobWorkerConfiguration(
            job_name="test_glue_job_name", aws_credentials=aws_credentials
        )
        glue_job_worker = GlueJobWorker(work_pool_name="test")
        glue_client = glue_job_worker._get_client(job_worker_configuration)
        assert hasattr(glue_client, "get_job_run")


async def test_start_job(aws_credentials, glue_job_client, flow_run):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )

        job_worker_configuration = GlueJobWorkerConfiguration(
            job_name="test_job_name", arguments={}
        )
        async with GlueJobWorker(work_pool_name="test") as worker:
            logger = worker.get_flow_run_logger(flow_run)
            res_job_id = worker._start_job(
                logger, glue_job_client, job_worker_configuration
            )
            assert res_job_id == "01"


async def test_start_job_fail_because_not_exist_job(
    aws_credentials, glue_job_client, flow_run
):
    with mock_glue():
        job_worker_configuration = GlueJobWorkerConfiguration(
            job_name="test_job_name", arguments={}
        )
        async with GlueJobWorker(work_pool_name="test") as worker:
            logger = worker.get_flow_run_logger(flow_run)
            with pytest.raises(RuntimeError):
                worker._start_job(logger, glue_job_client, job_worker_configuration)


async def test_watch_job_and_get_exit_code(aws_credentials, glue_job_client, flow_run):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]

        job_worker_configuration = GlueJobWorkerConfiguration(
            job_name="test_job_name", arguments={}, job_watch_poll_interval=1.0
        )
        async with GlueJobWorker(work_pool_name="test") as worker:
            glue_job_client.get_job_run = MagicMock(
                side_effect=[
                    {"JobRun": {"JobName": "test_job", "JobRunState": "RUNNING"}},
                    {"JobRun": {"JobName": "test_job", "JobRunState": "SUCCEEDED"}},
                ]
            )
            logger = worker.get_flow_run_logger(flow_run)
            exist_code = worker._watch_job_and_get_exit_code(
                logger, glue_job_client, job_run_id, job_worker_configuration
            )
            assert exist_code == 0


async def test_watch_job_and_get_exit_fail(aws_credentials, glue_job_client, flow_run):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]

        job_worker_configuration = GlueJobWorkerConfiguration(
            job_name="test_job_name", arguments={}, job_watch_poll_interval=1.0
        )
        async with GlueJobWorker(work_pool_name="test") as worker:
            glue_job_client.get_job_run = MagicMock(
                side_effect=[
                    {
                        "JobRun": {
                            "JobName": "test_job_name",
                            "JobRunState": "FAILED",
                            "ErrorMessage": "err",
                        }
                    },
                ]
            )
            logger = worker.get_flow_run_logger(flow_run)
            with pytest.raises(RuntimeError):
                worker._watch_job_and_get_exit_code(
                    logger, glue_job_client, job_run_id, job_worker_configuration
                )


async def test_run_with_client(aws_credentials, glue_job_client, flow_run):
    with mock_glue():
        async with GlueJobWorker(work_pool_name="test") as worker:
            glue_job_client.create_job(
                Name="test_job_name1", Role="test-role", Command={}, DefaultArguments={}
            )
            job_worker_configuration = GlueJobWorkerConfiguration(
                job_name="test_job_name1", arguments={}, job_watch_poll_interval=1.0
            )
            res = await worker.run_with_client(
                flow_run, glue_job_client, job_worker_configuration
            )

            assert res == GlueJobWorkerResult(identifier="01", status_code=0)
