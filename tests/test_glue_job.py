from unittest.mock import MagicMock

import pytest
from moto import mock_glue

from prefect_aws.glue_job import GlueJobBlock, GlueJobRun


@pytest.fixture(scope="function")
def glue_job_client(aws_credentials):
    with mock_glue():
        boto_session = aws_credentials.get_boto3_session()
        yield boto_session.client("glue")


async def test_fetch_result(aws_credentials, glue_job_client):
    glue_job_client.create_job(
        Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
    )
    job_run_id = glue_job_client.start_job_run(
        JobName="test_job_name",
        Arguments={},
    )["JobRunId"]
    glue_job_run = GlueJobRun(job_name="test_job_name", arguments={"arg1": "value1"})
    glue_job_run.job_id = job_run_id
    glue_job_run.client = glue_job_client
    result = await glue_job_run.fetch_result()
    assert result == "SUCCEEDED"


def test_start_job(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        glue_job_run = GlueJobRun(
            job_name="test_job_name", arguments={"arg1": "value1"}
        )
        glue_job_run.client = glue_job_client
        glue_job_run._start_job()
        assert glue_job_run.job_id != ""


def test_start_job_fail_because_not_exist_job(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_run = GlueJobRun(
            job_name="test_job_name", arguments={"arg1": "value1"}
        )
        glue_job_run.client = glue_job_client
        with pytest.raises(RuntimeError):
            glue_job_run._start_job()


def test_watch_job(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        glue_job_run = GlueJobRun(
            job_name="test_job_name",
            arguments={"arg1": "value1"},
            job_watch_poll_interval=0.1,
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]

        glue_job_client.get_job_run = MagicMock(
            side_effect=[
                {
                    "JobRun": {
                        "JobName": "test_job_name",
                        "JobRunState": "RUNNING",
                    }
                },
                {
                    "JobRun": {
                        "JobName": "test_job_name",
                        "JobRunState": "SUCCEEDED",
                    }
                },
            ]
        )
        glue_job_run.client = glue_job_client
        glue_job_run.job_id = job_run_id
        glue_job_run._watch_job()


def test_watch_job_fail(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        glue_job_run = GlueJobRun(
            job_name="test_job_name", arguments={"arg1": "value1"}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]
        glue_job_run.client = glue_job_client
        glue_job_run.job_id = job_run_id
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
        with pytest.raises(RuntimeError):
            glue_job_run._watch_job()


def test_get_client(aws_credentials):
    with mock_glue():
        glue_job_run = GlueJobRun(
            job_name="test_job_name", aws_credentials=aws_credentials
        )
        glue_job_run._get_client()
        assert hasattr(glue_job_run.client, "get_job_run")


def test__get_job_run(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]

        glue_job_run = GlueJobRun(
            job_name="test_job_name", arguments={"arg1": "value1"}
        )
        glue_job_run.job_id = job_run_id
        glue_job_run.client = glue_job_client
        response = glue_job_run._get_job_run()
        assert response["JobRun"]["JobRunState"] == "SUCCEEDED"


async def test_trigger():
    glue_job = GlueJobBlock(job_name="test_job_name", arguments={"arg1": "value1"})
    glue_job_run = await glue_job.trigger()
    assert isinstance(glue_job_run, GlueJobRun)
