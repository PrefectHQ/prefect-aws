from unittest.mock import MagicMock

import pytest
from moto import mock_glue
from prefect import flow

from prefect_aws.glue_job import GlueJob, GlueJobResult


@pytest.fixture(scope="function")
def glue_job_client(aws_credentials):
    with mock_glue():
        boto_session = aws_credentials.get_boto3_session()
        yield boto_session.client("glue")


def test_get_client(aws_credentials):
    with mock_glue():
        glue_client = GlueJob(
            job_name="test_job_name", arguments={}, aws_credentials=aws_credentials
        )._get_client()
        assert hasattr(glue_client, "get_job_run")


def test_start_job(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        glue_job = GlueJob(job_name="test_job_name", arguments={"arg1": "value1"})
        print(glue_job.preview())
        res_job_id = glue_job._start_job(glue_job_client)
        assert res_job_id == "01"


def test_start_job_fail_because_not_exist_job(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job = GlueJob(job_name="test_job_name", arguments={})
        with pytest.raises(RuntimeError):
            glue_job._start_job(glue_job_client)


def test_watch_job_and_get_exit_code(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]
        glue_job = GlueJob(
            job_name="test_job_name", arguments={}, job_watch_poll_interval=1.0
        )
        glue_job_client.get_job_run = MagicMock(
            side_effect=[
                {"JobRun": {"JobName": "test_job", "JobRunState": "RUNNING"}},
                {"JobRun": {"JobName": "test_job", "JobRunState": "SUCCEEDED"}},
            ]
        )

        exist_code = glue_job._watch_job_and_get_exit_code(glue_job_client, job_run_id)
        assert exist_code == 0


def test_watch_job_and_get_exit_fail(aws_credentials, glue_job_client):
    with mock_glue():
        glue_job_client.create_job(
            Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
        )
        job_run_id = glue_job_client.start_job_run(
            JobName="test_job_name",
            Arguments={},
        )["JobRunId"]
        glue_job = GlueJob(
            job_name="test_job_name", arguments={}, job_watch_poll_interval=1.0
        )
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
            glue_job._watch_job_and_get_exit_code(glue_job_client, job_run_id)


def test_run_with_client(aws_credentials, glue_job_client):
    with mock_glue():

        @flow
        def test_run_with_client_flow():
            glue_job_client.create_job(
                Name="test_job_name", Role="test-role", Command={}, DefaultArguments={}
            )
            return GlueJob(
                job_name="test_job_name", arguments={}, job_watch_poll_interval=1.0
            ).run_with_client(glue_job_client)

        res = test_run_with_client_flow()
        assert res == GlueJobResult(identifier="01", status_code=0)
