import pytest
import boto3
from moto import mock_sns

from prefect_aws.sns import SNS


@pytest.fixture
def sns_mock():
    """Mock connection to AWS SNS with boto3 client."""

    with mock_sns():
        yield boto3.client(
            service_name="sns",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )


def test_task_publishes(sns_mock, aws_credentials):
    task = SNS(aws_credentials=aws_credentials, sns_arn="some_test_arn")
    task.publish("mysubject", "mymessage")
    assert True
