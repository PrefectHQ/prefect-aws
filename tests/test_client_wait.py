from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_batch, mock_iam
from prefect import flow, task

from botocore.waiter import WaiterModel
from prefect_aws.credentials import AwsCredentials

from prefect_aws.client_wait import _load_prefect_waiter, client_waiter


TEST_WAITERS = {
    "version": 2,
    "waiters": {
        "TestWaiter": {
            "delay": 5,
            "operation": "DescribeJobs",
            "maxAttempts": 100,
            "acceptors": [
                {
                    "argument": "jobs[].status",
                    "expected": "FAILED",
                    "matcher": "pathAll",
                    "state": "success",
                },
                {
                    "argument": "jobs[].status",
                    "expected": "SUCCEEDED",
                    "matcher": "pathAll",
                    "state": "success",
                },
            ],
        }
    },
}


def mock_create_waiter_with_client(*args, **kwargs):
    return "waiter"

def mock_waiter_wait(*args,**kwargs):
    return "wait1"


def test_load_prefect_waiter(monkeypatch):
    monkeypatch.setattr(
        "prefect_aws.client_wait.create_waiter_with_client",
        mock_create_waiter_with_client,
    )
    mock_boto_client = MagicMock()
    result = _load_prefect_waiter(mock_boto_client, "batch", "JobExists")
    assert result == "waiter"

def test_client_waiter(monkeypatch, aws_credentials):
    monkeypatch.setattr(
        'boto3.waiter.EC2.Waiter.InstanceExists.wait',
        mock_waiter_wait
    )

    waiter = client_waiter(
        "ec2",
        "instance_exists",
        aws_credentials)
    
    assert waiter is None