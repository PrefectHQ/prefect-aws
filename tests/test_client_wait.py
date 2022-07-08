from prefect import task, flow

import boto3
import pytest
from unittest.mock import MagicMock
from moto import mock_batch, mock_iam
from prefect import flow

from prefect_aws.client_wait import _load_prefect_waiter

def mock_create_waiter_with_client(*args, **kwargs):
    return MagicMock()

def test_load_prefect_waiter(monkeypatch):
    monkeypatch.setattr("botocore.waiter.create_waiter_with_client", mock_create_waiter_with_client)

    mock_boto_client = MagicMock()
    result = _load_prefect_waiter(mock_boto_client, "batch", "JobExists")
    assert result
