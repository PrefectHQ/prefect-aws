from unittest.mock import patch

import pytest
from boto3.session import Session
from botocore.client import BaseClient
from moto import mock_s3

from prefect_aws.credentials import (
    AwsCredentials,
    ClientType,
    MinIOCredentials,
    _get_client_cached,
)


def test_aws_credentials_get_boto3_session():
    """
    Asserts that instantiated AwsCredentials block creates an
    authenticated boto3 session.
    """

    with mock_s3():
        aws_credentials_block = AwsCredentials()
        boto3_session = aws_credentials_block.get_boto3_session()
        assert isinstance(boto3_session, Session)


def test_minio_credentials_get_boto3_session():
    """
    Asserts that instantiated MinIOCredentials block creates
    an authenticated boto3 session.
    """

    minio_credentials_block = MinIOCredentials(
        minio_root_user="root_user", minio_root_password="root_password"
    )
    boto3_session = minio_credentials_block.get_boto3_session()
    assert isinstance(boto3_session, Session)


@pytest.mark.parametrize(
    "credentials",
    [
        AwsCredentials(),
        MinIOCredentials(
            minio_root_user="root_user", minio_root_password="root_password"
        ),
    ],
)
@pytest.mark.parametrize("client_type", ["s3", ClientType.S3])
def test_credentials_get_client(credentials, client_type):
    with mock_s3():
        assert isinstance(credentials.get_client(client_type), BaseClient)


@patch("prefect_aws.credentials._get_client_cached")
def test_get_client_cached(mock_get_client_cached):
    """
    Test to ensure that _get_client_cached function returns the same instance
    for multiple calls with the same parameters and properly utilizes lru_cache.
    """

    # Create a mock AwsCredentials instance
    aws_credentials_block = AwsCredentials()

    # Call _get_client_cached multiple times with the same parameters
    _get_client_cached(aws_credentials_block, ClientType.S3)
    _get_client_cached(aws_credentials_block, ClientType.S3)

    # Verify that _get_client_cached is called only once due to caching
    mock_get_client_cached.assert_called_once_with(aws_credentials_block, ClientType.S3)

    # Test with different parameters to ensure they are cached separately
    _get_client_cached(aws_credentials_block, ClientType.ECS)
    assert (
        mock_get_client_cached.call_count == 2
    ), "Should be called twice with different parameters"
