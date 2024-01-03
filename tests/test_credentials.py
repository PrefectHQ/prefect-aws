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


def test_get_client_cached():
    """
    Test to ensure that _get_client_cached function returns the same instance
    for multiple calls with the same parameters and properly utilizes lru_cache.
    """

    # Create a mock AwsCredentials instance
    aws_credentials_block = AwsCredentials()

    # Clear cache
    _get_client_cached.cache_clear()

    assert _get_client_cached.cache_info().hits == 0, "Initial call count should be 0"

    # Call get_client multiple times with the same parameters
    aws_credentials_block.get_client(ClientType.S3)
    aws_credentials_block.get_client(ClientType.S3)
    aws_credentials_block.get_client(ClientType.S3)

    # Verify that _get_client_cached is called only once due to caching
    assert _get_client_cached.cache_info().misses == 1
    assert _get_client_cached.cache_info().hits == 2

    # Test with different parameters to ensure they are cached separately
    aws_credentials_block.get_client(ClientType.SECRETS_MANAGER)
    aws_credentials_block.get_client(ClientType.SECRETS_MANAGER)

    # "Should be called again with different parameters"
    assert _get_client_cached.cache_info().misses == 2
    assert _get_client_cached.cache_info().hits == 3
