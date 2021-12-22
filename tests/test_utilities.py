import os
from pathlib import Path

import pytest
from prefect_aws.utilities import get_boto_client
from moto import mock_s3


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    moto_credentials_file_path = (
        Path(__file__).parent.absolute() / "mock_aws_credentials"
    )
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(moto_credentials_file_path)


def test_client_cache_same_credentials():
    client1 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    client2 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    assert client1 is client2


def test_client_cache_different_credentials():
    client1 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key_1",
        aws_secret_access_key="secret_key_1",
    )

    client2 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key_2",
        aws_secret_access_key="secret_key_3",
    )

    assert client1 is not client2


def test_client_cache_same_profile(aws_credentials):
    client1 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    assert client1 is client2


def test_client_cache_different_profile(aws_credentials):
    client1 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_2")

    assert client1 is not client2

@mock_s3
def test_client_cache_with_kwargs(aws_credentials):
    client1 = get_boto_client(resource="s3", use_ssl=False)

    client2 = get_boto_client(resource="s3", use_ssl=False)

    assert client1 is not client2
