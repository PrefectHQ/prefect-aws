import pytest
from boto3.session import Session
from botocore.client import BaseClient
from moto import mock_s3

from prefect_aws.credentials import AwsCredentials, ClientType, MinIOCredentials


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
