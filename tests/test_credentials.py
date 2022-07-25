import os
import pytest
from moto import mock_s3
from pathlib import Path
from prefect_aws.credentials import AwsCredentials
from botocore.client import ClientError

BUCKET_NAME = "MY_BUCKET"


@pytest.fixture()
def mock_creds():

    """Mock AWS credentials for moto."""

    moto_credentials_filepath = (
        Path(__file__).parent.absolute() / "mock_aws_credentials"
    )
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(moto_credentials_filepath)


@pytest.mark.parametrize(
    argnames="profile_nm", argvalues=[("TEST_PROFILE_1"), ("TEST_PROFILE_2")]
)
def test_get_s3_client(mock_creds, profile_nm: str):

    """
    Given an AWS profile name, will create an AwsCredentials block and return an S3 Client."""

    with mock_s3():
        aws_credentials_block = AwsCredentials(profile_name=profile_nm)
        s3_client = aws_credentials_block.get_boto3_session().client("s3")
        return s3_client


@pytest.mark.parametrize(
    argnames="profile_nm", argvalues=[("TEST_PROFILE_1"), ("TEST_PROFILE_2")]
)
def test_create_bucket_and_return_location(mock_creds, profile_nm: str) -> dict:

    """
    Given an S3 client generated from the AwsCredentials block, creates bucket
    and validates existence. If not exists will raise an exception.

    Called during testing as part of assertion that session is properly
    configured from instantiated AwsCredentials block.
    """

    with mock_s3():
        aws_credentials_block = AwsCredentials(profile_name=profile_nm)
        s3_client = aws_credentials_block.get_boto3_session().client("s3")
        s3_client.create_bucket(Bucket=BUCKET_NAME)

        try:
            return s3_client.get_bucket_location(Bucket=BUCKET_NAME)

        except ClientError:
            raise Exception("Bucket was not created.")
