import pytest
from botocore.client import ClientError
from moto import mock_s3

from prefect_aws.credentials import AwsCredentials

BUCKET_NAME = "MY_BUCKET"


@pytest.mark.parametrize(
    argnames="profile_nm", argvalues=[("TEST_PROFILE_1"), ("TEST_PROFILE_2")]
)
def test_get_s3_client(aws_credentials, profile_nm: str):

    """
    Given an AWS profile name, will create an AwsCredentials block and return
    an S3 Client."""

    with mock_s3():
        aws_credentials_block = AwsCredentials(profile_name=profile_nm)
        s3_client = aws_credentials_block.get_boto3_session().client("s3")
        return s3_client


@pytest.mark.parametrize(
    argnames="profile_nm", argvalues=[("TEST_PROFILE_1"), ("TEST_PROFILE_2")]
)
def test_create_bucket_and_return_location(aws_credentials, profile_nm: str) -> dict:

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
