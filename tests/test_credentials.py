from boto3.session import Session
from moto import mock_s3

from prefect_aws.credentials import AwsCredentials


def test_aws_credentials_get_boto3_session(aws_credentials):

    """
    Asserts that instantiated AwsCredentials block creates an
    authenticated boto3 session.
    """

    with mock_s3():
        aws_credentials_block = AwsCredentials()
        boto3_session = aws_credentials_block.get_boto3_session()
        assert isinstance(boto3_session, Session)
