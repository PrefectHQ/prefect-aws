"""Module handling AWS credentials"""

from dataclasses import dataclass
from typing import Optional

import boto3


@dataclass
class AwsCredentials:
    """
    Dataclass used to manage authentication with AWS. AWS authentication is
    handled via the `boto3` module. Refer to the
    [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.

    Args:
        aws_access_key_id: A specific AWS access key ID.
        aws_secret_access_key: A specific AWS secret access key.
        aws_session_token: The session key for your AWS account.
            This is only needed when you are using temporary credentials.
        profile_name: The profile to use when creating your session.
        region_name: The AWS Region where you want to create new connections.
    """  # noqa E501

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    profile_name: Optional[str] = None
    region_name: Optional[str] = None

    def get_boto3_session(self) -> boto3.Session:
        """
        Returns an authenticated boto3 session that can be used to create clients
        for AWS services

        Example:
            Create an S3 client from an authorized boto3 session

            >>> aws_credentials = AwsCredentials(
            >>>     aws_access_key_id = "access_key_id",
            >>>     aws_secret_access_key = "secret_access_key"
            >>> )
            >>> s3_client = aws_credentials.get_boto3_session().client("s3")
        """
        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            profile_name=self.profile_name,
            region_name=self.region_name,
        )
