"""Module handling AWS credentials"""

from typing import Optional

import boto3
from prefect.blocks.core import Block
from pydantic import Field, SecretStr


class AwsCredentials(Block):
    """
    Block used to manage authentication with AWS. AWS authentication is
    handled via the `boto3` module. Refer to the
    [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    for more info about the possible credential configurations.

    Example:
        Load stored AWS credentials:
        ```python
        from prefect_aws import AwsCredentials

        aws_credentials_block = AwsCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/1jbV4lceHOjGgunX15lUwT/db88e184d727f721575aeb054a37e277/aws.png?h=250"  # noqa
    _block_type_name = "AWS Credentials"

    aws_access_key_id: Optional[str] = Field(
        default=None, description="A specific AWS access key ID."
    )
    aws_secret_access_key: Optional[SecretStr] = Field(
        default=None, description="A specific AWS secret access key."
    )
    aws_session_token: Optional[str] = Field(
        default=None,
        description=(
            "The session key for your AWS account. "
            "This is only needed when you are using temporary credentials."
        ),
    )
    profile_name: Optional[str] = Field(
        default=None, description="The profile to use when creating your session."
    )
    region_name: Optional[str] = Field(
        default=None,
        description="The AWS Region where you want to create new connections.",
    )

    def get_boto3_session(self) -> boto3.Session:
        """
        Returns an authenticated boto3 session that can be used to create clients
        for AWS services

        Example:
            Create an S3 client from an authorized boto3 session:
            ```python
            aws_credentials = AwsCredentials(
                aws_access_key_id = "access_key_id",
                aws_secret_access_key = "secret_access_key"
                )
            s3_client = aws_credentials.get_boto3_session().client("s3")
            ```
        """

        if self.aws_secret_access_key:
            aws_secret_access_key = self.aws_secret_access_key.get_secret_value()
        else:
            aws_secret_access_key = None

        return boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            profile_name=self.profile_name,
            region_name=self.region_name,
        )


class MinIOCredentials(Block):
    """
    Block used to manage authentication with MinIO. Refer to the
    [MinIO docs](https://docs.min.io/docs/minio-server-configuration-guide.html)
    for more info about the possible credential configurations.

    Attributes:
        minio_root_user: Admin or root user.
        minio_root_password: Admin or root password.
        region_name: Location of server, e.g. "us-east-1".

    Example:
        Load stored MinIO credentials:
        ```python
        from prefect_aws import MinIOCredentials

        minio_credentials_block = MinIOCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/22vXcxsOrVeFrUwHfSoaeT/7607b876eb589a9028c8126e78f4c7b4/imageedit_7_2837870043.png?h=250"  # noqa
    _block_type_name = "MinIO Credentials"
    _description = (
        "Block used to manage authentication with MinIO. Refer to the MinIO "
        "docs: https://docs.min.io/docs/minio-server-configuration-guide.html "
        "for more info about the possible credential configurations."
    )

    minio_root_user: str
    minio_root_password: SecretStr
    region_name: Optional[str] = None

    def get_boto3_session(self) -> boto3.Session:
        """
        Returns an authenticated boto3 session that can be used to create clients
        and perform object operations on MinIO server.

        Example:
            Create an S3 client from an authorized boto3 session

            ```python
            minio_credentials = MinIOCredentials(
                minio_root_user = "minio_root_user",
                minio_root_password = "minio_root_password"
            )
            s3_client = minio_credentials.get_boto3_session().client(
                service="s3",
                endpoint_url="http://localhost:9000"
            )
            ```
        """

        minio_root_password = (
            self.minio_root_password.get_secret_value()
            if self.minio_root_password
            else None
        )

        return boto3.Session(
            aws_access_key_id=self.minio_root_user,
            aws_secret_access_key=minio_root_password,
            region_name=self.region_name,
        )
