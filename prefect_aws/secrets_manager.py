"""Tasks for interacting with AWS Secrets Manager"""
from functools import partial
from typing import Optional, Union

from anyio import to_thread
from botocore.exceptions import ClientError
from prefect import get_run_logger, task

from prefect_aws.credentials import AwsCredentials


@task
async def read_secret(
    secret_name: str,
    aws_credentials: AwsCredentials,
    version_id: Optional[str] = None,
    version_stage: Optional[str] = None,
) -> Union[str, bytes]:
    """
    Reads the value of a given secret from AWS Secrets Manager

    Args:
        secret_name: Name of stored secret
        aws_credentials: Credentials to use for authentication with AWS.
        version_id: Specifies version of secret to read. Defaults to the most recent
            version if not given.
        version_stage: Specifies the version stage of the secret to read. Defaults to
            AWS_CURRENT if not given.

    Returns:
        The secret values as a `str` or `bytes` depending on the format in which the
            secret was stored.

    Example:
        Read a secret value

        ```python
        from prefect import flow
        from prefect_aws.secrets_manager import read_secret
        from prefect_aws.credentials import AwsCredentials

        @flow
        def example_read_secret():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )
            secret_value = read_secret(
                secret_name="db_password",
                aws_credentials=aws_credentials
            )
        ```
    """
    logger = get_run_logger()
    logger.info("Getting value for secret %s", secret_name)

    client = aws_credentials.get_boto3_session().client("secretsmanager")

    get_secret_value_kwargs = dict(SecretId=secret_name)
    if version_id is not None:
        get_secret_value_kwargs["VersionId"] = version_id
    if version_stage is not None:
        get_secret_value_kwargs["VersionStage"] = version_stage

    try:
        get_secret_value = partial(client.get_secret_value, **get_secret_value_kwargs)
        response = await to_thread.run_sync(get_secret_value)
    except ClientError:
        logger.exception("Unable to get value for secret %s", secret_name)
        raise
    else:
        return response.get("SecretString") or response.get("SecretBinary")


@task
def update_secret():
    raise NotImplementedError()


@task
def create_secret():
    raise NotImplementedError()


@task
def delete_secret():
    raise NotImplementedError()
