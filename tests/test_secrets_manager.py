import boto3
import pytest
from moto import mock_secretsmanager
from prefect import flow

from prefect_aws.secrets_manager import read_secret


@pytest.fixture
def secretsmanager_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", "us-east-1")


@pytest.fixture(
    params=[
        dict(Name="secret_string_no_version", SecretString="1"),
        dict(
            Name="secret_string_with_version_id", SecretString="2", should_version=True
        ),
        dict(Name="secret_binary_no_version", SecretBinary=b"3"),
        dict(
            Name="secret_binary_with_version_id", SecretBinary=b"4", should_version=True
        ),
    ]
)
def secret_under_test(secretsmanager_client, request):
    should_version = request.param.pop("should_version", False)
    version_stage = request.param.pop("version_stage", None)
    secretsmanager_client.create_secret(**request.param)

    update_result = None
    if should_version:
        if "SecretString" in request.param:
            request.param["SecretString"] = request.param["SecretString"] + "-versioned"
        elif "SecretBinary" in request.param:
            request.param["SecretBinary"] = (
                request.param["SecretBinary"] + b"-versioned"
            )
        update_secret_kwargs = request.param.copy()
        update_secret_kwargs["SecretId"] = update_secret_kwargs.pop("Name")
        update_result = secretsmanager_client.update_secret(**update_secret_kwargs)

    return dict(
        secret_name=request.param.get("Name"),
        version_id=update_result.get("VersionId") if update_result else None,
        version_stage=version_stage,
        expected_value=request.param.get("SecretString")
        or request.param.get("SecretBinary"),
    )


async def test_read_secret(secret_under_test, aws_credentials):
    expected_value = secret_under_test.pop("expected_value")
    secret_under_test.pop("version_stage")

    @flow
    async def test_flow():
        return await read_secret(
            aws_credentials=aws_credentials,
            **secret_under_test,
        )

    assert (await test_flow()).result().result() == expected_value
