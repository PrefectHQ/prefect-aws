import boto3
import pytest
from moto import mock_secretsmanager
from prefect import flow

from prefect_aws.secrets_manager import read_secret, update_secret


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
        expected_value=request.param.get("SecretString")
        or request.param.get("SecretBinary"),
    )


async def test_read_secret(secret_under_test, aws_credentials):
    expected_value = secret_under_test.pop("expected_value")

    @flow
    async def test_flow():
        return await read_secret(
            aws_credentials=aws_credentials,
            **secret_under_test,
        )

    assert (await test_flow()).result().result() == expected_value


async def test_update_secret(secret_under_test, aws_credentials):
    current_secret_value = secret_under_test["expected_value"]
    new_secret_value = (
        current_secret_value + "2"
        if isinstance(current_secret_value, str)
        else current_secret_value + b"2"
    )

    @flow
    async def test_flow():
        return await update_secret(
            aws_credentials=aws_credentials,
            secret_name=secret_under_test["secret_name"],
            secret_value=new_secret_value,
        )

    flow_state = await test_flow()
    assert flow_state.result().result().get("Name") == secret_under_test["secret_name"]

    sm_client = aws_credentials.get_boto3_session().client("secretsmanager")
    updated_secret = sm_client.get_secret_value(
        SecretId=secret_under_test["secret_name"]
    )
    assert (
        updated_secret.get("SecretString") == new_secret_value
        or updated_secret.get("SecretBinary") == new_secret_value
    )
