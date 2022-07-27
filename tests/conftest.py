import pytest
from prefect.testing.utilities import prefect_test_harness

from prefect_aws.client_parameters import AwsClientParameters
from prefect_aws.credentials import AwsCredentials


@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    with prefect_test_harness():
        yield


@pytest.fixture
def aws_credentials():
    return AwsCredentials(
        aws_access_key_id="access_key_id",
        aws_secret_access_key="secret_access_key",
        region_name="us-east-1",
    )


@pytest.fixture
def aws_client_parameters_custom_endpoint():
    return AwsClientParameters(endpoint_url="http://custom.internal.endpoint.org")


@pytest.fixture
def aws_client_parameters_empty():
    return AwsClientParameters()
