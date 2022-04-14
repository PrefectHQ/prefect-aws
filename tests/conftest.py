import pytest
from prefect.utilities.testing import prefect_test_harness

from prefect_aws import AwsCredentials


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
