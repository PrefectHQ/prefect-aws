import pytest

from prefect_aws import AwsCredentials


@pytest.fixture
def aws_credentials():
    return AwsCredentials(
        aws_access_key_id="access_key_id",
        aws_secret_access_key="secret_access_key",
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")
def temp_db_path(tmpdir_factory):
    tmp_db_path = tmpdir_factory.mktemp("db")
    db_file_path = tmp_db_path.join("orion.db")
    return str(db_file_path)


@pytest.fixture(autouse=True)
def init_prefect_db(monkeypatch, temp_db_path):
    monkeypatch.setenv(
        "PREFECT_ORION_DATABASE_CONNECTION_URL", f"sqlite+aiosqlite://{temp_db_path}"
    )
