from datetime import datetime, timedelta
from unittest.mock import MagicMock

from moto import mock_ecr
from prefect.infrastructure.docker import DockerContainer

from prefect_aws.ecr import ElasticContainerRegistry


def test_ecr_login_no_registry_id_provided(aws_credentials):
    mock_base_login = MagicMock()
    registry = ElasticContainerRegistry(aws_credentials=aws_credentials)
    registry._login = mock_base_login

    with mock_ecr():
        registry.login()

    mock_base_login.assert_called_once_with(
        username="AWS",
        password="123456789012-auth-token",
        registry_url="https://123456789012.dkr.ecr.us-east-1.amazonaws.com",
        reauth=True,
    )


def test_ecr_login_with_registry_id(aws_credentials):
    mock_base_login = MagicMock()
    registry = ElasticContainerRegistry(
        aws_credentials=aws_credentials, registry_id="test"
    )
    registry._login = mock_base_login

    with mock_ecr():
        registry.login()

    mock_base_login.assert_called_once_with(
        username="AWS",
        password="test-auth-token",
        registry_url="https://test.dkr.ecr.us-east-1.amazonaws.com",
        reauth=True,
    )


def test_ecr_login_with_cached_token(aws_credentials):
    mock_base_login = MagicMock()
    registry = ElasticContainerRegistry(
        aws_credentials=aws_credentials, registry_id="test"
    )
    registry._login = mock_base_login

    mock_get_token = MagicMock()
    original_get_ecr_client = registry._get_ecr_client

    def tracked_get_ecr_client():
        client = original_get_ecr_client()
        # Retain original behavior
        mock_get_token.side_effect = client.get_authorization_token
        client.get_authorization_token = mock_get_token
        return client

    registry._get_ecr_client = tracked_get_ecr_client

    with mock_ecr():
        registry.login()

        # moto defaults to a token expiration in 2015, but it must be in the future for
        # this test
        registry._token_expiration = datetime.now(
            tz=registry._token_expiration.tzinfo
        ) + timedelta(hours=12)

        registry.login()

    assert mock_get_token.call_count == 1, "The token should be cached"

    # The second call is correct
    mock_base_login.assert_called_with(
        username="AWS",
        password="test-auth-token",
        registry_url="https://test.dkr.ecr.us-east-1.amazonaws.com",
        reauth=True,
    )
    assert mock_base_login.call_count == 2


def test_ecr_login_with_expired_cached_token(aws_credentials):
    mock_base_login = MagicMock()
    registry = ElasticContainerRegistry(
        aws_credentials=aws_credentials, registry_id="test"
    )
    registry._login = mock_base_login

    mock_get_token = MagicMock()
    original_get_ecr_client = registry._get_ecr_client

    def tracked_get_ecr_client():
        client = original_get_ecr_client()
        # Retain original behavior
        mock_get_token.side_effect = client.get_authorization_token
        client.get_authorization_token = mock_get_token
        return client

    registry._get_ecr_client = tracked_get_ecr_client

    with mock_ecr():
        registry.login()

        registry._token_expiration = datetime.now(tz=registry._token_expiration.tzinfo)

        registry.login()

    assert (
        mock_get_token.call_count == 2
    ), "The token should be expired and retrieved again"


def test_ecr_register_docker_container_compatibility(aws_credentials):
    registry = ElasticContainerRegistry(
        aws_credentials=aws_credentials, registry_id="test"
    )
    container = DockerContainer(image_registry=registry)

    assert container.image_registry == registry
