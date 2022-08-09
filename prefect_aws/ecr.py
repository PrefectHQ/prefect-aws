import base64
from datetime import datetime
from typing import Optional, Tuple

from prefect.infrastructure.docker import BaseDockerLogin
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import PrivateAttr

from prefect_aws import AwsCredentials


class ElasticContainerRegistry(BaseDockerLogin):

    """
    Block to login to ECR registries.
    """

    _block_type_name = "ECR Registry"

    registry_id: Optional[str]
    aws_credentials: AwsCredentials

    _cached_token: bytes = PrivateAttr(default=None)
    _registry_url: str = PrivateAttr(default=None)
    _token_expiration: datetime = PrivateAttr(default=None)

    @sync_compatible
    async def login(self):
        return await run_sync_in_worker_thread(self._login_sync)

    def _login_sync(self):
        token, registry_url = self._get_token_and_endpoint()
        username, password = self._parse_token(token)
        # Use the base implementation to perform login
        return self._login(
            username=username, password=password, registry_url=registry_url, reauth=True
        )

    def _parse_token(self, token: str) -> Tuple[str, str]:
        """
        Parse a base64 encoded token in format username:password into parts
        """
        decoded_token: str = base64.decodebytes(token).decode()
        username, password = decoded_token.split(sep=":", maxsplit=1)
        return username, password

    def _get_client(self):
        return self.aws_credentials.get_boto3_session().client(service_name="ecr")

    def _get_token_and_endpoint(self):
        if (
            self._cached_token
            and datetime.now(tz=self._token_expiration.tzinfo) < self._token_expiration
        ):
            # Return the cached token if it has not expired
            return self._cached_token, self._registry_url

        client = self._get_client()

        registry_id = self.registry_id or client.describe_registry()["registryId"]

        result = client.get_authorization_token(registryIds=[registry_id])[
            "authorizationData"
        ][0]

        token = self._cached_token = result["authorizationToken"].encode()
        registry_url = self._registry_url = result["proxyEndpoint"]
        self._token_expiration = result["expiresAt"]

        return token, registry_url
