"""Module handling Client parameters"""

from typing import Any, Dict, Optional, Union, List

from botocore.client import Config as BotoConfig

from prefect.blocks.core import Block
from pydantic import validator, Field

class AwsClientParameters(Block):
    """
    Dataclass used to manage extra parameters that you can pass when you initialize the Client. If you
    want to find more information, see
    [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    for more info about the possible client configurations.

    Args:
        api_version: The API version to use. By default, botocore will
            use the latest API version when creating a client. You only need
            to specify this parameter if you want to use a previous API version
            of the client.

        use_ssl: Whether or not to use SSL. By default, SSL is used.
            Note that not all services support non-ssl connections.

        verify: Whether or not to verify SSL certificates. By default
            SSL certificates are verified. You can provide the following values:

            * False - do not validate SSL certificates. SSL will still be
              used (unless use_ssl is False), but SSL certificates
              will not be verified.
            * path/to/cert/bundle.pem - A filename of the CA cert bundle to
              uses.  You can specify this argument if you want to use a
              different CA cert bundle than the one used by botocore.

        endpoint_url: The complete URL to use for the constructed
            client. Normally, botocore will automatically construct the
            appropriate URL to use when communicating with a service. You
            can specify a complete URL (including the "http/https" scheme)
            to override this behavior. If this value is provided,
            then ``use_ssl`` is ignored.

        config: Advanced configuration for Botocore clients. See
            [botocore docs](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html)
            for more details.
    """  # noqa E501

    api_version: Optional[str] = Field(default=None, description="")
    use_ssl: Optional[bool] = Field(default=None, description="")
    verify: Optional[Union[bool, str]] = Field(default=None, description="")
    endpoint_url: Optional[str] = Field(default=None, description="")
    config: Optional[BotoConfig] = Field(
        default_factory=BotoConfig,
        description="Advanced configuration for Botocore clients"
    )

    class Config:
        # Support serialization of the 'BotoConfig' type
        arbitrary_types_allowed = True
        json_encoders = {BotoConfig: lambda c: c.__dict__}

    def dict(self, *args, **kwargs) -> Dict:
        # Support serialization of the 'BotoConfig' type
        d = super().dict(*args, **kwargs)
        d["config"] = self.config.__dict__
        return d

    @validator("config", pre=True)
    def _cast_config_to_boto_config(
        cls, value: Union[Dict[str, Any], BotoConfig]
    ) -> BotoConfig:
        if isinstance(value, dict):
            return BotoConfig(value)
        return value

    def get_params_override(self) -> Dict[str, Any]:
        """
        Return the dictionary of the parameters to override. The parameters to override are the one which are not None.
        """  # noqa E501
        return {k: v for k, v in self.dict().items() if v is not None}
