"""Module handling Client parameters"""

import warnings
from typing import Any, Dict, Optional, Union

from botocore.client import Config
from pydantic import BaseModel, Field, FilePath, root_validator, validator


class AwsClientParameters(BaseModel):
    """
    Dataclass used to manage extra parameters that you can pass when you initialize
    the Client. If you want to find more information, see
    [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    for more info about the possible client configurations.

    Attributes:
        api_version: The API version to use. By default, botocore will
            use the latest API version when creating a client. You only need
            to specify this parameter if you want to use a previous API version
            of the client.
        use_ssl: Whether or not to use SSL. By default, SSL is used.
            Note that not all services support non-ssl connections.
        verify: Whether or not to verify SSL certificates. By default
            SSL certificates are verified. You can provide the following values:
            False - do not validate SSL certificates. SSL will still be
              used (unless use_ssl is False), but SSL certificates
              will not be verified.
        verify_cert_path: A filename of the CA cert bundle to
            uses. You can specify this argument if you want to use a
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

    api_version: Optional[str] = Field(
        default=None, description="The API version to use."
    )
    use_ssl: bool = Field(default=True, description="Whether or not to use SSL.")
    verify: bool = Field(
        default=True, description="Whether or not to verify SSL certificates."
    )
    verify_cert_path: Optional[FilePath] = Field(
        default=None,
        description="Path to the CA cert bundle to use.",
        title="Certificate Authority Bundle File Path",
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="The complete URL to use for the constructed client.",
        title="Endpoint URL",
    )
    config: Optional[Dict[str, Any]] = None

    @validator("config", pre=True)
    def instantiate_config(cls, value: Union[Config, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Casts lists to Config instances.
        """
        if isinstance(value, Config):
            return value.__dict__["_user_provided_options"]
        return value

    @root_validator(pre=True)
    def has_verify_cert_path_but_verify_is_false(cls, values) -> Dict[str, Any]:
        """
        If verify_cert_path is set but verify is False, raise a warning.
        """
        if values.get("verify_cert_path") and not values["verify"]:
            warnings.warn(
                "verify_cert_path is set but verify is False. "
                "verify_cert_path will be ignored."
            )
            values.pop("verify_cert_path")
        return values

    def get_params_override(self) -> Dict[str, Any]:
        """
        Return the dictionary of the parameters to override.
        The parameters to override are the one which are not None.
        """
        params_override = {}
        for key, value in self.dict().items():
            if value is None:
                continue
            elif key == "config":
                params_override[key] = Config(**value)
            elif key == "verify_cert_path":
                params_override["verify"] = value
                params_override.pop("verify_cert_path")
            params_override[key] = value
        return params_override
