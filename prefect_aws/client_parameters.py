"""Module handling Client parameters"""

import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from botocore.client import Config


@dataclass(frozen=True)
class AwsClientParameters:
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

    api_version: Optional[str] = None
    use_ssl: Optional[bool] = None
    verify: Optional[Union[bool, str]] = None
    endpoint_url: Optional[str] = None
    config: Optional[Config] = None

    def get_params_override(self) -> Dict[str, Any]:
        """
        Return the dictionary of the parameters to override. The parameters to override are the one which are not None.
        """  # noqa E501
        return {k: v for k, v in dataclasses.asdict(self).items() if v is not None}
