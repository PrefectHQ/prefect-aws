from typing import Any, Dict

import pytest

from prefect_aws.client_parameters import AwsClientParameters


@pytest.mark.parametrize(
    "params,result",
    [
        (AwsClientParameters(), {}),
        (
            AwsClientParameters(
                use_ssl=False, verify=False, endpoint_url="http://localhost:9000"
            ),
            {
                "use_ssl": False,
                "verify": False,
                "endpoint_url": "http://localhost:9000",
            },
        ),
        (
            AwsClientParameters(
                verify="/cert/ca_bundle.pem", endpoint_url="https://localhost:9000"
            ),
            {"verify": "/cert/ca_bundle.pem", "endpoint_url": "https://localhost:9000"},
        ),
        (
            AwsClientParameters(api_version="1.0.0"),
            {"api_version": "1.0.0"},
        ),
        # would need to implement a custom equality function or class matcher,
        # but this is too trivial of a test
        # (
        #     AwsClientParameters(config=botocore.client.Config(
        #         region_name="eu_west_1",
        #         retries={"max_attempts": 10, "mode": "standard"}
        #     )),
        #     {
        #         "config": {
        #             "region_name": "eu_west_1",
        #             "retries": {"max_attempts": 10, "mode": "standard"},
        #         }
        #     },
        # ),
    ],
)
def test_empty_AwsClientParameter_return_empty_dict(
    params: AwsClientParameters, result: Dict[str, Any]
):
    assert result == params.get_params_override()
