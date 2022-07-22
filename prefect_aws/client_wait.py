"""Task for waiting on a long-running AWS job"""

import json

try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

from functools import partial
from typing import Any, Dict, Optional

import boto3
from anyio import to_thread
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel, create_waiter_with_client,Waiter
from prefect import get_run_logger, task

from prefect_aws import waiters
from prefect_aws.credentials import AwsCredentials


@task
async def client_waiter(
    client: str,
    waiter_name: str,
    aws_credentials: AwsCredentials,
    waiter_definition: Optional[Dict[str, Any]] = None,
    **waiter_kwargs: Optional[Dict[str, Any]],
):
    """
    Uses the underlying boto3 waiter functionality.

    Args:
        client: The AWS client on which to wait (e.g., 'client_wait', 'ec2', etc).
        waiter_name: The name of the waiter to instantiate. Can be a boto-supported
            waiter or one of prefect's custom waiters. Currently, Prefect offers three additional
            waiters for AWS client_wait: `"JobExists"` waits for a job to be instantiated, `"JobRunning"`
            waits for a job to start running, and `"JobComplete"` waits for a job to finish. You can
            find the definitions for all prefect-defined waiters [here](
            https://github.com/PrefectHQ/prefect-aws/tree/master/prefect_aws/waiters/).
            You may also use a custom waiter name, if you supply an accompanying waiter definition
            dict.
        credentials: your AWS credentials passed from an upstream
            Secret task; this Secret must be a JSON string
            with two keys: `ACCESS_KEY` and `SECRET_ACCESS_KEY` which will be
            passed directly to `boto3`.  If not provided here or in context, `boto3`
            will fall back on standard AWS rules for authentication.
        waiter_definition: A valid custom waiter model, as a dict. Note that if
            you supply a custom definition, it is assumed that the provided 'waiter_name' is
            contained within the waiter definition dict.
        waiter_kwargs: Arguments to pass to the `waiter.wait(...)` method. Will
            depend upon the specific waiter being called.

    Example:
        Run an ec2 waiter until instance_exists.
        ```python
        from prefect import flow
        from prefect_aws import AwsCredentials
        from prefect_aws.client_wait import client_waiter

        @flow
        def example_client_wait_flow():
            aws_credentials = AwsCredentials(
                aws_access_key_id="acccess_key_id",
                aws_secret_access_key="secret_access_key"
            )

            waiter = client_waiter(
                "ec2",
                "instance_exists",
                aws_credentials
            )

            return waiter 
        example_client_wait_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Waiting on %s job", client)

    boto_client = aws_credentials.get_boto3_session().client(client)

    if waiter_definition:
        # Use user-provided waiter definition
        waiter_model = WaiterModel(waiter_definition)
        waiter = create_waiter_with_client(waiter_name, waiter_model, boto_client)
    else:
        # Use either boto-provided or prefect-provided waiter
        if waiter_name in boto_client.waiter_names:
            waiter = boto_client.get_waiter(waiter_name)
        else:
            waiter = _load_prefect_waiter(boto_client, client, waiter_name)

    partial_wait = partial(waiter.wait, **waiter_kwargs)
    await to_thread.run_sync(partial_wait)


def _load_prefect_waiter(
        boto_client: boto3.client, 
        client_str: str, 
        waiter_name: str,
    ) -> Waiter:
    
    """
    Load a custom waiter from the ./waiters directory.
    """
    # Instantiate waiter from accompanying client json file
    with pkg_resources.open_text(waiters, f"{client_str}.json") as handle:
        waiter_model = WaiterModel(json.load(handle))
    return create_waiter_with_client(waiter_name, waiter_model, boto_client)
 