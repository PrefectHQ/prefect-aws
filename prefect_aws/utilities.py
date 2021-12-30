from threading import Lock
from typing import (
    Any,
    Callable,
    List,
    Literal,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)
from weakref import WeakValueDictionary
from functools import partial, wraps
import inspect

import boto3

from prefect_aws.exceptions import MissingRequiredArgument

_CLIENT_CACHE: MutableMapping[
    Tuple[
        Union[str, None],
        Union[str, None],
        int,
        Literal["s3"],
        Union[str, None],
        Union[str, None],
        Union[str, None],
    ],
    Any,
] = WeakValueDictionary()
_LOCK = Lock()


def get_boto_client(
    resource: Literal["s3"],
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region_name: Optional[str] = None,
    profile_name: Optional[str] = None,
    **kwargs: Any
):
    """
    Utility function for loading boto3 client objects from a given set of credentials.
    If aws_secret_access_key and aws_session_token are not provided, authentication will
    be attempted via other methods defined in the [boto3 documentation]
    (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

    Note: this utility is threadsafe, and will cache _active_ clients to be
    potentially reused by other concurrent callers, reducing the overhead of
    client creation.

    Args:
        - resource (str): the name of the resource to retrieve a client for (e.g. s3)
        - aws_access_key_id (str, optional): AWS access key ID for client authentication.
        - aws_secret_access_key (str, optional): AWS secret access key for client authentication.
        - aws_session_token (str, optional): AWS session token used for client authentication.
        - region_name (str, optional): The AWS region name to use, defaults to your
            global configured default.
        - profile_name (str, optional): The name of an AWS profile to use which is
            configured in the AWS shared credentials file (e.g. ~/.aws/credentials)
        - **kwargs (Any, optional): additional keyword arguments to pass to boto3
    Returns:
        - Client: an initialized and authenticated boto3 Client
    """
    with _LOCK:
        botocore_session = kwargs.pop("botocore_session", None)
        cache_key = (
            profile_name,
            region_name,
            id(botocore_session),
            resource,
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
        )

        # If no extra kwargs and client already created, use the cached client
        if not kwargs and cache_key in _CLIENT_CACHE:
            return _CLIENT_CACHE[cache_key]

        session_args = dict(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            botocore_session=botocore_session,
            profile_name=profile_name,
        )

        client = boto3.Session(**session_args).client(service_name=resource, **kwargs)

        # Cache the client if no extra kwargs
        if not kwargs:
            _CLIENT_CACHE[cache_key] = client

    return client


def verify_required_args_present(
    __func: Optional[Callable] = None, *, arg_names: List[str]
):
    """
    Decorator that verifies that arguments of the decorated function defined by `arg_names` are not `None`

    Args:
        arg_names: List of names of arguments that need to be verified

    Raises:
        MissingRequiredArgument if any of the required arguments are `None`
    """
    if __func is None:
        return partial(verify_required_args_present, arg_names=arg_names)

    @wraps(__func)
    def wrapper(*args, **kwargs):
        bound_signature = inspect.signature(__func).bind(*args, **kwargs)
        bound_signature.apply_defaults()
        passed_args = dict(bound_signature.arguments)
        for required_arg_name in arg_names:
            if passed_args.get(required_arg_name) is None:
                raise MissingRequiredArgument(required_arg_name)
        return __func(*args, **kwargs)

    return wrapper
