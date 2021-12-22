from functools import wraps
from threading import Lock
from typing import Any, Callable, Optional
from weakref import WeakValueDictionary

import boto3

_CLIENT_CACHE = WeakValueDictionary()
_LOCK = Lock()

def get_boto_client(
    resource: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region_name: Optional[str] = None,
    profile_name: Optional[str] = None,
    **kwargs
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

        if profile_name or botocore_session:
            session = boto3.session.Session(
                profile_name=profile_name,
                region_name=region_name,
                botocore_session=botocore_session,
            )
        else:
            session = boto3

        client = session.client(
            resource,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            **kwargs,
        )
        # Cache the client if no extra kwargs
        if not kwargs:
            _CLIENT_CACHE[cache_key] = client

    return client


def defaults_from_attrs(*attr_args: str) -> Callable:
    """
    Helper decorator for dealing with Task classes with attributes that serve
    as defaults for `Task.run`.  Specifically, this decorator allows the author
    of a Task to identify certain keyword arguments to the run method which
    will fall back to `self.ATTR_NAME` if not explicitly provided to
    `self.run`.  This pattern allows users to create a Task "template", whose
    default settings can be created at initialization, but overridden in
    individual instances when the Task is called.
    Args:
        - *attr_args (str): a splatted list of strings specifying which
            kwargs should fallback to attributes, if not provided at runtime. Note that
            the strings provided here must match keyword arguments in the `run` call signature,
            as well as the names of attributes of this Task.
    Returns:
        - Callable: the decorated / altered `Task.run` method
    Example:
    ```python
    from prefect.tasks import Task

    class MyTask(Task):
        def __init__(self, a=None, b=None):
            self.a = a
            self.b = b
        @defaults_from_attrs('a', 'b')
        def run(self, a=None, b=None):
            return a, b

    task = MyTask(a=1, b=2)
    task.run() # (1, 2)
    task.run(a=99) # (99, 2)
    task.run(a=None, b=None) # (None, None)
    ```
    """

    def wrapper(run_method: Callable) -> Callable:
        @wraps(run_method)
        def method(self: Any, *args: Any, **kwargs: Any) -> Any:
            for attr in attr_args:
                kwargs.setdefault(attr, getattr(self, attr))
            return run_method(self, *args, **kwargs)

        return method

    return wrapper
