import inspect
from dataclasses import asdict
from functools import wraps
from threading import Lock
from typing import (
    Any,
    Callable,
    List,
    Literal,
    MutableMapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from weakref import WeakValueDictionary

import boto3
from prefect.flows import Flow, flow
from prefect.tasks import Task, task
from typing_extensions import ParamSpec, Protocol

from prefect_aws.exceptions import MissingRequiredArgument
from prefect_aws.schema import BaseDefaultValues, FlowArgs, TaskArgs

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


def get_boto3_client(
    resource: Literal["s3"],
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region_name: Optional[str] = None,
    profile_name: Optional[str] = None,
    **kwargs: Any,
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
        - aws_access_key_id (str, optional): AWS access key ID for client
            authentication.
        - aws_secret_access_key (str, optional): AWS secret access key for client
            authentication.
        - aws_session_token (str, optional): AWS session token used for client
            authentication.
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


R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the user's function
D = TypeVar(
    "D", bound=BaseDefaultValues, contravariant=True
)  # The default values for a factory


def verify_required_args_present(*arg_names: str):
    """
    Higher order function that verifies that the specified arguments are not `None`.

    Args:
        args_names: Names of required arguments which will be verified as present

    Returns:
        Wrapper function

    Raises:
        MissingRequiredArgument when a required argument is `None`
    """

    def wrapper(__func: Callable[P, R]) -> Callable[P, R]:
        @wraps(__func)
        def method(*args, **kwargs) -> R:
            bound_signature = inspect.signature(__func).bind(*args, **kwargs)
            bound_signature.apply_defaults()
            passed_args = dict(bound_signature.arguments)
            for required_arg_name in arg_names:
                if passed_args.get(required_arg_name) is None:
                    raise MissingRequiredArgument(required_arg_name)
            return __func(*args, **kwargs)

        return method

    return wrapper


def supply_args_defaults(
    default_values: BaseDefaultValues,
):
    """
    Higher order function that supplies default values to the wrapped function. If an
    argument is `None`, then the value for the corresponding argument in
    `default_values` will be passed to the wrapped function.

    Args:
        default_values: Dataclass holding the default values that should be used for
            arguments that are `None`

    Returns:
        Wrapper function
    """

    def wrapper(__func: Callable[P, R]) -> Callable[P, R]:
        @wraps(__func)
        def method(*args, **kwargs) -> R:
            # Inspect and bind the signature of wrapped function
            signature = inspect.signature(__func)
            bound_signature = signature.bind(*args, **kwargs)
            bound_signature.apply_defaults()
            passed_args = dict(bound_signature.arguments)

            new_positional_or_keyword_args = []
            new_kwargs = {}
            new_args = ()
            # Inspect each parameter along with the passed value for that parameter to
            # determine if a default value needs to be provided.
            # Maintain kind of each parameter to avoid runtime errors.
            for parameter in signature.parameters.values():
                if (
                    parameter.kind == parameter.POSITIONAL_OR_KEYWORD
                    or parameter.kind == parameter.POSITIONAL_ONLY
                ):
                    passed_value = passed_args.get(parameter.name)
                    new_value = (
                        passed_value
                        if passed_value is not None
                        else getattr(default_values, str(parameter.name))
                    )
                    new_positional_or_keyword_args.append(new_value)
                if parameter.kind == parameter.KEYWORD_ONLY:
                    passed_value = passed_args.get(parameter.name)
                    new_kwargs[parameter.name] = (
                        passed_value
                        if passed_value is not None
                        else getattr(default_values, str(parameter.name))
                    )
                elif parameter.kind == parameter.VAR_POSITIONAL:
                    new_args = passed_args.get(parameter.name, ())
                elif parameter.kind == parameter.VAR_KEYWORD:
                    new_kwargs = {**new_kwargs, **passed_args.get(parameter.name, {})}

            # Call function with newly determined arguments
            return __func(*new_positional_or_keyword_args, *new_args, **new_kwargs)

        return method

    return wrapper


def _chain(start: Any, *args: Callable):
    """
    Chain multiple functions together and invoke the functional chain with a starting
    value
    """
    res = start
    for func in args:
        res = func(res)
    return res


class TaskFactory(Protocol[P, R, D]):
    """
    Protocol class for task factory typing
    """

    def __call__(
        self,
        default_values: Optional[D] = None,
        task_args: Optional[TaskArgs] = None,
    ) -> Task[P, R]:
        ...


def task_factory(default_values_cls: Type[D], required_args: List[str]):
    """
    Decorator function that turns the decorated function into a task factory. Requires a
    corresponding dataclass used to hold the default values for a constructed task.
    Tasks created from a task factory can have default values assigned to their
    parameters that are then overrideable upon invocation. Tasks created from a task
    factory will also validate that all necessary arguments are present at invocation.

    Args:
        default_values_cls: Class used to populate default values for created tasks.
        required_args: List of arguments that are necessary for created tasks to be run.
    Returns:
        A task factory that accepts `default_values` and `task_args`.
    """

    def wrapper(__func: Callable[P, R]) -> TaskFactory[P, R, D]:
        @wraps(__func)
        def factory_func(
            default_values: Optional[D] = None,
            task_args: Optional[TaskArgs] = None,
        ) -> Task[P, R]:
            """
            Function that wraps the user supplied function with functions that will
            populate the configured defaults upon invocation, verify that the required
            arguments are present at invocation, and turn the function into
            a task that can be invoked within a flow.

            Args:
                default_values: An instance of dataclass that holds the default values
                    supplied to the created task.
                task_args: Task configuration that is passed to the Prefect Task
                    constructor.
            Returns:
                A task that can be invoked within a flow.
            """
            default_values = default_values or default_values_cls()
            task_args = task_args or TaskArgs()

            # chain higher order functions to construct and validate a task with
            # default values
            return _chain(
                __func,
                verify_required_args_present(*required_args),
                supply_args_defaults(default_values),
                task(**asdict(task_args)),
            )

        return factory_func

    return wrapper


class FlowFactory(Protocol[P, R, D]):
    """
    Protocol class for flow factory typing
    """

    def __call__(
        self,
        default_values: Optional[D] = None,
        flow_args: Optional[FlowArgs] = None,
    ) -> Flow[P, R]:
        ...


def flow_factory(default_values_cls: Type[D], required_args: List[str]):
    """
    Decorator function that turns the decorated function into a flow factory. Requires a
    corresponding dataclass used to hold the default values for a constructed flow.
    Flows created from a flow factory can have default values assigned to their
    parameters that are then overrideable upon invocation. Flows created from a flow
    factory will also validate that all necessary arguments are present at invocation.

    Args:
        default_values_cls: Class used to populate default values for created flows.
        required_args: List of arguments that are necessary for created flows to be run.
    Returns:
        A flow factory that accepts `default_values` and `flow_args`.
    """

    def wrapper(__func: Callable[P, R]) -> FlowFactory[P, R, D]:
        @wraps(__func)
        def factory_func(
            default_values: Optional[BaseDefaultValues] = None,
            flow_args: Optional[FlowArgs] = None,
        ) -> Flow[P, R]:
            """
            Function that wraps the user supplied function with functions that will
            populate the configured defaults upon invocation, verify that the required
            arguments are present at invocation, and turn the function into
            a flow that can be invoked on its own or as subflow within another flow.

            Args:
                default_values: An instance of dataclass that holds the default values
                    supplied to the created flow.
                flow_args: Flow configuration that is passed to the Prefect Flow
                    constructor.
            Returns:
                A flow that can be invoked on its own or as a subflow within another flow.
            """

            default_values = default_values or default_values_cls()
            flow_args = flow_args or FlowArgs()

            # chain higher order functions to construct and validate a flow with
            # default values
            return _chain(
                __func,
                verify_required_args_present(*required_args),
                supply_args_defaults(default_values),
                flow(**asdict(flow_args)),
            )

        return factory_func

    return wrapper
