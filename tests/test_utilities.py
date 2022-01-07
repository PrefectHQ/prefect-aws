import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest
from moto import mock_s3
from prefect import flow
from prefect.flows import Flow
from prefect_aws.exceptions import MissingRequiredArgument
from prefect_aws.schema import FlowArgs, TaskArgs
from prefect_aws.utilities import (
    flow_factory,
    get_boto3_client,
    supply_args_defaults,
    task_factory,
    verify_required_args_present,
)


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    moto_credentials_file_path = (
        Path(__file__).parent.absolute() / "mock_aws_credentials"
    )
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = str(moto_credentials_file_path)


def test_client_cache_same_credentials():
    client1 = get_boto3_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    client2 = get_boto3_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    assert client1 is client2


def test_client_cache_different_credentials():
    client1 = get_boto3_client(
        resource="s3",
        aws_access_key_id="access_key_1",
        aws_secret_access_key="secret_key_1",
    )

    client2 = get_boto3_client(
        resource="s3",
        aws_access_key_id="access_key_2",
        aws_secret_access_key="secret_key_3",
    )

    assert client1 is not client2


def test_client_cache_same_profile(aws_credentials):
    client1 = get_boto3_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto3_client(resource="s3", profile_name="TEST_PROFILE_1")

    assert client1 is client2


def test_client_cache_different_profile(aws_credentials):
    client1 = get_boto3_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto3_client(resource="s3", profile_name="TEST_PROFILE_2")

    assert client1 is not client2


@mock_s3
def test_client_cache_with_kwargs(aws_credentials):
    client1 = get_boto3_client(resource="s3", use_ssl=False)

    client2 = get_boto3_client(resource="s3", use_ssl=False)

    assert client1 is not client2


def test_verify_required_args_are_present():
    @verify_required_args_present("foo", "bar")
    def test_function(foo: str = None, bar: str = None, buzz: str = None):
        return "required args present"

    assert test_function(foo="foo", bar="bar") == "required args present"

    with pytest.raises(
        MissingRequiredArgument, match="Missing value for required argument bar."
    ):
        test_function(foo="foo")


@dataclass
class DefaultValuesForTests:
    foo: Optional[str] = None
    bar: Optional[str] = None
    buzz: str = "buzz"


def test_supply_args_defaults():
    @supply_args_defaults(DefaultValuesForTests(foo="foo"))
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    assert test_function("foo", "bar", "buzz", "other", fizz="fizz") == dict(
        foo="foo", bar="bar", buzz="buzz", args=("other",), kwargs={"fizz": "fizz"}
    )
    assert test_function(bar="bar") == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
    assert test_function(foo="f", bar="ba", buzz="bu") == dict(
        foo="f", bar="ba", buzz="bu", args=(), kwargs={}
    )


def test_supply_args_defaults_with_keyword_only():
    @supply_args_defaults(DefaultValuesForTests(foo="foo"))
    def test_function(
        foo: str = None, bar: str = None, *args, buzz: str = None, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    assert test_function("foo", "bar", "buzz", "other", fizz="fizz") == dict(
        foo="foo",
        bar="bar",
        buzz="buzz",
        args=(
            "buzz",
            "other",
        ),
        kwargs={"fizz": "fizz"},
    )
    assert test_function(bar="bar") == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
    assert test_function("f", bar="ba", buzz="bu") == dict(
        foo="f", bar="ba", buzz="bu", args=(), kwargs={}
    )


def test_task_factory_populates_defaults():
    @task_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_task = test_function(
        default_values=DefaultValuesForTests(foo="foo", bar="bar")
    )

    @flow
    def test_flow():
        return test_task()

    test_flow_state = test_flow()
    task_state = test_flow_state.result()
    assert task_state.result() == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )


def test_task_factory_allows_override():
    @task_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_task = test_function(
        default_values=DefaultValuesForTests(foo="foo", bar="bar")
    )

    @flow
    def test_flow():
        return test_task(foo="new_foo")

    test_flow_state = test_flow()
    task_state = test_flow_state.result()
    assert task_state.result() == dict(
        foo="new_foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )


def test_task_factory_raises_on_missing_arg():
    @task_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_task = test_function(default_values=DefaultValuesForTests(foo="foo"))

    @flow
    def test_flow():
        return test_task()

    test_flow_state = test_flow()
    with pytest.raises(
        MissingRequiredArgument, match="Missing value for required argument bar."
    ):
        task_state = test_flow_state.result()


def test_task_factory_respects_task_args():
    call_count_tracker = MagicMock()

    @task_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        call_count_tracker()
        if call_count_tracker.call_count == 1:
            raise Exception()
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_task = test_function(
        task_args=TaskArgs(
            name="Test Task",
            description="This is a test task",
            tags=["test"],
            retries=1,
        )
    )

    @flow
    def test_flow():
        return test_task(foo="foo", bar="bar")

    assert test_task.name == "Test Task"
    assert test_task.description == "This is a test task"
    assert test_task.tags == {"test"}

    test_flow_state = test_flow()
    task_state = test_flow_state.result()

    assert task_state.result() == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
    assert call_count_tracker.call_count == 2


def test_flow_factory_populates_defaults():
    @flow_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_flow = test_function(
        default_values=DefaultValuesForTests(foo="foo", bar="bar")
    )

    assert isinstance(test_flow, Flow)

    test_flow_state = test_flow()
    assert test_flow_state.result() == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )


def test_flow_factory_allows_override():
    @flow_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_flow = test_function(
        default_values=DefaultValuesForTests(foo="foo", bar="bar")
    )

    test_flow_state = test_flow(foo="new_foo")
    assert test_flow_state.result() == dict(
        foo="new_foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )


def test_flow_factory_raises_on_missing_arg():
    @flow_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_flow = test_function(default_values=DefaultValuesForTests(foo="foo"))

    test_flow_state = test_flow()
    with pytest.raises(
        MissingRequiredArgument, match="Missing value for required argument bar."
    ):
        flow_state = test_flow_state.result()


def test_flow_factory_respects_flow_args():
    @flow_factory(
        default_values_cls=DefaultValuesForTests, required_args=["foo", "bar"]
    )
    def test_function(
        foo: str = None, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    test_flow = test_function(
        flow_args=FlowArgs(
            name="Test Flow",
            description="This is a test flow",
        )
    )

    assert test_flow.name == "Test Flow"
    assert test_flow.description == "This is a test flow"

    test_flow_state = test_flow(foo="foo", bar="bar")

    assert test_flow_state.result() == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
