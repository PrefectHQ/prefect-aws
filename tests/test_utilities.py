# type: ignore
import os
from pathlib import Path
from dataclasses import dataclass

import pytest
from moto import mock_s3
from prefect_aws.exceptions import MissingRequiredArgument
from prefect_aws.schema import DefaultValues
from prefect_aws.utilities import (
    get_boto_client,
    supply_args_defaults,
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
    client1 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    client2 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )

    assert client1 is client2


def test_client_cache_different_credentials():
    client1 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key_1",
        aws_secret_access_key="secret_key_1",
    )

    client2 = get_boto_client(
        resource="s3",
        aws_access_key_id="access_key_2",
        aws_secret_access_key="secret_key_3",
    )

    assert client1 is not client2


def test_client_cache_same_profile(aws_credentials):
    client1 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    assert client1 is client2


def test_client_cache_different_profile(aws_credentials):
    client1 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_1")

    client2 = get_boto_client(resource="s3", profile_name="TEST_PROFILE_2")

    assert client1 is not client2


@mock_s3
def test_client_cache_with_kwargs(aws_credentials):
    client1 = get_boto_client(resource="s3", use_ssl=False)

    client2 = get_boto_client(resource="s3", use_ssl=False)

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


def test_supply_args_defaults():
    @dataclass
    class TestDefaultValues(DefaultValues):
        foo: str = None
        bar: str = None
        buzz: str = "buzz"

    @supply_args_defaults(TestDefaultValues(foo="foo"))
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


def test_supply_args_defaults_with_positional_only():
    @dataclass
    class TestDefaultValues(DefaultValues):
        foo: str = None
        bar: str = None
        buzz: str = "buzz"

    @supply_args_defaults(TestDefaultValues(foo="foo"))
    def test_function(
        foo: str = None, /, bar: str = None, buzz: str = None, *args, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    assert test_function("foo", "bar", "buzz", "other", fizz="fizz") == dict(
        foo="foo", bar="bar", buzz="buzz", args=("other",), kwargs={"fizz": "fizz"}
    )
    assert test_function(bar="bar") == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
    assert test_function("f", bar="ba", buzz="bu") == dict(
        foo="f", bar="ba", buzz="bu", args=(), kwargs={}
    )


def test_supply_args_defaults_with_keyword_only():
    @dataclass
    class TestDefaultValues(DefaultValues):
        foo: str = None
        bar: str = None
        buzz: str = "buzz"

    @supply_args_defaults(TestDefaultValues(foo="foo"))
    def test_function(
        foo: str = None, bar: str = None, *args, buzz: str = None, **kwargs
    ):
        return dict(foo=foo, bar=bar, buzz=buzz, args=args, kwargs=kwargs)

    assert test_function("foo", "bar", "buzz", "other", fizz="fizz") == dict(
        foo="foo", bar="bar", buzz="buzz", args=("other",), kwargs={"fizz": "fizz"}
    )
    assert test_function(bar="bar") == dict(
        foo="foo", bar="bar", buzz="buzz", args=(), kwargs={}
    )
    assert test_function("f", bar="ba", buzz="bu") == dict(
        foo="f", bar="ba", buzz="bu", args=(), kwargs={}
    )
