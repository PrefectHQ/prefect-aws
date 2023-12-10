import io
import json
import zipfile

import boto3
import pytest
from moto import mock_iam, mock_lambda
from pytest_lazyfixture import lazy_fixture

from prefect_aws.credentials import AwsCredentials
from prefect_aws.lambda_function import LambdaFunction


@pytest.fixture
def lambda_mock(aws_credentials: AwsCredentials):
    with mock_lambda():
        yield boto3.client(
            "lambda",
            region_name=aws_credentials.region_name,
        )


@pytest.fixture
def iam_mock(aws_credentials: AwsCredentials):
    with mock_iam():
        yield boto3.client(
            "iam",
            region_name=aws_credentials.region_name,
        )


@pytest.fixture
def mock_iam_rule(iam_mock):
    yield iam_mock.create_role(
        RoleName="test-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )


LAMBDA_TEST_CODE = """
def handler(event, context):
    if isinstance(event, dict):
        if "error" in event:
            raise Exception(event["error"])
        event["foo"] = "bar"
    else:
        event = {"foo": "bar"}
    return event
"""


@pytest.fixture
def mock_lambda_code():
    with io.BytesIO() as f:
        with zipfile.ZipFile(f, mode="w") as z:
            z.writestr("foo.py", LAMBDA_TEST_CODE)
        f.seek(0)
        yield f.read()


@pytest.fixture
def mock_lambda_function(lambda_mock, mock_iam_rule, mock_lambda_code):
    r = lambda_mock.create_function(
        FunctionName="test-function",
        Runtime="python3.10",
        Role=mock_iam_rule["Role"]["Arn"],
        Handler="foo.handler",
        Code={"ZipFile": mock_lambda_code},
    )
    r2 = lambda_mock.publish_version(
        FunctionName="test-function",
    )
    r["Version"] = r2["Version"]
    yield r


LAMBDA_TEST_CODE_V2 = """
def handler(event, context):
    event = {"data": [1, 2, 3]}
    return event
"""


@pytest.fixture
def mock_lambda_code_v2():
    with io.BytesIO() as f:
        with zipfile.ZipFile(f, mode="w") as z:
            z.writestr("foo.py", LAMBDA_TEST_CODE_V2)
        f.seek(0)
        yield f.read()


@pytest.fixture
def add_lambda_version(mock_lambda_function, lambda_mock, mock_lambda_code_v2):
    r = mock_lambda_function.copy()
    lambda_mock.update_function_code(
        FunctionName="test-function",
        ZipFile=mock_lambda_code_v2,
    )
    r2 = lambda_mock.publish_version(
        FunctionName="test-function",
    )
    r["Version"] = r2["Version"]
    yield r


@pytest.fixture
def lambda_function(aws_credentials):
    return LambdaFunction(
        function_name="test-function",
        aws_credentials=aws_credentials,
    )


class TestLambdaFunction:
    def test_init(self, aws_credentials):
        function = LambdaFunction(
            function_name="test-function",
            aws_credentials=aws_credentials,
        )
        assert function.function_name == "test-function"
        assert function.qualifier is None

    @pytest.mark.parametrize(
        "payload,expected",
        [
            ({"foo": "baz"}, {"foo": "bar"}),
            (None, {"foo": "bar"}),
        ],
    )
    def test_invoke_lambda_payloads(
        self,
        payload: dict | None,
        expected: dict,
        mock_lambda_function,
        lambda_function: LambdaFunction,
    ):
        result = lambda_function.invoke(payload)
        assert result["StatusCode"] == 200
        response_payload = json.loads(result["Payload"].read())
        assert response_payload == expected

    def test_invoke_lambda_tail(
        self, lambda_function: LambdaFunction, mock_lambda_function
    ):
        result = lambda_function.invoke(tail=True)
        assert result["StatusCode"] == 200
        response_payload = json.loads(result["Payload"].read())
        assert response_payload == {"foo": "bar"}
        assert "LogResult" in result

    def test_invoke_lambda_client_context(
        self, lambda_function: LambdaFunction, mock_lambda_function
    ):
        # Just making sure boto doesn't throw an error
        result = lambda_function.invoke(client_context={"bar": "foo"})
        assert result["StatusCode"] == 200
        response_payload = json.loads(result["Payload"].read())
        assert response_payload == {"foo": "bar"}

    @pytest.mark.parametrize(
        "func_fixture,expected",
        [
            (lazy_fixture("mock_lambda_function"), {"foo": "bar"}),
            (lazy_fixture("add_lambda_version"), {"data": [1, 2, 3]}),
        ],
    )
    def test_invoke_lambda_qualifier(
        self, func_fixture, expected, lambda_function: LambdaFunction
    ):
        try:
            lambda_function.qualifier = func_fixture["Version"]
            result = lambda_function.invoke()
            assert result["StatusCode"] == 200
            response_payload = json.loads(result["Payload"].read())
            assert response_payload == expected
        finally:
            lambda_function.qualifier = None
