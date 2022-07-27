import io

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_s3
from prefect import flow
from pytest_lazyfixture import lazy_fixture

from prefect_aws.client_parameters import AwsClientParameters
from prefect_aws.s3 import s3_download, s3_list_objects, s3_upload

aws_clients = [
    (lazy_fixture("aws_client_parameters_custom_endpoint")),
    (lazy_fixture("aws_client_parameters_empty")),
]


@pytest.fixture
def s3_mock(monkeypatch, client_parameters):
    if client_parameters.endpoint_url:
        monkeypatch.setenv("MOTO_S3_CUSTOM_ENDPOINTS", client_parameters.endpoint_url)
    with mock_s3():
        yield


@pytest.fixture
def client_parameters(request):
    client_parameters = request.param
    return client_parameters


@pytest.fixture
def bucket(s3_mock):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("bucket")
    bucket.create()
    return bucket


@pytest.fixture
def object(bucket, tmp_path):
    file = tmp_path / "object.txt"
    file.write_text("TEST")
    with open(file, "rb") as f:
        return bucket.upload_fileobj(f, "object")


@pytest.fixture
def object_in_folder(bucket, tmp_path):
    file = tmp_path / "object_in_folder.txt"
    file.write_text("TEST OBJECT IN FOLDER")
    with open(file, "rb") as f:
        return bucket.upload_fileobj(f, "folder/object")


@pytest.fixture
def a_lot_of_objects(bucket, tmp_path):
    objects = []
    for i in range(0, 20):
        file = tmp_path / f"object{i}.txt"
        file.write_text("TEST")
        with open(file, "rb") as f:
            objects.append(bucket.upload_fileobj(f, f"object{i}"))
    return objects


@pytest.mark.parametrize(
    "client_parameters",
    [lazy_fixture("aws_client_parameters_custom_endpoint")],
    indirect=True,
)
async def test_s3_download_failed_with_wrong_endpoint_setup(
    object, client_parameters, aws_credentials
):
    client_parameters_wrong_endpoint = AwsClientParameters(
        endpoint_url="http://something"
    )

    @flow
    async def test_flow():
        return await s3_download(
            bucket="bucket",
            key="object",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters_wrong_endpoint,
        )

    with pytest.raises(EndpointConnectionError):
        await test_flow()


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_download(object, client_parameters, aws_credentials):
    @flow
    async def test_flow():
        return await s3_download(
            bucket="bucket",
            key="object",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
        )

    result = await test_flow()
    assert result == b"TEST"


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_download_object_not_found(object, client_parameters, aws_credentials):
    @flow
    async def test_flow():
        return await s3_download(
            key="unknown_object",
            bucket="bucket",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
        )

    with pytest.raises(ClientError):
        await test_flow()


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_upload(bucket, client_parameters, tmp_path, aws_credentials):
    @flow
    async def test_flow():
        test_file = tmp_path / "test.txt"
        test_file.write_text("NEW OBJECT")
        with open(test_file, "rb") as f:
            return await s3_upload(
                data=f.read(),
                bucket="bucket",
                key="new_object",
                aws_credentials=aws_credentials,
                aws_client_parameters=client_parameters,
            )

    await test_flow()

    stream = io.BytesIO()
    bucket.download_fileobj("new_object", stream)
    stream.seek(0)
    output = stream.read()

    assert output == b"NEW OBJECT"


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_list_objects(
    object, client_parameters, object_in_folder, aws_credentials
):
    @flow
    async def test_flow():
        return await s3_list_objects(
            bucket="bucket",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
        )

    objects = await test_flow()
    assert len(objects) == 2
    assert [object["Key"] for object in objects] == ["folder/object", "object"]


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_list_objects_multiple_pages(
    a_lot_of_objects, client_parameters, aws_credentials
):
    @flow
    async def test_flow():
        return await s3_list_objects(
            bucket="bucket",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
            page_size=2,
        )

    objects = await test_flow()
    assert len(objects) == 20
    assert sorted([object["Key"] for object in objects]) == sorted(
        [f"object{i}" for i in range(0, 20)]
    )


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_list_objects_prefix(
    object, client_parameters, object_in_folder, aws_credentials
):
    @flow
    async def test_flow():
        return await s3_list_objects(
            bucket="bucket",
            prefix="folder",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
        )

    objects = await test_flow()
    assert len(objects) == 1
    assert [object["Key"] for object in objects] == ["folder/object"]


@pytest.mark.parametrize("client_parameters", aws_clients, indirect=True)
async def test_s3_list_objects_filter(
    object, client_parameters, object_in_folder, aws_credentials
):
    @flow
    async def test_flow():
        return await s3_list_objects(
            bucket="bucket",
            jmespath_query="Contents[?Size > `10`][]",
            aws_credentials=aws_credentials,
            aws_client_parameters=client_parameters,
        )

    objects = await test_flow()
    assert len(objects) == 1
    assert [object["Key"] for object in objects] == ["folder/object"]
