import io

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3
from prefect import flow

from prefect_aws.credentials import AwsCredentials
from prefect_aws.s3 import s3_download, s3_list_objects, s3_upload


@pytest.fixture
def s3_mock():
    with mock_s3():
        yield


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
def aws_credentials():
    return AwsCredentials(
        aws_access_key_id="access_key_id", aws_secret_access_key="secret_access_key"
    )


def test_s3_download(object, aws_credentials):
    @flow
    def test_flow():
        return s3_download(
            bucket="bucket", key="object", aws_credentials=aws_credentials
        )

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == b"TEST"


def test_s3_download_object_not_found(object, aws_credentials):
    @flow
    def test_flow():
        return s3_download(
            key="unknown_object", bucket="bucket", aws_credentials=aws_credentials
        )

    flow_state = test_flow()
    with pytest.raises(ClientError):
        flow_state.result()


def test_s3_upload(bucket, tmp_path, aws_credentials):
    @flow
    def test_flow():
        test_file = tmp_path / "test.txt"
        test_file.write_text("NEW OBJECT")
        with open(test_file, "rb") as f:
            return s3_upload(
                data=f.read(),
                bucket="bucket",
                key="new_object",
                aws_credentials=aws_credentials,
            )

    flow_state = test_flow()
    assert flow_state.is_completed

    stream = io.BytesIO()
    bucket.download_fileobj("new_object", stream)
    stream.seek(0)
    output = stream.read()

    assert output == b"NEW OBJECT"


def test_s3_list_objects(object, object_in_folder, aws_credentials):
    @flow
    def test_flow():
        return s3_list_objects(bucket="bucket", aws_credentials=aws_credentials)

    flow_state = test_flow()
    task_state = flow_state.result()
    objects = task_state.result()
    assert len(objects) == 2
    assert [object["Key"] for object in objects] == ["folder/object", "object"]


def test_s3_list_objects_prefix(object, object_in_folder, aws_credentials):
    @flow
    def test_flow():
        return s3_list_objects(
            bucket="bucket", prefix="folder", aws_credentials=aws_credentials
        )

    flow_state = test_flow()
    task_state = flow_state.result()
    objects = task_state.result()
    assert len(objects) == 1
    assert [object["Key"] for object in objects] == ["folder/object"]


def test_s3_list_objects_filter(object, object_in_folder, aws_credentials):
    @flow
    def test_flow():
        return s3_list_objects(
            bucket="bucket",
            jmespath_query="Contents[?Size > `10`][]",
            aws_credentials=aws_credentials,
        )

    flow_state = test_flow()
    task_state = flow_state.result()
    objects = task_state.result()
    assert len(objects) == 1
    assert [object["Key"] for object in objects] == ["folder/object"]
