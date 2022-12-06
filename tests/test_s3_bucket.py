from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3
from prefect.deployments import Deployment

from prefect_aws import AwsCredentials, MinIOCredentials
from prefect_aws.s3 import S3Bucket


@pytest.fixture
def aws_creds_block():
    return AwsCredentials(aws_access_key_id="testing", aws_secret_access_key="testing")


@pytest.fixture
def minio_creds_block():
    return MinIOCredentials(
        minio_root_user="minioadmin", minio_root_password="minioadmin"
    )


BUCKET_NAME = "test_bucket"


@pytest.fixture
def s3():

    """Mock connection to AWS S3 with boto3 client."""

    with mock_s3():

        yield boto3.client(
            service_name="s3",
            region_name="us-east-1",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )


@pytest.fixture
def nested_s3_bucket_structure(s3, s3_bucket, tmp_path: Path):
    """Creates an S3 bucket with multiple files in a nested structure"""

    file = tmp_path / "object.txt"
    file.write_text("TEST")

    s3.upload_file(str(file), BUCKET_NAME, "object.txt")
    s3.upload_file(str(file), BUCKET_NAME, "level1/object_level1.txt")
    s3.upload_file(str(file), BUCKET_NAME, "level1/level2/object_level2.txt")
    s3.upload_file(str(file), BUCKET_NAME, "level1/level2/object2_level2.txt")

    file.unlink()
    assert not file.exists()


@pytest.fixture(params=["aws_credentials", "minio_credentials"])
def s3_bucket(s3, request, aws_creds_block, minio_creds_block):

    key = request.param

    if key == "aws_credentials":
        fs = S3Bucket(bucket_name=BUCKET_NAME, aws_credentials=aws_creds_block)
    elif key == "minio_credentials":
        fs = S3Bucket(bucket_name=BUCKET_NAME, minio_credentials=minio_creds_block)

    s3.create_bucket(Bucket=BUCKET_NAME)

    return fs


@pytest.fixture
def s3_bucket_with_file(s3_bucket):
    key = s3_bucket.write_path("test.txt", content=b"hello")
    return s3_bucket, key


async def test_read_write_roundtrip(s3_bucket):

    """
    Create an S3 bucket, instantiate S3Bucket block, write to and read from
    bucket.
    """

    key = await s3_bucket.write_path("test.txt", content=b"hello")
    assert await s3_bucket.read_path(key) == b"hello"


async def test_write_with_missing_directory_succeeds(s3_bucket):

    """
    Create an S3 bucket, instantiate S3Bucket block, write to path with
    missing directory.
    """

    key = await s3_bucket.write_path("folder/test.txt", content=b"hello")
    assert await s3_bucket.read_path(key) == b"hello"


async def test_read_fails_does_not_exist(s3_bucket):

    """
    Create an S3 bucket, instantiate S3Bucket block, assert read from
    nonexistent path fails.
    """

    with pytest.raises(ClientError):
        await s3_bucket.read_path("test_bucket/foo/bar")


async def test_aws_basepath(s3_bucket, aws_creds_block):

    """Test the basepath functionality."""

    # create a new block with a subfolder
    s3_bucket_block = S3Bucket(
        bucket_name=BUCKET_NAME,
        aws_credentials=aws_creds_block,
        basepath="subfolder",
    )

    key = await s3_bucket_block.write_path("test.txt", content=b"hello")
    assert await s3_bucket_block.read_path("test.txt") == b"hello"
    assert key == "subfolder/test.txt"


async def test_get_directory(
    nested_s3_bucket_structure, s3_bucket: S3Bucket, tmp_path: Path
):
    await s3_bucket.get_directory(local_path=str(tmp_path))

    assert (tmp_path / "object.txt").exists()
    assert (tmp_path / "level1" / "object_level1.txt").exists()
    assert (tmp_path / "level1" / "level2" / "object_level2.txt").exists()
    assert (tmp_path / "level1" / "level2" / "object2_level2.txt").exists()


async def test_get_directory_respects_basepath(
    nested_s3_bucket_structure, s3_bucket: S3Bucket, tmp_path: Path, aws_creds_block
):
    s3_bucket_block = S3Bucket(
        bucket_name=BUCKET_NAME,
        aws_credentials=aws_creds_block,
        basepath="level1/level2",
    )

    await s3_bucket_block.get_directory(local_path=str(tmp_path))

    assert (len(list(tmp_path.glob("*")))) == 2
    assert (tmp_path / "object_level2.txt").exists()
    assert (tmp_path / "object2_level2.txt").exists()


async def test_get_directory_respects_from_path(
    nested_s3_bucket_structure, s3_bucket: S3Bucket, tmp_path: Path, aws_creds_block
):
    await s3_bucket.get_directory(local_path=str(tmp_path), from_path="level1")

    assert (tmp_path / "object_level1.txt").exists()
    assert (tmp_path / "level2" / "object_level2.txt").exists()
    assert (tmp_path / "level2" / "object2_level2.txt").exists()


async def test_put_directory(s3_bucket: S3Bucket, tmp_path: Path):
    (tmp_path / "file1.txt").write_text("FILE 1")
    (tmp_path / "file2.txt").write_text("FILE 2")
    (tmp_path / "folder1").mkdir()
    (tmp_path / "folder1" / "file3.txt").write_text("FILE 3")
    (tmp_path / "folder1" / "file4.txt").write_text("FILE 4")
    (tmp_path / "folder1" / "folder2").mkdir()
    (tmp_path / "folder1" / "folder2" / "file5.txt").write_text("FILE 5")

    uploaded_file_count = await s3_bucket.put_directory(local_path=str(tmp_path))
    assert uploaded_file_count == 5

    (tmp_path / "downloaded_files").mkdir()

    await s3_bucket.get_directory(local_path=str(tmp_path / "downloaded_files"))

    assert (tmp_path / "downloaded_files" / "file1.txt").exists()
    assert (tmp_path / "downloaded_files" / "file2.txt").exists()
    assert (tmp_path / "downloaded_files" / "folder1" / "file3.txt").exists()
    assert (tmp_path / "downloaded_files" / "folder1" / "file4.txt").exists()
    assert (
        tmp_path / "downloaded_files" / "folder1" / "folder2" / "file5.txt"
    ).exists()


async def test_put_directory_respects_basepath(
    s3_bucket: S3Bucket, tmp_path: Path, aws_creds_block
):
    (tmp_path / "file1.txt").write_text("FILE 1")
    (tmp_path / "file2.txt").write_text("FILE 2")
    (tmp_path / "folder1").mkdir()
    (tmp_path / "folder1" / "file3.txt").write_text("FILE 3")
    (tmp_path / "folder1" / "file4.txt").write_text("FILE 4")
    (tmp_path / "folder1" / "folder2").mkdir()
    (tmp_path / "folder1" / "folder2" / "file5.txt").write_text("FILE 5")

    s3_bucket_block = S3Bucket(
        bucket_name=BUCKET_NAME,
        aws_credentials=aws_creds_block,
        basepath="subfolder",
    )

    uploaded_file_count = await s3_bucket_block.put_directory(local_path=str(tmp_path))
    assert uploaded_file_count == 5

    (tmp_path / "downloaded_files").mkdir()

    await s3_bucket_block.get_directory(local_path=str(tmp_path / "downloaded_files"))

    assert (tmp_path / "downloaded_files" / "file1.txt").exists()
    assert (tmp_path / "downloaded_files" / "file2.txt").exists()
    assert (tmp_path / "downloaded_files" / "folder1" / "file3.txt").exists()
    assert (tmp_path / "downloaded_files" / "folder1" / "file4.txt").exists()
    assert (
        tmp_path / "downloaded_files" / "folder1" / "folder2" / "file5.txt"
    ).exists()


async def test_put_directory_with_ignore_file(
    s3_bucket: S3Bucket, tmp_path: Path, aws_creds_block
):
    (tmp_path / "file1.txt").write_text("FILE 1")
    (tmp_path / "file2.txt").write_text("FILE 2")
    (tmp_path / "folder1").mkdir()
    (tmp_path / "folder1" / "file3.txt").write_text("FILE 3")
    (tmp_path / "folder1" / "file4.txt").write_text("FILE 4")
    (tmp_path / "folder1" / "folder2").mkdir()
    (tmp_path / "folder1" / "folder2" / "file5.txt").write_text("FILE 5")
    (tmp_path / ".prefectignore").write_text("folder2/*")

    uploaded_file_count = await s3_bucket.put_directory(
        local_path=str(tmp_path / "folder1"),
        ignore_file=str(tmp_path / ".prefectignore"),
    )
    assert uploaded_file_count == 2

    (tmp_path / "downloaded_files").mkdir()

    await s3_bucket.get_directory(local_path=str(tmp_path / "downloaded_files"))

    assert (tmp_path / "downloaded_files" / "file3.txt").exists()
    assert (tmp_path / "downloaded_files" / "file4.txt").exists()
    assert not (tmp_path / "downloaded_files" / "folder2").exists()
    assert not (tmp_path / "downloaded_files" / "folder2" / "file5.txt").exists()


async def test_put_directory_respects_local_path(
    s3_bucket: S3Bucket, tmp_path: Path, aws_creds_block
):
    (tmp_path / "file1.txt").write_text("FILE 1")
    (tmp_path / "file2.txt").write_text("FILE 2")
    (tmp_path / "folder1").mkdir()
    (tmp_path / "folder1" / "file3.txt").write_text("FILE 3")
    (tmp_path / "folder1" / "file4.txt").write_text("FILE 4")
    (tmp_path / "folder1" / "folder2").mkdir()
    (tmp_path / "folder1" / "folder2" / "file5.txt").write_text("FILE 5")

    uploaded_file_count = await s3_bucket.put_directory(
        local_path=str(tmp_path / "folder1")
    )
    assert uploaded_file_count == 3

    (tmp_path / "downloaded_files").mkdir()

    await s3_bucket.get_directory(local_path=str(tmp_path / "downloaded_files"))

    assert (tmp_path / "downloaded_files" / "file3.txt").exists()
    assert (tmp_path / "downloaded_files" / "file4.txt").exists()
    assert (tmp_path / "downloaded_files" / "folder2" / "file5.txt").exists()


async def test_too_many_credentials_arguments(
    s3_bucket, aws_creds_block, minio_creds_block
):

    """Test providing too many credentials as input."""
    with pytest.raises(ValueError):
        # create a new block with a subfolder
        S3Bucket(
            bucket_name=BUCKET_NAME,
            aws_credentials=aws_creds_block,
            minio_credentials=minio_creds_block,
            basepath="subfolder",
        )


async def test_too_few_credentials_arguments(s3_bucket, aws_creds_block):

    """Test providing no credentials as input."""
    with pytest.raises(ValueError):
        # create a new block with a subfolder
        S3Bucket(
            bucket_name=BUCKET_NAME,
            basepath="subfolder",
        )


def test_read_path_in_sync_context(s3_bucket_with_file):
    """Test that read path works in a sync context."""
    s3_bucket, key = s3_bucket_with_file
    content = s3_bucket.read_path(key)
    assert content == b"hello"


def test_write_path_in_sync_context(s3_bucket):
    """Test that write path works in a sync context."""
    key = s3_bucket.write_path("test.txt", content=b"hello")
    content = s3_bucket.read_path(key)
    assert content == b"hello"


def test_deployment_default_basepath(s3_bucket):
    deployment = Deployment(name="testing", storage=s3_bucket)
    assert deployment.location == "/"


@pytest.mark.parametrize("type_", [str, Path])
def test_deployment_set_basepath(s3_bucket, type_):
    s3_bucket.basepath = type_("home")
    deployment = Deployment(name="testing", storage=s3_bucket)
    assert deployment.location == "home/"
