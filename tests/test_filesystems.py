import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from prefect_aws import AwsCredentials, MinIOCredentials, S3Bucket


@pytest.fixture
def aws_creds_block():
    return AwsCredentials(aws_access_key_id="testing", aws_secret_access_key="testing")


@pytest.fixture
def minio_creds_block():
    return MinIOCredentials(minio_root_user="testing", minio_root_password="testing")


bucket_name = "test_bucket"


@pytest.fixture
def s3():

    """Mock connection to AWS S3 with boto3 client."""

    with mock_s3():
        yield boto3.client(
            service_name="s3",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            aws_session_token="testing",
        )


@pytest.mark.parametrize("creds", [aws_creds_block, minio_creds_block])
async def test_read_write_roundtrip(s3, creds):

    """
    Create an S3 bucket, instantiate S3Bucket block, write to and read from
    bucket.
    """

    s3.create_bucket(Bucket=bucket_name)
    print("Bucket created")
    fs = S3Bucket(bucket_name=bucket_name, credentials=creds)
    key = await fs.write_path("test.txt", content=b"hello")
    print("Wrote to path:", key)
    assert await fs.read_path(key) == b"hello"
    print("Read pathx")


@pytest.mark.parametrize("creds", [aws_creds_block, minio_creds_block])
async def test_write_with_missing_directory_succeeds(s3, creds):

    """
    Create an S3 bucket, instantiate S3Bucket block, write to path with
    missing directory.
    """

    s3.create_bucket(Bucket=bucket_name)
    fs = S3Bucket(bucket_name=bucket_name, credentials=creds)
    key = await fs.write_path("folder/test.txt", content=b"hello")
    assert await fs.read_path(key) == b"hello"


@pytest.mark.parametrize("creds", [aws_creds_block, minio_creds_block])
async def test_read_fails_does_not_exist(s3, creds):

    """
    Create an S3 bucket, instantiate S3Bucket block, assert read from
    nonexistent path fails.
    """

    s3.create_bucket(Bucket=bucket_name)
    fs = S3Bucket(bucket_name=bucket_name, credentials=creds)
    with pytest.raises(ClientError):
        await fs.read_path("test_bucket/foo/bar")


@pytest.mark.parametrize("creds", [aws_creds_block, minio_creds_block])
async def test_basepath(s3, creds):

    """
    Create an S3 bucket, instantiate S3Bucket block, write to and read from
    bucket.
    """

    fs = S3Bucket(bucket_name=bucket_name, credentials=creds, basepath="subfolder")
    key = await fs.write_path("test.txt", content=b"hello")
    assert await fs.read_path(key) == b"hello"
