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
    return MinIOCredentials(
        minio_root_user="minioadmin",
        minio_root_password="minioadmin"
    )


bucket_name = "test_bucket"


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


@pytest.fixture(params=["aws_credentials", "minio_credentials"])
def s3_bucket(s3, request, aws_creds_block, minio_creds_block):

    key = request.param

    if key == "aws_credentials":
        fs = S3Bucket(bucket_name=bucket_name, aws_credentials=aws_creds_block)
    elif key == "minio_credentials":
        fs = S3Bucket(bucket_name=bucket_name, minio_credentials=minio_creds_block)

    s3.create_bucket(Bucket=bucket_name)

    return fs


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
        bucket_name=bucket_name,
        aws_credentials=aws_creds_block,
        basepath="subfolder",
    )

    key = await s3_bucket_block.write_path("data.csv", content=b"hello")
    assert await s3_bucket_block.read_path(key) == b"hello"
