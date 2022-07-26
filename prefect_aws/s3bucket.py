import io
from prefect.filesystems import ReadableFileSystem, WritableFileSystem
from pyparsing import Optional
from prefect_aws import AwsClientParameters, AwsCredentials

class S3Bucket(ReadableFileSystem, WritableFileSystem):
    """Info here."""
    bucket: str
    key: str 
    credentials: AwsCredentials # default but could specify other credentials, i.e MinIO
    base_path: Optional[str] = None
    endpoint_url: Optional[str] = None

    def block_initialization(self) -> None:
        self.aws_session = self.credentials.get_boto3_session()

    def check_basepath():
        pass

    def _resolve_path():
        pass

    def read_path(self, path) -> bytes:
        resolved_path = self._resolve_path(path)
        s3_client = self.aws_session.client('s3', endpoint_url=self.endpoint_url)
        with io.BytesIO() as stream:
            s3_client.download_fileobj(Bucket=self.bucket, Key=self.key, FileObj=stream)
            stream.seek(0)
            output = stream.read()
            return output

    def write_path(self, path: str, content: bytes) -> None:
        s3_client = self.aws_session.client('s3', endpoint_url=self.endpoint_url)
        with io.BytesIO(content) as stream:
            s3_client.upload_fileobj(Bucket=self.bucket, Key=self.key, FileObj=stream)
