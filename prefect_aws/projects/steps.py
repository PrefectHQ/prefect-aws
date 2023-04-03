from pathlib import Path, PurePosixPath
from typing import Dict, Optional
from typing_extensions import TypedDict
import boto3
from prefect.utilities.filesystem import filter_files


class PushProjectToS3Output(TypedDict):
    bucket: str
    folder: str

class PullProjectFromS3Output(TypedDict):
    bucket: str
    folder: str
    directory: str


def push_project_to_s3(
    bucket: str,
    folder: str,
    credentials: Optional[Dict] = None,
    client_parameters: Optional[Dict] = None,
    ignore_file: Optional[str] = ".prefectignore",
) -> PushProjectToS3Output:
    if credentials is None:
        credentials = {}
    if client_parameters is None:
        client_parameters = {}
    client = boto3.client("s3", **credentials, **client_parameters)

    local_path = Path.cwd()

    included_files = None
    if ignore_file:
        with open(ignore_file, "r") as f:
            ignore_patterns = f.readlines()

        included_files = filter_files(str(local_path), ignore_patterns)

    for local_file_path in local_path.expanduser().rglob("*"):
        if (
            included_files is not None
            and str(local_file_path.relative_to(local_path)) not in included_files
        ):
            continue
        elif not local_file_path.is_dir():
            remote_file_path = Path(folder) / local_file_path.relative_to(local_path)
            client.upload_file(str(local_file_path), bucket, str(remote_file_path))

    return {
        "bucket": bucket,
        "folder": folder,
    }


def pull_project_from_s3(
    bucket: str,
    folder: str,
    credentials: Optional[Dict] = None,
    client_parameters: Optional[Dict] = None,
) -> PullProjectFromS3Output:

    if credentials is None:
        credentials = {}
    if client_parameters is None:
        client_parameters = {}
    bucket_resource = boto3.Session(**credentials).resource("s3").Bucket(bucket)
    
    local_path = Path.cwd()
    for obj in bucket_resource.objects.filter(Prefix=folder):
        if obj.key[-1] == "/":
            # object is a folder and will be created if it contains any objects
            continue
        target = local_path / PurePosixPath(obj.key).relative_to(folder)
        Path.mkdir(target.parent, parents=True, exist_ok=True)
        bucket_resource.download_file(obj.key, str(target))

    return {
        "bucket": bucket,
        "folder": folder,
        "directory": str(local_path),
    }

