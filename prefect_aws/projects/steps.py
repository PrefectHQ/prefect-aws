"""
Prefect project steps for code storage and retrieval in S3 and S3 compatible services.
"""
from pathlib import Path, PurePosixPath
from typing import Dict, Optional

import boto3
from botocore.client import Config
from prefect.utilities.filesystem import filter_files, relative_path_to_current_platform
from typing_extensions import TypedDict


class PushProjectToS3Output(TypedDict):
    """
    The output of the `push_project_to_s3` step.
    """

    bucket: str
    folder: str


class PullProjectFromS3Output(TypedDict):
    """
    The output of the `pull_project_from_s3` step.
    """

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
    """
    Pushes the contents of the current working directory to an S3 bucket,
    excluding files and folders specified in the ignore_file.

    Args:
        bucket: The name of the S3 bucket where the project files will be uploaded.
        folder: The folder in the S3 bucket where the project files will be uploaded.
        credentials: A dictionary of AWS credentials (aws_access_key_id,
            aws_secret_access_key, aws_session_token).
        client_parameters: A dictionary of additional parameters to pass to the boto3
            client.
        ignore_file: The name of the file containing ignore patterns.

    Returns:
        A dictionary containing the bucket and folder where the project was uploaded.

    Examples:
        Push a project to an S3 bucket:
        ```yaml
        build:
            - prefect_aws.projects.steps.push_project_to_s3:
                requires: prefect-aws
                bucket: my-bucket
                folder: my-project
        ```

        Push a project to an S3 bucket using credentials stored in a block:
        ```yaml
        build:
            - prefect_aws.projects.steps.push_project_to_s3:
                requires: prefect-aws
                bucket: my-bucket
                folder: my-project
                credentials: "{{ prefect.blocks.aws-credentials.dev-credentials }}"
        ```

    """
    if credentials is None:
        credentials = {}
    if client_parameters is None:
        client_parameters = {}
    advanced_config = client_parameters.pop("config", {})
    client = boto3.client(
        "s3", **credentials, **client_parameters, config=Config(**advanced_config)
    )

    local_path = Path.cwd()

    included_files = None
    if ignore_file and Path(ignore_file).exists():
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
    """
    Pulls the contents of a project from an S3 bucket to the current working directory.

    Args:
        bucket: The name of the S3 bucket where the project files are stored.
        folder: The folder in the S3 bucket where the project files are stored.
        credentials: A dictionary of AWS credentials (aws_access_key_id,
            aws_secret_access_key, aws_session_token).
        client_parameters: A dictionary of additional parameters to pass to the
            boto3 client.

    Returns:
        A dictionary containing the bucket, folder, and local directory where the
            project files were downloaded.

    Examples:
        Pull a project from S3 using the default credentials and client parameters:
        ```yaml
        build:
            - prefect_aws.projects.steps.pull_project_from_s3:
                requires: prefect-aws
                bucket: my-bucket
                folder: my-project
        ```

        Pull a project from S3 using credentials stored in a block:
        ```yaml
        build:
            - prefect_aws.projects.steps.pull_project_from_s3:
                requires: prefect-aws
                bucket: my-bucket
                folder: my-project
                credentials: "{{ prefect.blocks.aws-credentials.dev-credentials }}"
        ```
    """
    if credentials is None:
        credentials = {}
    if client_parameters is None:
        client_parameters = {}
    advanced_config = client_parameters.pop("config", {})

    session = boto3.Session(**credentials)
    s3 = session.client("s3", **client_parameters, config=Config(**advanced_config))

    local_path = Path.cwd()

    paginator = s3.get_paginator("list_objects_v2")
    for result in paginator.paginate(Bucket=bucket, Prefix=folder):
        for obj in result.get("Contents", []):
            remote_key = obj["Key"]

            if remote_key[-1] == "/":
                # object is a folder and will be created if it contains any objects
                continue

            target = PurePosixPath(
                local_path
                / relative_path_to_current_platform(remote_key).relative_to(folder)
            )
            Path.mkdir(Path(target.parent), parents=True, exist_ok=True)
            s3.download_file(bucket, remote_key, str(target))

    return {
        "bucket": bucket,
        "folder": folder,
        "directory": str(local_path),
    }
