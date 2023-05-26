# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed
- Change prefect.docker import to prefect.utilities.dockerutils to fix a crash when using custom blocks based on S3Bucket  - [#273](https://github.com/PrefectHQ/prefect-aws/pull/273)
## 0.3.2

Released on May 25th, 2023.

### Added

- Stream ECS Worker flow run logs to the API - [#267](https://github.com/PrefectHQ/prefect-aws/pull/267)

### Fixed

- Fixed bug where incorrect credentials model was selected when `MinIOCredentials` was used with `S3Bucket` - [#254](https://github.com/PrefectHQ/prefect-aws/pull/254)
- Fixed bug where `S3Bucket.list_objects` was truncating prefix paths ending with slashes - [#263](https://github.com/PrefectHQ/prefect-aws/pull/263)
- Fixed bug where ECS worker could not cancel flow runs - [#268](https://github.com/PrefectHQ/prefect-aws/pull/268)

## 0.3.1

Released on April 20th, 2023.

### Added

- `ECSWorker` for executing Prefect flow runs as ECS tasks - [#238](https://github.com/PrefectHQ/prefect-aws/pull/238)

### Fixed

- Fixes retrieving files from large buckets via pagination in the `pull_project_from_s3` step - [#240](https://github.com/PrefectHQ/prefect-aws/pull/240)
- Slugify tags to ensure compatibility with ECS limitations - [#245](https://github.com/PrefectHQ/prefect-aws/pull/245)

## 0.3.0

Released on April 6th, 2023.

### Added

- Support for unsigned AWS requests - [#220](https://github.com/PrefectHQ/prefect-aws/pull/220)
- Added push and pull project steps for S3 - [#229](https://github.com/PrefectHQ/prefect-aws/pull/229)
- `basepath` property to `S3Bucket` to maintain compatibility with storage block based deployments - [#231](https://github.com/PrefectHQ/prefect-aws/pull/231)

### Changed

- Added string support to `JsonPatch` implementation for task customizations Link [#233](https://github.com/PrefectHQ/prefect-aws/pull/233)

### Removed

- `basepath`, `aws_credentials` and `minio_credentials` fields from `S3Bucket` - [#231](https://github.com/PrefectHQ/prefect-aws/pull/231)

## 0.2.5

Released on March 13th, 2023.

### Fixed

- Fixed errors raised when using `write_path` and `read_path` with `credentials` field on `S3Bucket` - [#208](https://github.com/PrefectHQ/prefect-aws/pull/208)
- Resolving paths in `S3Bucket` unintentionally generating an arbitrary UUID when path is an empty string - [#212](https://github.com/PrefectHQ/prefect-aws/pull/212)
- Fixed crashes when pausing flow runs executed with `ECSTask` - [#218](https://github.com/PrefectHQ/prefect-aws/pull/218)

## 0.2.4

Released on January 23rd, 2023.

### Added

- `AwsSecret` block with `read_secret`, `write_secret`, and `delete_secret` methods - [#176](https://github.com/PrefectHQ/prefect-aws/pull/176)

### Changed

- Object keys sent in S3 requests use '/' delimiters instead of system default - [#192](https://github.com/PrefectHQ/prefect-aws/pull/192)

### Fixed

- Fix bug where ECSTask could fail to stream logs - [#186](https://github.com/PrefectHQ/prefect-aws/pull/186)

## 0.2.3

Released on January 4th, 2023.

### Fixed

- Missing `mypy_boto3_s3` in requirements.txt - [#189](https://github.com/PrefectHQ/prefect-aws/pull/189)

## 0.2.2

Released on January 4th, 2023.

### Added

- `list_objects`, `download_object_to_path`, `download_object_to_file_object`, `download_folder_to_path`, `upload_from_path`, `upload_from_file_object`, `upload_from_folder` methods in `S3Bucket` - [#85](https://github.com/PrefectHQ/prefect-aws/pull/175)
- `aws_client_parameters` as a field in `AwsCredentials` and `MinioCredentials` blocks - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)
- `get_client` and `get_s3_client` methods to `AwsCredentials` and `MinioCredentials` blocks  - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)

### Changed

- `S3Bucket` additionally inherits from `ObjectStorageBlock` - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)
- Exposed all existing blocks to the top level init - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)
- Inherit `CredentialsBlock` for `AwsCredentials` and `MinIOCredentials` - [#183](https://github.com/PrefectHQ/prefect-aws/pull/183)

### Deprecated

- `endpoint_url` field in S3Bucket; specify `aws_client_parameters` in `AwsCredentials` or `MinIOCredentials` instead - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)
- `basepath` field in S3Bucket; specify `bucket_folder` instead - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)
- `minio_credentials` and `aws_credentials` field in S3Bucket; use the `credentials` field instead  - [#175](https://github.com/PrefectHQ/prefect-aws/pull/175)

## 0.2.1

Released on December 7th, 2022.

### Changed

- `ECSTask` now logs the difference between the requested and the pre-registered task definition when using a `task_definition_arn` - [#166](https://github.com/PrefectHQ/prefect-aws/pull/166)
- Default of `S3Bucket` to be an empty string rather than None - [#169](https://github.com/PrefectHQ/prefect-aws/pull/169)

### Fixed

- Deployments of `S3Bucket` - [#169](https://github.com/PrefectHQ/prefect-aws/pull/169)
- The image from `task_definition_arn` will be respected if `image` is not explicitly set - [#170](https://github.com/PrefectHQ/prefect-aws/pull/170)

## 0.2.0

Released on December 2nd, 2022.

### Added

- `ECSTask.kill` method for cancellation support - [#163](https://github.com/PrefectHQ/prefect-aws/pull/163)

### Changed

- Breaking: Identifiers `ECSTask` now include the cluster in addition to the task ARN - [#163](https://github.com/PrefectHQ/prefect-aws/pull/163)
- Bumped minimum required `prefect` version - [#154](https://github.com/PrefectHQ/prefect-aws/pull/154)

## 0.1.8

Released on November 16th, 2022.

### Added

- Added `family` field to `ECSTask` to configure task definition family names — [#152](https://github.com/PrefectHQ/prefect-aws/pull/152)

### Changed

- Changes the default `ECSTask` family to include the flow and deployment names if available — [#152](https://github.com/PrefectHQ/prefect-aws/pull/152)

### Fixed

- Fixed failure while watching ECS task execution when the task is missing — [#153](https://github.com/PrefectHQ/prefect-aws/pull/153)

## 0.1.7

Released on October 28th, 2022.

### Changed

- `ECSTask` is no longer experimental — [#137](https://github.com/PrefectHQ/prefect-aws/pull/137)

### Fixed
- Fix ignore_file option in `S3Bucket` skipping files which should be included — [#139](https://github.com/PrefectHQ/prefect-aws/pull/139)
- Fixed bug where `basepath` is used twice in the path when using `S3Bucket.put_directory` - [#143](https://github.com/PrefectHQ/prefect-aws/pull/143)

## 0.1.6

Released on October 19th, 2022.

### Added

- `get_directory` and `put_directory` methods on `S3Bucket`. The `S3Bucket` block is now usable for remote flow storage with deployments. - [#82](https://github.com/PrefectHQ/prefect-aws/pull/82)

## 0.1.5

Released on October 14th, 2022.

### Added

- Add `ECSTask.cloudwatch_logs_options` for customization of CloudWatch logging — [#116](https://github.com/PrefectHQ/prefect-aws/pull/116)
- Added `config` parameter to AwsClientParameters to support advanced configuration (e.g. accessing public S3 buckets) [#117](https://github.com/PrefectHQ/prefect-aws/pull/117)
- Add `@sync_compatible` to `S3Bucket` methods to allow calling them in sync contexts - [#119](https://github.com/PrefectHQ/prefect-aws/pull/119).
- Add `ECSTask.task_customizations` for customization of arbitary fields in the run task payload — [#120](https://github.com/PrefectHQ/prefect-aws/pull/120)

### Fixed

- Fix configuration to submit doc edits via GitHub - [#110](https://github.com/PrefectHQ/prefect-aws/pull/110)
- Removed invalid ecs task register fields - [#126](https://github.com/PrefectHQ/prefect-aws/issues/126)

## 0.1.4

Released on September 13th, 2022.

### Changed

- Increased default timeout on the `ECSTask` block - [#106](https://github.com/PrefectHQ/prefect-aws/pull/106)

## 0.1.3

Released on September 12th, 2022.

### Added

- `client_waiter` task - [#43](https://github.com/PrefectHQ/prefect-aws/pull/43)
- `ECSTask` infrastructure block - [#85](https://github.com/PrefectHQ/prefect-aws/pull/85)

## 0.1.2

Released on August 2nd, 2022.

### Added

- `batch_submit` task - [#41](https://github.com/PrefectHQ/prefect-aws/pull/41)
- `MinIOCredentials` block - [#46](https://github.com/PrefectHQ/prefect-aws/pull/46)
- `S3Bucket` block - [#47](https://github.com/PrefectHQ/prefect-aws/pull/47)

### Changed

- Converted `AwsCredentials` into a `Block` [#45](https://github.com/PrefectHQ/prefect-aws/pull/45)

### Deprecated

### Removed

- Removed `.result()` and `is_complete` on test flow calls. [#45](https://github.com/PrefectHQ/prefect-aws/pull/45)

## 0.1.1

## Added

Released on April 18th, 2022.

- `AwsClientParameters` for added configuration of the `boto3` S3 client - [#29](https://github.com/PrefectHQ/prefect-aws/pull/29)
  - Contributed by [davzucky](https://github.com/davzucky)
- Added boto3 client type hinting via `types-boto3` - [#26](https://github.com/PrefectHQ/prefect-aws/pull/26)
  - Contributed by [davzucky](https://github.com/davzucky)

## 0.1.0

Released on March 9th, 2022.

### Added

- `s3_download`, `s3_upload` and `s3_list_objects` tasks
- `read_secret` task - [#6](https://github.com/PrefectHQ/prefect-aws/pull/6)
- `update_secret` task - [#12](https://github.com/PrefectHQ/prefect-aws/pull/12)
- `create_secret` and `delete_secret` tasks - [#13](https://github.com/PrefectHQ/prefect-aws/pull/13)
