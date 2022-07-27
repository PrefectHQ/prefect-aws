# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `batch_submit` task - [#41](https://github.com/PrefectHQ/prefect-aws/issues/41)

### Changed

- Converted `AwsCredentials` into a `Block` [#45](https://github.com/PrefectHQ/prefect-aws/pull/45)

### Deprecated

### Removed

- Removed `.result()` and `is_complete` on test flow calls. [#45](https://github.com/PrefectHQ/prefect-aws/pull/45)

### Fixed

### Security

## 0.1.1

## Added

Released on April 18th, 2022

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
