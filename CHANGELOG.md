# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

- Fix configuration to submit doc edits via GitHub - [#110](https://github.com/PrefectHQ/prefect-aws/pull/110)
- Add `ECSTask.cloudwatch_logs_options` for customization of CloudWatch logging — [#116](https://github.com/PrefectHQ/prefect-aws/pull/116)
- Added `config` parameter to AwsClientParameters to support advanced configuration (e.g. accessing public S3 buckets) [#117](https://github.com/PrefectHQ/prefect-aws/pull/117)
- Add `@sync_compatible` to `S3Bucket` methods to allow calling them in sync contexts - [#119](https://github.com/PrefectHQ/prefect-aws/pull/119).
- Add `ECSTask.task_customizations` for customization of arbitary fields in the run task payload — [#120](https://github.com/PrefectHQ/prefect-aws/pull/120)

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

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
