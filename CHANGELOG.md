# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2018-03-09
### Changes
- `containers` in the config file is renamed to `apps` in order to be more flexible

## [0.2.0] - 2018-02-15
### Added
- clean up local log files when job is complete
- added CLI framework for easier handling of options
- added tests from 0 to 31% test coverage
- use LogDNA archive files in Google Cloud Storage instead of AWS S3 for bandwidth cost savings

### Changed
- `Bucket` in GCP config is now `UploadBucket`

### Removed
- AWS S3 config and download of log archive from AWS. Check latest README for up to date config documentation 

## [0.1.0] - 2017-11-12
### Added
- Working code to take LogDNA archives in AWS S3, process them, and ingest them BigQuery. 

[Unreleased]: https://github.com/lifechurch/dnaquery/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/lifechurch/dnaquery/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lifechurch/dnaquery/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lifechurch/dnaquery/compare/9bece45e6dfdb371b54a765a672429f9958bc2ca...v0.1.0