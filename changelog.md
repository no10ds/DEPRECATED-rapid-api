# Changelog

All notable changes to this project will be documented in this file. This project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## v1.3.0 - _2022-06-17_

See [v1.3.0] changes

### Added
- Getting started documentation to walkthrough dataset uploading and querying

### Fixed
- Add `resource_prefix` to the necessary AWS resources to enable the hosting of multiple rAPId instances

[v1.3.0]: https://github.com/no10ds/rapid-api/compare/v1.2.0...v1.3.0

## v1.2.0 - _2022-05-31_

See [v1.2.0] changes

### Added
- Documentation improvements
  - OpenAPI spec includes endpoint behaviour documentation
  - Added example scripts for programmatic interaction

[v1.2.0]: https://github.com/no10ds/rapid-api/compare/v1.1.0...v1.2.0

## v1.1.0 - _2022-05-29_

See [v1.1.0] changes

### Added
- Protected domains:
  - Allows the separation of access permissions for specific `protected domains`
  - See the data acccess [docs](./docs/guides/usage/data_access.md)
  - See usage [docs](./docs/guides/usage/usage.md#domain)
- Overwrite update behaviour:
  - Allows datasets to be overwritten when a new file is uploaded, rather than just appended to
  - See the schema creation [docs](./docs/guides/usage/schema_creation.md#update-behaviour)

[v1.1.0]: https://github.com/no10ds/rapid-api/compare/v1.0.0...v1.1.0


## v1.0.0 - _2022-05-10_

See [v1.0.0] changes

### Added
- Documentation and usage guides can be found [here](https://github.com/no10ds/rapid-api/tree/master/docs)
- First full application release
- Features:
  - Generate a schema from a dataset
  - Upload a schema
  - Upload a dataset
  - List available datasets
  - Get metadata information for a dataset
  - Query a previously uploaded dataset
  - Add a new client app

[Unreleased changes]: https://github.com/no10ds/rapid-api/compare/v1.0.0...HEAD
[v1.0.0]: https://github.com/no10ds/rapid-api/compare/ff60bf65...v1.0.0
