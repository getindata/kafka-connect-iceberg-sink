# Changelog

## [Unreleased]

- Added support for Hive metastore catalog
- Replaced maven-shade plugin with maven-assembly. To add hadoop default configuration files
- Integrated updates from https://github.com/memiiso/debezium-server-iceberg
- Updated Iceberg to 1.0.0
- Updated to Kafka Connect API 3.2.2

### Version Compatibility

This Iceberg Sink depends on a Spark 3.2 Runtime, which depends on a specific jackson minor version. 
Kafka Connect >= 3.2.3 has updated the jackson version to an incompatible minor release (2.13)

## [0.1.3] - 2022-04-11

-   Logger levels changes
-   Added documentation to sink configuration

## [0.1.2] - 2022-03-25

## [0.1.1] - 2022-03-25

-   First release

[Unreleased]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.3...HEAD

[0.1.3]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/1190003ddc686273cb9ad28ce7dd2d8e458471d7...0.1.1
