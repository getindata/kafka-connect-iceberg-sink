# Changelog

- Updates AWS lakeformation transitive dependency providing lakeformation support in s3 iceberg tables.

## [Unreleased]

- Updates dependencies to resolve some jackson-databind critical CVEs.
- Updates AWS lakeformation transitive dependency providing lakeformation support in s3 iceberg tables.
- Added Iceberg coercion support for Avro Array<Struct> types. Supports Debezium `data_collections` metadata.
- Added support for coercion of five Debezium temporal types to their Iceberg equivalents: Date, MicroTimestamp, ZonedTimestamp, MicroTime, and ZonedTime
- Rich temporal types are toggled on by new boolean configuration property: `rich-temporal-types`

## [0.3.1] - 2023-04-06

-   Add `iceberg.format-version` config setting to indicate which Iceberg table format version is used.

## [0.3.0] - 2023-03-24

-   Add `iceberg.partition` config setting to allow any column to be used for partitioning.

## [0.2.5] - 2023-03-20

-   Reverted pom.xml groupid

## [0.2.4] - 2023-03-13

-   Added support for `double` primitive type fields.
-   Allow coercion of iceberg table identifiers to `snake_case` setting `table.snake-case` boolean configuration.
## [0.2.2] - 2023-02-17

-   Allow changing iceberg-table specific settings using `iceberg.table-default.*` connector configuration properties

## [0.2.1] - 2022-12-09

-   removed 'table.write-format', can be replaced with 'iceberg.table-default.write.format.default'

## [0.2.0] - 2022-11-16

-   Added support for Hive metastore catalog
-   Replaced maven-shade plugin with maven-assembly. To add hadoop default configuration files
-   Integrated updates from <https://github.com/memiiso/debezium-server-iceberg>
-   Updated Iceberg to 1.0.0
-   Updated to Kafka Connect API 3.2.2

### Version Compatibility

This Iceberg Sink depends on a Spark 3.2 Runtime, which depends on a specific jackson minor version.
Kafka Connect >= 3.2.3 has updated the jackson version to an incompatible minor release (2.13)

## [0.1.3] - 2022-04-11

-   Logger levels changes
-   Added documentation to sink configuration

## [0.1.2] - 2022-03-25

## [0.1.1] - 2022-03-25

-   First release

[Unreleased]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.3.1...HEAD

[0.3.1]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.3.0...0.3.1

[0.3.0]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.2.5...0.3.0

[0.2.5]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.2.4...0.2.5

[0.2.4]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.2.2...0.2.4

[0.2.2]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.2.1...0.2.2

[0.2.1]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.3...0.2.0

[0.1.3]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/getindata/kafka-connect-iceberg-sink/compare/1190003ddc686273cb9ad28ce7dd2d8e458471d7...0.1.1
