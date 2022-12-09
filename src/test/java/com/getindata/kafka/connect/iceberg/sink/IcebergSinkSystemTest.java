package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.getindata.kafka.connect.iceberg.sink.testcontainers.*;
import com.getindata.kafka.connect.iceberg.sink.testresources.MinioTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.PostgresTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.SparkTestHelper;
import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_NAMESPACE;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.TABLE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.given;

@Testcontainers
class IcebergSinkSystemTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Network network = Network.newNetwork();

    @Container
    private static final S3MinioContainer s3MinioContainer = new S3MinioContainer()
            .withNetwork(network);

    @Container
    private static final KafkaContainer kafkaContainer = (KafkaContainer) new KafkaContainer()
            .withNetwork(network);

    @Container
    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer()
            .withNetwork(network)
            .withKafkaBoostrap(kafkaContainer.getInternalBootstrap());

    @Container
    private static final PostgresContainer postgresContainer = new PostgresContainer()
            .withNetwork(network);

    @Container
    private static final DebeziumConnectContainer debeziumConnectContainer = new DebeziumConnectContainer()
            .withNetwork(network)
            .withKafkaBootstrap(kafkaContainer.getInternalBootstrap())
            .withPlugin(getPluginPath());

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static SparkTestHelper sparkTestHelper;
    private static MinioTestHelper minioTestHelper;
    private static PostgresTestHelper postgresTestHelper;

    @BeforeAll
    static void setup() throws Exception {
        sparkTestHelper = new SparkTestHelper(s3MinioContainer.getUrl());
        minioTestHelper = new MinioTestHelper(s3MinioContainer.getUrl());
        postgresTestHelper = new PostgresTestHelper(postgresContainer.getJdbcUrl(), postgresContainer.getUser(), postgresContainer.getPassword());
        minioTestHelper.createDefaultBucket();
        createConnector(postgresConnectorConfig());
        createConnector(icebergSinkConfig());
    }

    @Test
    void shouldUpsertValue() throws SQLException {
        postgresTestHelper.execute("create table dbz_test1 (timestamp bigint, id int PRIMARY KEY, value varchar(20))");
        postgresTestHelper.execute("insert into dbz_test1 values(123, 1, 'ABC')");

        String query = "SELECT timestamp, id, value FROM " + getIcebergTableName("dbz_test1");
        given().ignoreExceptions().await().atMost(Duration.ofSeconds(15)).until(() -> sparkTestHelper.query(query).count() == 1);

        Dataset<Row> result = sparkTestHelper.query(query);
        assertThat(result.count()).isEqualTo(1);
        List<String> row = CollectionConverters.asJava(CollectionConverters.asJava(result.getRows(1, 0)).get(1));
        assertThat(row.get(0)).isEqualTo("123");
        assertThat(row.get(1)).isEqualTo("1");
        assertThat(row.get(2)).isEqualTo("ABC");

        postgresTestHelper.execute("update dbz_test1 set value='DEF' where id = 1");
        postgresTestHelper.execute("insert into dbz_test1 values(456, 2, 'ABC')");

        given().ignoreExceptions().await().atMost(Duration.ofSeconds(15)).until(() -> sparkTestHelper.query(query).count() == 2);

        result = sparkTestHelper.query(query);
        assertThat(result.count()).isEqualTo(2);
        Map<String, List<String>> rows = toMapById(result.getRows(2, 0));
        List<String> row1 = rows.get("1");
        assertThat(row1.get(0)).isEqualTo("123");
        assertThat(row1.get(1)).isEqualTo("1");
        assertThat(row1.get(2)).isEqualTo("DEF");
        List<String> row2 = rows.get("2");
        assertThat(row2.get(0)).isEqualTo("456");
        assertThat(row2.get(1)).isEqualTo("2");
        assertThat(row2.get(2)).isEqualTo("ABC");
    }

    @Test
    void shouldModifySchema() throws SQLException {
        postgresTestHelper.execute("create table dbz_test2 (timestamp bigint, id int PRIMARY KEY, value varchar(20))");
        postgresTestHelper.execute("insert into dbz_test2 values(123, 1, 'ABC')");

        String query1 = "SELECT timestamp, id, value FROM " + getIcebergTableName("dbz_test2");
        given().ignoreExceptions().await().atMost(Duration.ofSeconds(15)).until(() -> sparkTestHelper.query(query1).count() == 1);

        postgresTestHelper.execute("alter table dbz_test2 add column value2 varchar(20)");
        postgresTestHelper.execute("insert into dbz_test2 values(456, 2, 'DEF', 'XYZ')");

        String query2 = "SELECT timestamp, id, value, value2 FROM " + getIcebergTableName("dbz_test2");
        given().ignoreExceptions().await().atMost(Duration.ofSeconds(15)).until(() -> sparkTestHelper.query(query2).count() == 2);

        Dataset<Row> result = sparkTestHelper.query(query2);
        assertThat(result.count()).isEqualTo(2);
        Map<String, List<String>> rows = toMapById(result.getRows(2, 0));
        List<String> row = rows.get("1");
        assertThat(row.get(0)).isEqualTo("123");
        assertThat(row.get(1)).isEqualTo("1");
        assertThat(row.get(2)).isEqualTo("ABC");
        assertThat(row.get(3)).isEqualTo("null");
        row = rows.get("2");
        assertThat(row.get(0)).isEqualTo("456");
        assertThat(row.get(1)).isEqualTo("2");
        assertThat(row.get(2)).isEqualTo("DEF");
        assertThat(row.get(3)).isEqualTo("XYZ");
    }

    private Map<String, List<String>> toMapById(Seq<Seq<String>> rows) {
        return CollectionConverters.asJava(rows).stream()
                .map(CollectionConverters::asJava)
                .collect(Collectors.toMap(e -> e.get(1), e -> e));
    }

    private String getIcebergTableName(String postgresTableName) {
        return String.format("%s.%spostgres_public_%s", TABLE_NAMESPACE, TABLE_PREFIX, postgresTableName);
    }

    private static void createConnector(String config) throws URISyntaxException, IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(debeziumConnectContainer.getUrl() + "/connectors/"))
                .header("Content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(config))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(201);
    }

    private static String postgresConnectorConfig() {
        return "{\n" +
                "  \"name\": \"postgres-connector\",  \n" +
                "  \"config\": {\n" +
                "    \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\", \n" +
                "    \"database.hostname\": \"postgres\", \n" +
                "    \"database.port\": \"5432\", \n" +
                "    \"database.user\": \"postgres\", \n" +
                "    \"database.password\": \"postgres\", \n" +
                "    \"database.dbname\" : \"postgres\", \n" +
                "    \"database.server.name\": \"postgres\",\n" +
                "    \"slot.name\": \"dbzkafkaconnect\",\n" +
                "    \"plugin.name\": \"pgoutput\",\n" +
                "    \"topic.prefix\": \"postgres\",\n" +
                "    \"table.include.list\": \"public.dbz_test,public.dbz_test1,public.dbz_test2\"\n" +
                "  }\n" +
                "}";
    }

    private static String icebergSinkConfig() {
        ObjectNode icebergConfig = MAPPER.createObjectNode();
        icebergConfig.put("name", "iceberg-sink");
        ObjectNode configNode = MAPPER.createObjectNode();
        TestConfig.builder().withS3(s3MinioContainer.getInternalUrl()).build()
                .getProperties()
                .forEach(configNode::put);
        configNode.put("connector.class", "com.getindata.kafka.connect.iceberg.sink.IcebergSink");
        configNode.put("topics", "postgres.public.dbz_test,postgres.public.dbz_test1,postgres.public.dbz_test2");
        icebergConfig.set("config", configNode);
        return icebergConfig.toString();
    }

    private static String getPluginPath() {
        return new File("./target/plugin/kafka-connect-iceberg-sink").getAbsolutePath();
    }
}
