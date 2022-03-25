package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.SCHEMA_REGISTRY_IMAGE;
import static java.lang.String.format;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;
    private static final String NETWORK_ALIAS = "schema-registry";

    public SchemaRegistryContainer() {
        super(SCHEMA_REGISTRY_IMAGE);

        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases(NETWORK_ALIAS);

        waitingFor(Wait.forHttp("/subjects"));
    }

    public SchemaRegistryContainer withKafkaBoostrap(String kafkaBoostrap) {
        return withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaBoostrap);
    }

    public String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }

    public String getInternalUrl() {
        return format("http://%s:%d", NETWORK_ALIAS, SCHEMA_REGISTRY_INTERNAL_PORT);
    }
}