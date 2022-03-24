package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.containers.GenericContainer;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.POSTGRES_IMAGE;

public class PostgresContainer extends GenericContainer<PostgresContainer> {

    private static final int PORT = 5432;
    private static final String NETWORK_ALIAS = "postgres";
    private static final String PASSWORD = "postgres";

    public PostgresContainer() {
        super(POSTGRES_IMAGE);
        withExposedPorts(PORT);
        withEnv("POSTGRES_PASSWORD", PASSWORD);
        withCommand("-c wal_level=logical");
        withNetworkAliases(NETWORK_ALIAS);
    }

    public String getInternalJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/postgres", NETWORK_ALIAS, PORT);
    }

    public String getJdbcUrl() {
        return String.format("jdbc:postgresql://localhost:%d/postgres", getMappedPort(PORT));
    }

    public String getUser() {
        return "postgres";
    }

    public String getPassword() {
        return PASSWORD;
    }
}
