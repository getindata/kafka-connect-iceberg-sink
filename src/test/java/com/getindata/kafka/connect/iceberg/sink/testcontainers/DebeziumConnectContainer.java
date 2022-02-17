package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.file.Paths;
import java.time.Duration;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.DEBEZIUM_CONNECT_IMAGE;

public class DebeziumConnectContainer extends GenericContainer<DebeziumConnectContainer> {

    private static final String NETWORK_ALIAS = "connect";
    private static final int PORT = 8083;

    public DebeziumConnectContainer() {
        super(DEBEZIUM_CONNECT_IMAGE);
        withNetworkAliases(NETWORK_ALIAS);
        withExposedPorts(PORT);
        withEnv("GROUP_ID", "1");
        withEnv("CONFIG_STORAGE_TOPIC", "my-connect-configs");
        withEnv("OFFSET_STORAGE_TOPIC", "my-connect-offsets");

        waitingFor(Wait.forHttp("/").withStartupTimeout(Duration.ofSeconds(120L)));
    }

    public DebeziumConnectContainer withKafkaBootstrap(String kafkaBootstrap) {
        return withEnv("BOOTSTRAP_SERVERS", kafkaBootstrap);
    }

    public DebeziumConnectContainer withPlugin(String filePath) {
        String fileName = Paths.get(filePath).getFileName().toString();
        return withFileSystemBind(filePath, "/kafka/connect/" + fileName);
    }

    public String getUrl() {
        return String.format("http://localhost:%d", getMappedPort(PORT));
    }
}
