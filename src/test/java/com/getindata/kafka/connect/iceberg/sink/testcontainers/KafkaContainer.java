package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.utility.DockerImageName;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.KAFKA_IMAGE;

public class KafkaContainer extends org.testcontainers.containers.KafkaContainer {

    public KafkaContainer() {
        super(DockerImageName.parse(KAFKA_IMAGE));
        withNetworkAliases("kafka");
        withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");
    }

    public String getInternalBootstrap() {
        return "kafka:9092";
    }
}
