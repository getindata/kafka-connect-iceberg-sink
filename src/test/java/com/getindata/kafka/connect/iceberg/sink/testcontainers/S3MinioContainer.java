package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.MINIO_IMAGE;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_ACCESS_KEY;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_REGION_NAME;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_SECRET_KEY;

public class S3MinioContainer extends GenericContainer<S3MinioContainer> {

    private static final String NETWORK_ALIAS = "minio";
    private static final int MINIO_DEFAULT_PORT = 9000;
    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    public S3MinioContainer() {
        super(MINIO_IMAGE);
        withNetworkAliases(NETWORK_ALIAS);
        withExposedPorts(MINIO_DEFAULT_PORT);
        withEnv("MINIO_ACCESS_KEY", S3_ACCESS_KEY);
        withEnv("MINIO_SECRET_KEY", S3_SECRET_KEY);
        withEnv("MINIO_REGION_NAME", S3_REGION_NAME);
        withCommand("server " + DEFAULT_STORAGE_DIRECTORY);
        waitingFor(new HttpWaitStrategy()
                .forPath(HEALTH_ENDPOINT)
                .forPort(MINIO_DEFAULT_PORT)
                .withStartupTimeout(Duration.ofSeconds(30)));
    }

    public String getInternalUrl() {
        return String.format("http://%s:%d", NETWORK_ALIAS, MINIO_DEFAULT_PORT);
    }

    public String getUrl() {
        return String.format("http://localhost:%d", getMappedPort(MINIO_DEFAULT_PORT));
    }
}
