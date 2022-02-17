package com.getindata.kafka.connect.iceberg.sink.testcontainers;

import com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig;
import org.testcontainers.containers.GenericContainer;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.ZOOKEEPER_IMAGE;
import static java.lang.String.format;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

    private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 2000;

    private final String networkAlias = "zookeeper";

    public ZookeeperContainer() {
        super(ZOOKEEPER_IMAGE);

        addEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
        addEnv("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));

        addExposedPort(ZOOKEEPER_INTERNAL_PORT);
        withNetworkAliases(networkAlias);
    }

    public String getInternalUrl() {
        return format("%s:%d", networkAlias, ZOOKEEPER_INTERNAL_PORT);
    }


}