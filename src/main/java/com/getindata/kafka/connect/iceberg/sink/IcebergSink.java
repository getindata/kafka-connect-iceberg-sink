package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcebergSink extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSink.class);
    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> properties) {
        LOGGER.info("Sink configuration:");
        properties.forEach((key, value) -> LOGGER.info(String.format("%s : %s", key, value)));
        this.properties = properties;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IcebergSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return IntStream.range(0, maxTasks)
                .mapToObj(i -> properties)
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        LOGGER.info("Sink stopped");
    }

    @Override
    public ConfigDef config() {
        return IcebergSinkConfiguration.getConfigDef();
    }

    @Override
    public String version() {
        return IcebergSinkVersion.getVersion();
    }
}
