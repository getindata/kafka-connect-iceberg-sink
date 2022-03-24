package com.getindata.kafka.connect.iceberg.sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.Map;

public class IcebergCatalogFactory {
    public static Catalog create(IcebergSinkConfiguration icebergSinkConfig) {
        Configuration hadoopConfig = new Configuration();
        Map<String, String> catalogConfiguration = icebergSinkConfig.getIcebergCatalogConfiguration();
        catalogConfiguration.forEach(hadoopConfig::set);
        return CatalogUtil.buildIcebergCatalog(icebergSinkConfig.getCatalogName(), catalogConfiguration, hadoopConfig);
    }
}
