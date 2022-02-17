package com.getindata.kafka.connect.iceberg.sink.testresources;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_ACCESS_KEY;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_BUCKET;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_REGION_NAME;
import static com.getindata.kafka.connect.iceberg.sink.testresources.TestConfig.S3_SECRET_KEY;

public class MinioTestHelper {
    private final MinioClient client;

    public MinioTestHelper(String s3url) {
        client = MinioClient.builder()
                .endpoint(s3url)
                .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
                .build();
    }

    public void createDefaultBucket() throws NoSuchAlgorithmException, KeyManagementException, ServerException, InsufficientDataException, ErrorResponseException, IOException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        client.ignoreCertCheck();
        client.makeBucket(MakeBucketArgs.builder()
                .region(S3_REGION_NAME)
                .bucket(S3_BUCKET)
                .build());
    }
}
