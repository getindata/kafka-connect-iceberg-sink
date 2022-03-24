package com.getindata.kafka.connect.iceberg.sink.testresources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresTestHelper {
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public PostgresTestHelper(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public void execute(String sql) throws SQLException {
        try (Connection con = DriverManager.getConnection(jdbcUrl, user, password)) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute(sql);
            }
        }
    }
}
