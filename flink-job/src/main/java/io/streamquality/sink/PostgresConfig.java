package io.streamquality.sink;

import java.io.Serializable;

public record PostgresConfig(String url, String user, String password,
                             int batchRows, int maxRetries, long flushIntervalMs) implements Serializable {
    public static PostgresConfig of(String url, String user, String password) {
        return new PostgresConfig(url, user, password, 5000, 4, 2000);
    }
}
