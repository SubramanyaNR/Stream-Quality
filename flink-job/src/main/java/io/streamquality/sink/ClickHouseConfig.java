package io.streamquality.sink;

import java.io.Serializable;

public record ClickHouseConfig(String url, String database, String user, String password,
                               int batchRows, int maxRetries, long flushIntervalMs) implements Serializable {
    public static ClickHouseConfig of(String url, String user, String password) {
        return new ClickHouseConfig(url, "sq", user, password, 5000, 4, 2000);
    }
}
