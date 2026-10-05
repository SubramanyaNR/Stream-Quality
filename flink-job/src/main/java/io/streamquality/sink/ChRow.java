package io.streamquality.sink;

import java.io.Serializable;

/** One JSONEachRow line destined for a ClickHouse table (unqualified name, e.g. "check_results"). */
public record ChRow(String table, String json) implements Serializable {}
