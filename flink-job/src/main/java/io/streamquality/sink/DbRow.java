package io.streamquality.sink;

import java.io.Serializable;

/**
 * One row destined for a table in schema sq (unqualified name, e.g. "check_results"), as a flat JSON object.
 * key = the table's natural key (null if the table needs no de-duplication); a batch keeps only the last row per key,
 * because a single INSERT ... ON CONFLICT DO UPDATE must not touch the same row twice.
 */
public record DbRow(String table, String key, String json) implements Serializable {}
