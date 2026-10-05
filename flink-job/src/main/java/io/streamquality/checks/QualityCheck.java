package io.streamquality.checks;

import io.streamquality.model.CheckResult;
import io.streamquality.model.ParsedRecord;
import java.io.Serializable;
import java.util.List;

/**
 * Contract every check implements. One instance = one check type; it is applied per topic
 * inside a single windowed operator so a record is touched once, not once per check.
 *
 * Lifecycle per (topic, window):
 *   acc = createAccumulator()
 *   acc = add(record, acc)        // for each record, hot path: O(1), no allocations if possible
 *   acc = merge(a, b)             // session/merging windows & parallel pre-aggregation
 *   results = evaluate(acc, ctx)  // once, at window close
 *
 * ACC must be Serializable and small (sketch bytes, counters) - it lives in RocksDB state.
 * Checks are stateless otherwise; long-lived baselines are accessed via ctx.baseline().
 */
public interface QualityCheck<ACC extends Serializable> extends Serializable {

    /** Stable id, used in config keys and the check_type column. */
    String name();

    ACC createAccumulator();

    ACC add(ParsedRecord record, ACC acc);

    ACC merge(ACC a, ACC b);

    /** Emit zero or more rows (one per field for field-level checks). Must be pure and deterministic. */
    List<CheckResult> evaluate(ACC acc, WindowContext ctx);
}
