package io.streamquality.checks;

import io.streamquality.model.CheckResult;
import io.streamquality.model.ParsedRecord;
import java.io.Serializable;
import java.util.List;

/**
 * Contract every check implements. One instance = one check type; all checks run inside ONE
 * windowed operator per topic so each record is touched once, not once per check.
 *
 * Lifecycle per (topic, window):
 *   acc = createAccumulator()
 *   acc = add(record, acc)        // hot path, only real (non-synthetic) records; O(1)
 *   acc = merge(a, b)
 *   results = evaluate(acc, ctx)  // once, when the window closes
 *
 * ACC must be small, Serializable and Flink-friendly (public fields / no-arg ctor): it lives in state.
 * evaluate() must be pure; baseline-relative checks read ctx.history().
 */
public interface QualityCheck<ACC extends Serializable> extends Serializable {

    /** Stable id; also the key under which the accumulator is stored. */
    String name();

    ACC createAccumulator();

    ACC add(ParsedRecord record, ACC acc);

    ACC merge(ACC a, ACC b);

    List<CheckResult> evaluate(ACC acc, WindowContext ctx);
}
