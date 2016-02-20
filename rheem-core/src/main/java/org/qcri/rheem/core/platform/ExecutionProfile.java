package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains metadata from an instrumented execution.
 */
public class ExecutionProfile {

    private final Map<Channel, Long> cardinalities = new HashMap<>();

    public Map<Channel, Long> getCardinalities() {
        return this.cardinalities;
    }

    public void merge(ExecutionProfile that) {
        if (that != null) this.cardinalities.putAll(that.cardinalities);
    }
}
