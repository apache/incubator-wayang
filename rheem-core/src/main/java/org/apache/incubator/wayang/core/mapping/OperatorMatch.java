package org.apache.incubator.wayang.core.mapping;

import org.apache.incubator.wayang.core.plan.wayangplan.Operator;

/**
 * An operator match correlates an {@link OperatorPattern} to an actually matched {@link Operator}.
 */
public class OperatorMatch {

    private final OperatorPattern pattern;

    private final Operator operator;

    public OperatorMatch(OperatorPattern pattern, Operator operator) {
        this.pattern = pattern;
        this.operator = operator;
    }

    public OperatorPattern getPattern() {
        return this.pattern;
    }

    public Operator getOperator() {
        return this.operator;
    }
}
