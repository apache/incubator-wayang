package org.qcri.rheem.core.mapping;

import org.qcri.rheem.core.plan.Operator;

/**
 * An operator match correlates an {@link OperatorPattern} to an actually matched
 * {@link org.qcri.rheem.core.plan.Operator}.
 */
public class OperatorMatch {

    private final OperatorPattern pattern;

    private final Operator operator;

    public OperatorMatch(OperatorPattern pattern, Operator operator) {
        this.pattern = pattern;
        this.operator = operator;
    }

    public OperatorPattern getPattern() {
        return pattern;
    }

    public Operator getOperator() {
        return operator;
    }
}
