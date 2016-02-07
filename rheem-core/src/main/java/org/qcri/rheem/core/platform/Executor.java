package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * An executor executes {@link ExecutionOperator}s.
 */
public interface Executor {

    /**
     * Evaluate an execution operator. This is only a dummy implementation!
     */
    void evaluate(ExecutionOperator executionOperator);

    /**
     * Factory for {@link Executor}s.
     */
    public interface Factory {

        /**
         * @return a new {@link Executor}
         */
        Executor create();

    }

}
