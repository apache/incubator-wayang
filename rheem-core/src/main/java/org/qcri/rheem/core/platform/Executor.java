package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.ExecutionOperator;

/**
 * An executor executes {@link org.qcri.rheem.core.plan.ExecutionOperator}s.
 */
public interface Executor {

    /**
     * Evaluate an execution operator. This is only a dummy implementation!
     */
    void evaluate(ExecutionOperator executionOperator);

    /**
     * Factory for {@link Executor}s.
     */
    interface Factory {

        /**
         * @return a new {@link Executor}
         */
        Executor create();

    }

}
