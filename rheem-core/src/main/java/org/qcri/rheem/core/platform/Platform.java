package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.ExecutionOperator;

/**
 * A platform describes an execution engine that executes {@link ExecutionOperator}s.
 */
public class Platform {

    private final String name;

    private final Executor.Factory executorFactory;

    public Platform(String name, Executor.Factory executorFactory) {
        this.name = name;
        this.executorFactory = executorFactory;
    }

    /**
     * Dummy interface for executing plans. This will definitively change in the real implementation.
     *
     * @param executionOperator the execution operator whose result should be evaluated
     */
    public void evaluate(ExecutionOperator executionOperator) {
        final Executor executor = this.executorFactory.create();
        executor.evaluate(executionOperator);
    }
}
