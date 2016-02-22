package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * An executor executes {@link ExecutionOperator}s.
 */
public interface Executor {

    /**
     * Executes the given {@code stage}.
     *
     * @param stage should be executed; must be executable by this instance, though
     * @return collected metadata from instrumentation
     */
    ExecutionProfile execute(ExecutionStage stage);

    /**
     * Releases any instances acquired by this instance to execute {@link ExecutionStage}s.
     */
    void dispose();

    /**
     * @return the {@link Platform} this instance belongs to
     */
    Platform getPlatform();

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
