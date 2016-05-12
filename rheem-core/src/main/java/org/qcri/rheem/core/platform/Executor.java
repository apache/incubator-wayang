package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * An executor executes {@link ExecutionOperator}s.
 */
public interface Executor {

    /**
     * Executes the given {@code stage}.
     *
     * @param stage          should be executed; must be executable by this instance, though
     * @param executionState provides and accepts execution-related objects
     * @return collected metadata from instrumentation
     */
    void execute(ExecutionStage stage, ExecutionState executionState);

    /**
     * Releases any instances acquired by this instance to execute {@link ExecutionStage}s.
     */
    void dispose();

    /**
     * @return the {@link Platform} this instance belongs to
     */
    Platform getPlatform();

    /**
     * Register a new {@link ExecutionResource} that is maintained by this instance.
     *
     * @param resource the {@link ExecutionResource}
     */
    void register(ExecutionResource resource);

    /**
     * Unregister a disposed {@link ExecutionResource} that was maintained by this instance.
     *
     * @param resource the {@link ExecutionResource}
     */
    void unregister(ExecutionResource resource);

    /**
     * Factory for {@link Executor}s.
     */
    interface Factory {

        /**
         * @return a new {@link Executor}
         */
        Executor create(Job job);

    }

}
