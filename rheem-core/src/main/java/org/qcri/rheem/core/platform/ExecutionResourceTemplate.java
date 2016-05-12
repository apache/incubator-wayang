package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.LoggerFactory;

/**
 * Implements various functionalities of an {@link ExecutionResource}.
 */
public abstract class ExecutionResourceTemplate implements ExecutionResource {

    /**
     * Maintains this instance.
     */
    private final Executor executor;

    /**
     * Creates a new instance and registers it with its {@link Executor}.
     *
     * @param executor that maintains this instance or {@code null} if none
     */
    protected ExecutionResourceTemplate(Executor executor) {
        this.executor = executor;
        if (this.executor != null) {
            this.executor.register(this);
        }
    }

    @Override
    public void dispose() throws RheemException {
        try {
            this.doDispose();
        } catch (Throwable t) {
            throw new RheemException(String.format("Releasing %s failed.", this), t);
        } finally {
            if (this.executor != null) {
                this.executor.unregister(this);
            }
        }
    }

    /**
     * Performs the actual disposing work of this instance.
     *
     * @throws Throwable in case anything goes wrong
     */
    abstract protected void doDispose() throws Throwable;

    /**
     * Perform the given {@link Action}. If it fails, just log the error.
     *
     * @param action should be performed
     */
    protected void doSafe(Action action) {
        try {
            action.execute();
        } catch (Throwable t) {
            LoggerFactory.getLogger(this.getClass()).error("Action failed.", t);
        }
    }

    @Override
    public Executor getExecutor() {
        return this.executor;
    }

    /**
     * This interface represents any piece of code that takes no input and produces no output but may fail.
     */
    @FunctionalInterface
    protected interface Action {

        /**
         * Perform this action.
         *
         * @throws Throwable in case anything goes wrong
         */
        void execute() throws Throwable;

    }

}
