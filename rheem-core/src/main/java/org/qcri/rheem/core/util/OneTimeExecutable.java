package org.qcri.rheem.core.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Method wrapper that ensures that it is called only once.
 */
public abstract class OneTimeExecutable {

    /**
     * Guard to avoid double execution.
     */
    private final AtomicBoolean isNotExecuted = new AtomicBoolean(true);

    /**
     * Invoke {@link #doExecute()} unless it has been executed already. Also, ensure that it will not be invoked
     * a second time.
     *
     * @return whether the method invocation resulted in invoking {@link #doExecute()}
     */
    protected boolean tryExecute() {
        if (this.isNotExecuted.getAndSet(false)) {
            this.doExecute();
            return true;
        }
        return false;
    }

    /**
     * Invoke {@link #doExecute()}. Also, ensure that it will not be invoked
     * a second time.
     *
     * @throws IllegalStateException if {@link #doExecute()} has been already invoked
     */
    protected void execute() throws IllegalStateException {
        if (!this.tryExecute()) {
            throw new IllegalStateException(String.format("%s cannot be executed a second time.", this));
        }
    }

    /**
     * Performs the actual work of this instance. Should only be invoked via {@link #execute()} and
     * {@link #tryExecute()}.
     */
    protected abstract void doExecute();

}
