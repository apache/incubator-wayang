package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.util.AbstractReferenceCountable;

/**
 * Implements various functionalities of an {@link ExecutionResource}.
 */
public abstract class ExecutionResourceTemplate extends AbstractReferenceCountable implements ExecutionResource {

    /**
     * Maintains this instance.
     */
    private final CompositeExecutionResource container;

    /**
     * Creates a new instance and registers it with an {@link CompositeExecutionResource}. If the latter is an
     * {@link ExecutionResource} itself, then this instance obtains a reference on it during its life cycle.
     *
     * @param container that maintains this instance or {@code null} if none
     */
    protected ExecutionResourceTemplate(CompositeExecutionResource container) {
        this.container = container;
        if (this.container != null) {
            this.container.register(this);
            this.container.noteObtainedReference();
        }
    }

    @Override
    protected void disposeUnreferenced() {
        this.dispose();
    }

    @Override
    public void dispose() throws RheemException {
        try {
            this.doDispose();
        } catch (Throwable t) {
            throw new RheemException(String.format("Releasing %s failed.", this), t);
        } finally {
            if (this.container != null) {
                this.container.unregister(this);
                this.container.noteDiscardedReference(true);
            }
        }
    }

    /**
     * Performs the actual disposing work of this instance.
     *
     * @throws Throwable in case anything goes wrong
     */
    abstract protected void doDispose() throws Throwable;

}
