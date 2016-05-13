package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.util.AbstractReferenceCountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements the {@link ExecutionResource} handling as defined by {@link Executor}.
 */
public abstract class ExecutorTemplate extends AbstractReferenceCountable implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Resources being held by this instance.
     */
    private final Set<ExecutionResource> registeredResources = new HashSet<>();

    @Override
    protected void disposeUnreferenced() {
        this.dispose();
    }

    @Override
    public void register(ExecutionResource resource) {
        if (!this.registeredResources.add(resource)) {
            LoggerFactory.getLogger(this.getClass()).warn("Registered {} twice.", resource);
        }
    }

    @Override
    public void unregister(ExecutionResource resource) {
        if (!this.registeredResources.remove(resource)) {
            LoggerFactory.getLogger(this.getClass()).warn("Could not unregister {}, as it was not registered.", resource);
        }
    }

    @Override
    public void dispose() {
        for (ExecutionResource resource : new ArrayList<>(this.registeredResources)) {
            resource.dispose();
        }

        if (this.getNumReferences() > 0) {
            this.logger.warn("There are still {} referenced on {}, which is about to be disposed.", this.getNumReferences(), this);
        }
    }
}
