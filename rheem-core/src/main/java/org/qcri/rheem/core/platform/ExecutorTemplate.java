package org.qcri.rheem.core.platform;

import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Implements the {@link ExecutionResource} handling as defined by {@link Executor}.
 */
public abstract class ExecutorTemplate implements Executor {

    private final Set<ExecutionResource> registeredResources = new HashSet<>();

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
}
