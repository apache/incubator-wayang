package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.util.AbstractReferenceCountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements the {@link ExecutionResource} handling as defined by {@link Executor}.
 */
public abstract class ExecutorTemplate extends AbstractReferenceCountable implements Executor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Provides IDs to distinguish instances humanly readable.
     */
    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    /**
     * The {@link CrossPlatformExecutor} that instruments this instance or {@code null} if none
     */
    private final CrossPlatformExecutor crossPlatformExecutor;

    /**
     * Resources being held by this instance.
     */
    private final Set<ExecutionResource> registeredResources = new HashSet<>();

    /**
     * ID of this instance.
     */
    private final int id = idGenerator.getAndIncrement();

    /**
     * Creates a new instance.
     *
     * @param crossPlatformExecutor the {@link CrossPlatformExecutor} that instruments this instance or {@code null} if none
     */
    protected ExecutorTemplate(CrossPlatformExecutor crossPlatformExecutor) {
        this.crossPlatformExecutor = crossPlatformExecutor;
    }

    @Override
    protected void disposeUnreferenced() {
        this.dispose();
    }

    @Override
    public void register(ExecutionResource resource) {
        if (!this.registeredResources.add(resource)) {
            this.logger.warn("Registered {} twice.", resource);
        }
    }

    @Override
    public void unregister(ExecutionResource resource) {
        if (!this.registeredResources.remove(resource)) {
            this.logger.warn("Could not unregister {}, as it was not registered.", resource);
        }
    }

    @Override
    public void dispose() {
        if (this.getNumReferences() != 0) {
            this.logger.warn("Disposing {} although it is still being referenced.", this);
        }

        for (ExecutionResource resource : new ArrayList<>(this.registeredResources)) {
            resource.dispose();
        }

        if (this.getNumReferences() > 0) {
            this.logger.warn("There are still {} referenced on {}, which is about to be disposed.", this.getNumReferences(), this);
        }
    }

    @Override
    public CrossPlatformExecutor getCrossPlatformExecutor() {
        return this.crossPlatformExecutor;
    }

    @Override
    public String toString() {
        return String.format("%s[%x]", this.getClass().getSimpleName(), this.id);
    }
}
