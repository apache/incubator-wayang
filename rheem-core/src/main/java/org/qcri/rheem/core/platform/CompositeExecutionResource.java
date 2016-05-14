package org.qcri.rheem.core.platform;

/**
 * {@link ExecutionResource} that itself contains {@link ExecutionResource}s.
 */
public interface CompositeExecutionResource extends ExecutionResource {

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

}
