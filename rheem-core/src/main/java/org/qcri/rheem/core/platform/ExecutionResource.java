package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;

/**
 * Represents a resource allocated for execution that needs to be released manually.
 */
public interface ExecutionResource {

    /**
     * Releases the allocated assets of this resource and unregisters it with its {@link Executor} if there is one.
     */
    void dispose() throws RheemException;

    /**
     * An instance can be associated to an {@link Executor} that maintains it.
     *
     * @return the associated {@link Executor} or {@code null} if noen
     */
    Executor getExecutor();

}
