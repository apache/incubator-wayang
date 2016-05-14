package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.util.ReferenceCountable;

/**
 * Represents a resource allocated for execution that needs to be released manually.
 */
public interface ExecutionResource extends ReferenceCountable {

    /**
     * Releases the allocated assets of this resource and unregisters it with its {@link Executor} if there is one.
     */
    void dispose() throws RheemException;

}
