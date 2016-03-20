package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;

/**
 * Represents a resource allocated for execution that needs to be released manually.
 */
public interface ExecutionResource {

    /**
     * Releases the allocated assets of this resource.
     */
    void release() throws RheemException;

}
