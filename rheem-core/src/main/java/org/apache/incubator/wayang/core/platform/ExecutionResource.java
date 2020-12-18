package org.apache.incubator.wayang.core.platform;

import org.apache.incubator.wayang.core.api.exception.WayangException;
import org.apache.incubator.wayang.core.util.ReferenceCountable;

/**
 * Represents a resource allocated for execution that needs to be released manually.
 */
public interface ExecutionResource extends ReferenceCountable {

    /**
     * Releases the allocated assets of this resource and unregisters it with its {@link Executor} if there is one.
     */
    void dispose() throws WayangException;

}
