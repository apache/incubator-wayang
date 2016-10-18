package org.qcri.rheem.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide logging via {@link org.slf4j.Logger}.
 */
public interface Logging {

    /**
     * Create a {@link Logger} for this instance.
     *
     * @return the {@link Logger}
     */
    default Logger logger() {
        return LoggerFactory.getLogger(this.getClass());
    }

}
