package org.qcri.rheem.core.plan;

import org.qcri.rheem.core.platform.Platform;

/**
 * An execution operator is handled by a certain platform.
 */
public interface ExecutionOperator extends ActualOperator {

    /**
     * @return the platform that can run this operator
     */
    Platform getPlatform();

    /**
     * @return a copy of this instance; it's {@link Slot}s will not be connected
     */
    ExecutionOperator copy();

}
