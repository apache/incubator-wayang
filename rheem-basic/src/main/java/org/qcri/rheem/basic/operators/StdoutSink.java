package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This sink prints all incoming data units to the {@code stdout}.
 */
public class StdoutSink<T> extends UnarySink<T> {

    public StdoutSink(DataSetType<T> inputType) {
        super(inputType, null);
    }

}
