package org.qcri.rheem.basic.operators.test;

import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSet;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    public TestSource(DataSet outputType) {
        super(outputType, null);
    }

}
