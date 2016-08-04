package org.qcri.rheem.basic.operators.test;

import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    public TestSource(DataSetType outputType) {
        super(outputType);
    }

}
