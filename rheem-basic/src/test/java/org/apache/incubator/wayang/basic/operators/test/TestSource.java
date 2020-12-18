package org.apache.incubator.wayang.basic.operators.test;

import org.apache.incubator.wayang.core.plan.wayangplan.UnarySource;
import org.apache.incubator.wayang.core.types.DataSetType;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    public TestSource(DataSetType outputType) {
        super(outputType);
    }

}
