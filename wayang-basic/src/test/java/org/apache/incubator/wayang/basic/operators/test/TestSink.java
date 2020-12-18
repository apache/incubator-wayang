package org.apache.incubator.wayang.basic.operators.test;

import org.apache.incubator.wayang.core.plan.wayangplan.UnarySink;
import org.apache.incubator.wayang.core.types.DataSetType;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink<T> extends UnarySink<T> {

    public TestSink(DataSetType<T> inputType) {
        super(inputType);
    }

}
