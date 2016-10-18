package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

/**
 * Another dummy sink for testing purposes.
 */
public class TestSink2<T> extends UnarySink<T> {

    public TestSink2(DataSetType<T> inputType) {
        super(inputType);
    }
}