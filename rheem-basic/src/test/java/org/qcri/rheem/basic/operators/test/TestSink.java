package org.qcri.rheem.basic.operators.test;

import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.FlatDataSet;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink<T> extends UnarySink<T> {

    public TestSink(FlatDataSet inputType) {
        super(inputType, null);
    }

}