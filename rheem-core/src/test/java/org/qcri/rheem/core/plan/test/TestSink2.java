package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.FlatDataSet;

/**
 * Another dummy sink for testing purposes.
 */
public class TestSink2<T> extends UnarySink<T> {

    public TestSink2(FlatDataSet inputType) {
        super(inputType, null);
    }
}