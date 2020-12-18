package io.rheem.rheem.core.plan.rheemplan.test;

import io.rheem.rheem.core.plan.rheemplan.UnarySink;
import io.rheem.rheem.core.types.DataSetType;

/**
 * Another dummy sink for testing purposes.
 */
public class TestSink2<T> extends UnarySink<T> {

    public TestSink2(DataSetType<T> inputType) {
        super(inputType);
    }
}
