package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Optional;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink<T> extends UnarySink<T> {

    public TestSink(DataSetType<T> inputType) {
        super(inputType);
    }

    public TestSink(Class<T> typeClass) {
        this(DataSetType.createDefault(DataUnitType.createBasic(typeClass)));
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        throw new RuntimeException();
    }
}
