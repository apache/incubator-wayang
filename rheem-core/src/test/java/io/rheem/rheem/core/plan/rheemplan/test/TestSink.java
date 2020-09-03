package io.rheem.rheem.core.plan.rheemplan.test;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.rheem.core.plan.rheemplan.UnarySink;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.types.DataUnitType;

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
