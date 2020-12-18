package org.apache.incubator.wayang.core.plan.wayangplan.test;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySink;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.types.DataUnitType;

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
