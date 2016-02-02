package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * Dummy sink for testing purposes.
 */
public class TestSink<T> extends UnarySink<T> {

    public static final int OUTPUT_SIZE = 100;

    public TestSink(DataSetType<T> inputType) {
        super(inputType, null);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex) {
        return Optional.of(new FixedSizeCardinalityEstimator(OUTPUT_SIZE));
    }
}
