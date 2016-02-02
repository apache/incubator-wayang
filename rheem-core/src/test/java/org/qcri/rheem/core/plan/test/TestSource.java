package org.qcri.rheem.core.plan.test;

import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Map;
import java.util.Optional;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    public TestSource(DataSetType outputType) {
        super(outputType, null);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex, Map<OutputSlot<?>, CardinalityEstimate> cache) {
        return Optional.of(new FixedSizeCardinalityEstimator(100, this.getOutput(outputIndex), cache));
    }
}
