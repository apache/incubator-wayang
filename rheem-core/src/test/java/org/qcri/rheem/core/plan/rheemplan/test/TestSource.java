package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Optional;

/**
 * Dummy source for testing purposes.
 */
public class TestSource<T> extends UnarySource<T> {

    private CardinalityEstimator cardinalityEstimator = new FixedSizeCardinalityEstimator(100);

    public TestSource(DataSetType outputType) {
        super(outputType);
    }

    public TestSource(Class<?> dataQuantumClass) {
        this(DataSetType.createDefault(DataUnitType.createBasic(dataQuantumClass)));
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        return Optional.ofNullable(this.cardinalityEstimator);
    }

    public void setCardinalityEstimators(CardinalityEstimator cardinalityEstimators) {
        this.cardinalityEstimator = cardinalityEstimators;
    }
}
