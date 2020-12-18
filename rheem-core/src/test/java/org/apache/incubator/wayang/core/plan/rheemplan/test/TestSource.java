package org.apache.incubator.wayang.core.plan.wayangplan.test;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.incubator.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySource;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.core.types.DataUnitType;

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
