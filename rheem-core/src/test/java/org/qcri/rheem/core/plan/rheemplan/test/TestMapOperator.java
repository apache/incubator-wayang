package org.qcri.rheem.core.plan.rheemplan.test;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * Test operator that exposes map-like behavior.
 */
public class TestMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {
    /**
     * Creates a new instance.
     */
    public TestMapOperator(DataSetType inputType, DataSetType outputType) {
        super(inputType, outputType, true, null);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex,
                                                                  Configuration configuration) {
        Validate.isTrue(outputIndex == 0);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, true, cards -> cards[0]));
    }
}
