package io.rheem.rheem.core.plan.rheemplan.test;

import org.apache.commons.lang3.Validate;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import io.rheem.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.types.DataUnitType;

import java.util.Optional;

/**
 * Test operator that exposes map-like behavior.
 */
public class TestMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {
    /**
     * Creates a new instance.
     */
    public TestMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
    }

    public TestMapOperator(Class<InputType> inputTypeClass, Class<OutputType> outputTypeClass) {
        this(
                DataSetType.createDefault(DataUnitType.createBasic(inputTypeClass)),
                DataSetType.createDefault(DataUnitType.createBasic(outputTypeClass))
        );
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        Validate.isTrue(outputIndex == 0);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, true, cards -> cards[0]));
    }

    @Override
    public boolean isSupportingBroadcastInputs() {
        return true;
    }
}
