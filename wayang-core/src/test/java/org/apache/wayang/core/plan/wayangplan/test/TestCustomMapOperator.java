package org.apache.wayang.core.plan.wayangplan.test;

import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * Test operator that exposes map-like behavior. Does not provide a {@link CardinalityEstimator}.
 */
public class TestCustomMapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {
    /**
     * Creates a new instance.
     */
    public TestCustomMapOperator(DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
    }

}
