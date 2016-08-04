package org.qcri.rheem.core.plan.rheemplan.test;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

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
