package org.qcri.rheem.core.plan.rheemplan.test;


import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * Join-like operator.
 */
public class TestJoin<In1, In2, Out> extends OperatorBase implements ActualOperator {

    public TestJoin(DataSetType<In1> inputType1, DataSetType<In2> inputType2, DataSetType<Out> outputType) {
        super(2, 1, false, null);
        this.inputSlots[0] = new InputSlot<>("in1", this, inputType1);
        this.inputSlots[1] = new InputSlot<>("in2", this, inputType2);
        this.outputSlots[0] = new OutputSlot<>("out", this, outputType);
    }

    @Override
    public <Payload, Return> Return accept(TopDownPlanVisitor<Payload, Return> visitor, OutputSlot<?> outputSlot, Payload payload) {
        return visitor.visit(this, outputSlot, payload);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex,
                                                                  Configuration configuration) {
        return Optional.of(new DefaultCardinalityEstimator(0.7d, 2, false, (cards) -> cards[0] * cards[1]));
    }
}