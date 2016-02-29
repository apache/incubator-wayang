package org.qcri.rheem.core.plan.rheemplan.test;


import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.util.Optional;

/**
 * Join-like operator.
 */
public class TestJoin<In1, In2, Out> extends OperatorBase implements ActualOperator {

    public TestJoin(DataSetType<In1> inputType1, DataSetType<In2> inputType2, DataSetType<Out> outputType) {
        super(2, 1, false, null);
        this.inputSlots[0] = new InputSlot<>("input0", this, inputType1);
        this.inputSlots[1] = new InputSlot<>("input1", this, inputType2);
        this.outputSlots[0] = new OutputSlot<>("output", this, outputType);
    }

    public TestJoin(Class<In1> input1TypeClass, Class<In2> input2TypeClass, Class<Out> outputTypeClass) {
        this(
                DataSetType.createDefault(DataUnitType.createBasic(input1TypeClass)),
                DataSetType.createDefault(DataUnitType.createBasic(input2TypeClass)),
                DataSetType.createDefault(DataUnitType.createBasic(outputTypeClass))
        );
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