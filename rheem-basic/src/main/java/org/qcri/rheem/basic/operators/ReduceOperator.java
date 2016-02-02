package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * This operator is context dependent: after a {@link GroupByOperator}, it is meant to be a {@link ReduceByOperator};
 * otherwise, it is a {@link GlobalReduceOperator}.
 */
public class ReduceOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    private final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * @deprecated This method is just a hack that is necessary because of the ambiguous nature of this operator.
     */
    public static <Type> ReduceOperator<Type> createGroupedReduce(
            ReduceDescriptor<Type> reduceDescriptor,
            DataSetType<? extends Iterator<Type>> inputType,
            DataSetType<Type> outputType) {
        return new ReduceOperator<>(reduceDescriptor, (DataSetType<Type>) inputType, outputType);
    }

    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed by this operator
     */
    public ReduceOperator(ReduceDescriptor<Type> reduceDescriptor,
                          DataSetType<Type> inputType, DataSetType<Type> outputType) {
        super(inputType, outputType, null);
        this.reduceDescriptor = reduceDescriptor;
    }


    public ReduceDescriptor<Type> getReduceDescriptor() {
        return reduceDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex,
                                                                  Map<OutputSlot<?>, CardinalityEstimate> cache) {
        return Optional.of(new FixedSizeCardinalityEstimator(1L, this.getOutput(outputIndex), cache));
    }
}
