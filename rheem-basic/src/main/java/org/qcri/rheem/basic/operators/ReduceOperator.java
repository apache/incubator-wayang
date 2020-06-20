package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

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
            DataSetType<? extends Iterable<Type>> inputType,
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
        super(inputType, outputType, true);
        this.reduceDescriptor = reduceDescriptor;
    }


    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed by this operator
     */
    public ReduceOperator(FunctionDescriptor.SerializableBinaryOperator<Type> reduceDescriptor,
                          Class<Type> typeClass) {
        this(new ReduceDescriptor<>(reduceDescriptor, typeClass),
                DataSetType.createDefault(typeClass),
                DataSetType.createDefault(typeClass));
    }




    public ReduceDescriptor<Type> getReduceDescriptor() {
        return this.reduceDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex,
                                                                     Configuration configuration) {
        return Optional.of(new FixedSizeCardinalityEstimator(1L));
    }
}
