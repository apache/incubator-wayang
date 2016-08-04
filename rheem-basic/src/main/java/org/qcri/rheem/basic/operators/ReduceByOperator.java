package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class ReduceByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final TransformationDescriptor<Type, Key> keyDescriptor;

    protected final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     */
    public ReduceByOperator(FunctionDescriptor.SerializableFunction<Type, Key> keyFunction,
                            FunctionDescriptor.SerializableBinaryOperator<Type> reduceDescriptor,
                            Class<Key> keyClass,
                            Class<Type> typeClass) {
        this(new TransformationDescriptor<>(keyFunction, typeClass, keyClass),
                new ReduceDescriptor<>(reduceDescriptor, typeClass));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public ReduceByOperator(TransformationDescriptor<Type, Key> keyDescriptor,
                            ReduceDescriptor<Type> reduceDescriptor) {
        this(keyDescriptor, reduceDescriptor, DataSetType.createDefault(keyDescriptor.getInputType()));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     */
    public ReduceByOperator(TransformationDescriptor<Type, Key> keyDescriptor,
                            ReduceDescriptor<Type> reduceDescriptor,
                            DataSetType<Type> type) {
        super(type, type, true);
        this.keyDescriptor = keyDescriptor;
        this.reduceDescriptor = reduceDescriptor;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ReduceByOperator(ReduceByOperator<Type, Key> that) {
        super(that);
        this.keyDescriptor = that.getKeyDescriptor();
        this.reduceDescriptor = that.getReduceDescriptor();
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return this.reduceDescriptor;
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new DefaultCardinalityEstimator(
                0.5d,
                1,
                this.isSupportingBroadcastInputs(),
                inputCards -> (long) (inputCards[0] * 0.1)));
    }
}
