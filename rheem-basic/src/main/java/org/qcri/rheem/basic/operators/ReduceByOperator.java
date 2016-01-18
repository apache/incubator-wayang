package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class ReduceByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final TransformationDescriptor<Type, Key> keyDescriptor;

    protected final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     *
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public ReduceByOperator(DataSetType<Type> type,
                            TransformationDescriptor<Type, Key> keyDescriptor,
                            ReduceDescriptor<Type> reduceDescriptor) {
        super(type, type, null);
        this.keyDescriptor = keyDescriptor;
        this.reduceDescriptor = reduceDescriptor;
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return keyDescriptor;
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return reduceDescriptor;
    }
}
