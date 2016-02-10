package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;

/**
 * This is the auxiliary GroupBy operator, i.e., it behaves differently depending on its context. If it is followed
 * by a {@link ReduceOperator} (and akin), it turns that one into a {@link ReduceByOperator}. Otherwise, it corresponds to a
 * {@link MaterializedGroupByOperator}.
 *
 * @see MaterializedGroupByOperator
 * @see ReduceOperator
 */
public class GroupByOperator<Input, Key> extends UnaryToUnaryOperator<Input, Iterator<Input>> {

    protected final TransformationDescriptor<Input, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     * @param inputType     class of the input types (i.e., type of {@link #getInput()}
     * @param outputType    class of the output types (i.e., type of {@link #getOutput()}
     */
    public GroupByOperator(TransformationDescriptor<Input, Key> keyDescriptor,
                           DataSetType<Input> inputType, DataSetType<Iterator<Input>> outputType) {
        super(inputType, outputType, null);
        this.keyDescriptor = keyDescriptor;
    }

    public TransformationDescriptor<Input, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }

}
