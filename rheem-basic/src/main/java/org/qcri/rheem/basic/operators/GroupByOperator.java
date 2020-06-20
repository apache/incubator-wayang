package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This is the auxiliary GroupBy operator, i.e., it behaves differently depending on its context. If it is followed
 * by a {@link ReduceOperator} (and akin), it turns that one into a {@link ReduceByOperator}. Otherwise, it corresponds to a
 * {@link MaterializedGroupByOperator}.
 *
 * @see MaterializedGroupByOperator
 * @see ReduceOperator
 */
public class GroupByOperator<Input, Key> extends UnaryToUnaryOperator<Input, Iterable<Input>> {

    protected final TransformationDescriptor<Input, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param keyFunction describes how to extract the key from data units
     * @param typeClass   class of the data quanta to be grouped
     * @param keyClass    class of the extracted keys
     */
    public GroupByOperator(FunctionDescriptor.SerializableFunction<Input, Key> keyFunction,
                           Class<Input> typeClass,
                           Class<Key> keyClass) {
        this(new TransformationDescriptor<>(keyFunction, typeClass, keyClass));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     */
    public GroupByOperator(TransformationDescriptor<Input, Key> keyDescriptor) {
        this(keyDescriptor,
                DataSetType.createDefault(keyDescriptor.getInputType()),
                DataSetType.createGrouped(keyDescriptor.getInputType()));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     * @param inputType     class of the input types (i.e., type of {@link #getInput()}
     * @param outputType    class of the output types (i.e., type of {@link #getOutput()}
     */
    public GroupByOperator(TransformationDescriptor<Input, Key> keyDescriptor,
                           DataSetType<Input> inputType, DataSetType<Iterable<Input>> outputType) {
        super(inputType, outputType, false);
        this.keyDescriptor = keyDescriptor;
    }


    public GroupByOperator(GroupByOperator<Input, Key> that){
        super(that);
        this.keyDescriptor = that.keyDescriptor;
    }

    public TransformationDescriptor<Input, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }


}
