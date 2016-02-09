package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.KeyExtractorDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
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

    protected final KeyExtractorDescriptor<Input, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     * @param inputType     class of the input types (i.e., type of {@link #getInput()}
     * @param outputType    class of the output types (i.e., type of {@link #getOutput()}
     */
    public GroupByOperator(KeyExtractorDescriptor<Input, Key> keyDescriptor,
                           DataSetType<Input> inputType, DataSetType<Iterator<Input>> outputType) {
        super(inputType, outputType, null);
        this.keyDescriptor = keyDescriptor;
    }

    public KeyExtractorDescriptor<Input, Key> getKeyDescriptor() {
        return keyDescriptor;
    }
}
