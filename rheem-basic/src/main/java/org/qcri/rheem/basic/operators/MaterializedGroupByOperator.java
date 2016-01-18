package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Iterator;

/**
 * This operator collocates the data units in a data set w.r.t. a key function.
 */
public class MaterializedGroupByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Iterator<Type>> {

    protected final TransformationDescriptor keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     */
    public MaterializedGroupByOperator(DataSetType<Type> type,
                                       TransformationDescriptor<Type, Key> keyDescriptor) {
        super(type, type, null);
        this.keyDescriptor = keyDescriptor;
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public TransformationDescriptor<Type, Key> getKeyDescriptor() {
        return keyDescriptor;
    }

}
