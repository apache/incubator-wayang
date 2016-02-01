package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the distinct elements in this dataset.
 */
public class DistinctOperator<Type> extends UnaryToUnaryOperator<Type, Type> {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public DistinctOperator(DataSetType<Type> type) {
        super(type, type, null);
    }
}
