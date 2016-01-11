package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.OneToOneOperator;
import org.qcri.rheem.core.types.DataSet;

/**
 * This operator is context dependent: after a {@link GroupByOperator}, it is meant to be a {@link ReduceByOperator};
 * otherwise, it is a {@link GlobalReduceOperator}.
 */
public class ReduceOperator<Type> extends OneToOneOperator<Type, Type> {

    private final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed by this operator
     */
    public ReduceOperator(ReduceDescriptor<Type> reduceDescriptor,
                          DataSet inputType, DataSet outputType) {
        super(inputType, outputType);
        this.reduceDescriptor = reduceDescriptor;
    }

    public ReduceDescriptor getReduceDescriptor() {
        return reduceDescriptor;
    }
}
