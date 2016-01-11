package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.OneToOneOperator;
import org.qcri.rheem.core.types.FlatDataSet;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class GlobalReduceOperator<Type> extends OneToOneOperator<Type, Type> {

    protected final ReduceDescriptor reduceDescriptor;

    /**
     * Creates a new instance.
     *
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public GlobalReduceOperator(FlatDataSet type,
                                ReduceDescriptor reduceDescriptor) {
        super(type, type);
        this.reduceDescriptor = reduceDescriptor;
    }

    public FlatDataSet getType() {
        return (FlatDataSet) this.getInputType();
    }

    public ReduceDescriptor getReduceDescriptor() {
        return reduceDescriptor;
    }
}
