package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.BinaryToUnaryOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OperatorContainer;
import org.qcri.rheem.core.types.DataSetType;

/**
 * This {@link Operator} creates the union (bag semantics) of two .
 */
public class CoalesceOperator<Type> extends BinaryToUnaryOperator<Type, Type, Type> {
    /**
     * Creates a new instance.
     *
     * @param type      the type of the datasets to be coalesced
     */
    public CoalesceOperator(DataSetType<Type> type) {
        super(type, type, type);
    }
}
