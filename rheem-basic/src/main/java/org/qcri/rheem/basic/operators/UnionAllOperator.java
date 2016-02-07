package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the union of all elements in these datasets(duplicates are not remmoved).
 */
public class UnionAllOperator<Type>
        extends BinaryToUnaryOperator<Type, Type, Type> {



    public UnionAllOperator(DataSetType<Type> type) {
        super(type, type, type);
    }
}
