package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the set intersection of elements of input datasets.
 */
public class IntersectOperator<Type> extends BinaryToUnaryOperator<Type, Type, Type> {

    public IntersectOperator(Class<Type> typeClass) {
        this(DataSetType.createDefault(typeClass));
    }

    public IntersectOperator(DataSetType<Type> dataSetType) {
        super(dataSetType, dataSetType, dataSetType, false);
    }

}
