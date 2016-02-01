package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the count of elements in this stream.
 */
public class CountOperator<Type> extends UnaryToUnaryOperator<Type, Long> {


    /**
     * Creates a new instance.
     *
     * @param type type of the stream elements
     */
    public CountOperator(DataSetType<Type> type) {
        super(type, DataSetType.createDefault(Long.class), null);
    }
}
