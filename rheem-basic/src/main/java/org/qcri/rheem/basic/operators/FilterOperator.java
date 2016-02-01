package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.function.Predicate;


/**
 * This operator returns a new dataset after filtering by applying predicate.
 */
public class FilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {


    /**
     * Function that this operator applies to the input elements.
     */
    protected final Predicate<Type> predicate;

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public FilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {

        super(type, type, null);
        this.predicate = predicate;
    }

    public Predicate<Type> getFunctionDescriptor() {
        return predicate;
    }
}
