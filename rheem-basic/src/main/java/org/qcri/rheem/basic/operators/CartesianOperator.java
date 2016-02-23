package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class CartesianOperator<InputType0, InputType1>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    public CartesianOperator(Class<InputType0> inputType0Class, Class<InputType1> inputType1Class) {
        super(DataSetType.createDefault(inputType0Class),
                DataSetType.createDefault(inputType1Class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                true);
    }

    public CartesianOperator(DataSetType<InputType0> inputType0, DataSetType inputType1) {
        super(inputType0, inputType1, DataSetType.createDefaultUnchecked(Tuple2.class), true);
    }
}
