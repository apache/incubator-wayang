package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class JoinOperator<InputType0, InputType1, Key>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    private static <InputType0, InputType1> DataSetType<Tuple2<InputType0, InputType1>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    protected final TransformationDescriptor<InputType0, Key> keyDescriptor0;

    protected final TransformationDescriptor<InputType1, Key> keyDescriptor1;

    public JoinOperator(DataSetType<InputType0> inputType0,
                        DataSetType<InputType1> inputType1,
                        TransformationDescriptor<InputType0, Key> keyDescriptor0,
                        TransformationDescriptor<InputType1, Key> keyDescriptor1) {
        super(inputType0, inputType1, JoinOperator.<InputType0, InputType1>createOutputDataSetType());
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;

    }

    public TransformationDescriptor<InputType0, Key> getKeyDescriptor0() {
        return this.keyDescriptor0;
    }

    public TransformationDescriptor<InputType1, Key> getKeyDescriptor1() {
        return this.keyDescriptor1;
    }
}
