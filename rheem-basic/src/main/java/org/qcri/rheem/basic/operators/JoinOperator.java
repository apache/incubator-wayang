package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.KeyExtractorDescriptor;
import org.qcri.rheem.core.plan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;


/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class JoinOperator<InputType0, InputType1, Key>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    protected final KeyExtractorDescriptor<InputType0, Key> keyDescriptor0;
    protected final KeyExtractorDescriptor<InputType1, Key> keyDescriptor1;


    public JoinOperator(DataSetType <InputType0> inputType0, DataSetType inputType1,
                        KeyExtractorDescriptor<InputType0, Key> keyDescriptor0,
                        KeyExtractorDescriptor<InputType1, Key> keyDescriptor1) {
        super(inputType0, inputType1, DataSetType.createDefaultUnchecked(Tuple2.class), null);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }

    public KeyExtractorDescriptor<InputType0, Key> getKeyDescriptor0() {
        return keyDescriptor0;
    }

    public KeyExtractorDescriptor<InputType1, Key> getKeyDescriptor1() {
        return keyDescriptor1;
    }
}
