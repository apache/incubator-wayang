package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.BinaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;


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

    public CartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1, DataSetType.createDefaultUnchecked(Tuple2.class), true);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CartesianOperator(CartesianOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(
                1d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0] * inputCards[1]
        ));
    }
}
