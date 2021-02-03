package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * This operators attaches a unique ID to each input data quantum.
 */
public class ZipWithIdOperator<InputType> extends UnaryToUnaryOperator<InputType, Tuple2<Long, InputType>> {

    /**
     * Creates a new instance.
     */
    public ZipWithIdOperator(Class<InputType> inputTypeClass) {
        this(DataSetType.createDefault(inputTypeClass));
    }

    /**
     * Creates a new instance.
     */
    public ZipWithIdOperator(DataSetType<InputType> inputType) {
        super(inputType, DataSetType.createDefaultUnchecked(Tuple2.class), false);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ZipWithIdOperator(ZipWithIdOperator<InputType> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(
                1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]
        ));
    }
}
