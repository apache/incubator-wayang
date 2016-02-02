package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * This sink prints all incoming data units to the {@code stdout}.
 */
public class StdoutSink<T> extends UnarySink<T> {

    public StdoutSink(DataSetType<T> inputType) {
        super(inputType, null);
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return super.getCardinalityEstimator(outputIndex);
    }

}
