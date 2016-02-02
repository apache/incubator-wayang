package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.DefaultCardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class GlobalReduceOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     *
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public GlobalReduceOperator(DataSetType<Type> type,
                                ReduceDescriptor<Type> reduceDescriptor) {
        super(type, type, null);
        this.reduceDescriptor = reduceDescriptor;
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return reduceDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(int outputIndex) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }}
