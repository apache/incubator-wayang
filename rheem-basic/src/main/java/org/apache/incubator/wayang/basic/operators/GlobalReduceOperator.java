package io.rheem.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.function.FunctionDescriptor;
import io.rheem.rheem.core.function.ReduceDescriptor;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import io.rheem.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import io.rheem.rheem.core.types.BasicDataUnitType;
import io.rheem.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class GlobalReduceOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final ReduceDescriptor<Type> reduceDescriptor;

    /**
     * Creates a new instance.
     */
    public GlobalReduceOperator(FunctionDescriptor.SerializableBinaryOperator<Type> reduceFunction,
                                Class<Type> typeClass) {
        this(new ReduceDescriptor<>(reduceFunction, typeClass));
    }

    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor) {
        this(reduceDescriptor, DataSetType.createDefault(
                (BasicDataUnitType<Type>) reduceDescriptor.getInputType().getBaseType()));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public GlobalReduceOperator(GlobalReduceOperator<Type> that) {
        super(that);
        this.reduceDescriptor = that.reduceDescriptor;
    }


    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed on the elements
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     */
    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor, DataSetType<Type> type) {
        super(type, type, true);
        this.reduceDescriptor = reduceDescriptor;
    }


    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return this.reduceDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }
}
