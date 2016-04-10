package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * A sample operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a probability to it, and decides whether it will be in the output and pushes that element to the output slot.
 */
public class SampleOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final double fraction;

    /**
     * Function that this operator applies to the input elements.
     */
    protected final PredicateDescriptor<Type> predicateDescriptor;

    /**
     * Creates a new instance.
     */
    public SampleOperator(double fraction, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        this(fraction, new PredicateDescriptor<>(predicateDescriptor, BasicDataUnitType.createBasic(typeClass)));
    }

    /**
     * Creates a new instance.
     */
    public SampleOperator(double fraction, PredicateDescriptor<Type> predicateDescriptor) {
        super(DataSetType.createDefault(predicateDescriptor.getInputType()),
                DataSetType.createDefault(predicateDescriptor.getInputType()),
                true,
                null);
        this.predicateDescriptor = predicateDescriptor;
        this.fraction = fraction;
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public SampleOperator(double fraction, DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        this(fraction, new PredicateDescriptor<>(predicateDescriptor, (BasicDataUnitType) type.getDataUnitType()), type);
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public SampleOperator(double fraction, PredicateDescriptor<Type> predicateDescriptor, DataSetType<Type> type) {
        super(type, type, true, null);
        this.predicateDescriptor = predicateDescriptor;
        this.fraction = fraction;
    }

    public PredicateDescriptor<Type> getPredicateDescriptor() {
        return this.predicateDescriptor;
    }

    public DataSetType getType() { return this.getInputType(); }

    public double getFraction() { return this.fraction; }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }
}
