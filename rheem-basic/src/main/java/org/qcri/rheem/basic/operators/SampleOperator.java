package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;
import java.util.Random;

/**
 * A random sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.
 */
public class SampleOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected int sampleSize;
    protected double sampleFraction;
    protected long totalSize;
    protected Random rand;

    /**
     * Function that this operator applies to the input elements.
     */
    protected PredicateDescriptor<Type> predicateDescriptor = null;

    /**
     * Creates a new instance given the sample size.
     */
    public SampleOperator(int sampleSize, DataSetType type) {
        super(type, type,
                true,
                null);
        this.sampleSize = sampleSize;
        this.rand = new Random();
    }

    /**
     *  Creates a new instance given the sample size and total dataset size.
     */
    public SampleOperator(int sampleSize, long totalSize, DataSetType type) {
        this(sampleSize, type);
        this.totalSize = totalSize;
    }

    /**
     * Creates a new instance given the sample size and predicate descriptor.
     */
    public SampleOperator(int sampleSize, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        this(sampleSize, new PredicateDescriptor<>(predicateDescriptor, BasicDataUnitType.createBasic(typeClass)));
    }

    /**
     * Creates a new instance given the sample size and predicate descriptor.
     */
    public SampleOperator(int sampleSize, PredicateDescriptor<Type> predicateDescriptor) {
        super(DataSetType.createDefault(predicateDescriptor.getInputType()),
                DataSetType.createDefault(predicateDescriptor.getInputType()),
                true,
                null);
        this.predicateDescriptor = predicateDescriptor;
        this.sampleSize = sampleSize;
        rand = new Random();
    }


    /**
     * Creates a new instance given the fraction of sample.
     */
    public SampleOperator(double sampleFraction, DataSetType type) {
        super(type, type,
                true,
                null);
        this.sampleFraction = sampleFraction;
        this.rand = new Random();
    }

    /**
     * Creates a new instance given the fraction of sample.
     */
    public SampleOperator(double sampleFraction, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        this(sampleFraction, new PredicateDescriptor<>(predicateDescriptor, BasicDataUnitType.createBasic(typeClass)));
    }

    /**
     * Creates a new instance given the sample size and predicate descriptor.
     */
    public SampleOperator(double sampleFraction, PredicateDescriptor<Type> predicateDescriptor) {
        super(DataSetType.createDefault(predicateDescriptor.getInputType()),
                DataSetType.createDefault(predicateDescriptor.getInputType()),
                true,
                null);
        this.predicateDescriptor = predicateDescriptor;
        this.sampleFraction = sampleFraction;
        rand = new Random();
    }

    public PredicateDescriptor<Type> getPredicateDescriptor() {
        return this.predicateDescriptor;
    }

    public DataSetType getType() { return this.getInputType(); }

    public int getSampleSize() { return this.sampleSize; }

    public double getSampleFraction() { return this.sampleFraction; }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(//TODO
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }
}
