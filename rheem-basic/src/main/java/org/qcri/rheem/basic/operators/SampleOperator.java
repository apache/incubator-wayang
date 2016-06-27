package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;

/**
 * A random sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.
 */
public class SampleOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    public enum Methods {
        BERNOULLI, //Bernoulli sampling
        RANDOM, // Randomly pick a sample
        SHUFFLE_FIRST, // shuffle data first and then take sequentially the sample
        RESERVOIR //reservoir sampling
    }

    protected Integer sampleSize = 0;
    protected Long datasetSize = 0L;

    protected Methods sampleMethod;

    /**
     * Creates a new instance given the sample size.
     */
    public SampleOperator(Integer sampleSize, DataSetType<Type> type, Methods sampleMethod) {
        super(type, type,
                true,
                null);
        this.sampleSize = sampleSize;
        this.sampleMethod = sampleMethod;
    }

    /**
     *  Creates a new instance given the sample size and total dataset size.
     */
    public SampleOperator(Integer sampleSize, Long datasetSize, DataSetType<Type> type, Methods sampleMethod) {
        this(sampleSize, type, sampleMethod);
        this.datasetSize = datasetSize;
    }


    public DataSetType getType() { return this.getInputType(); }

    public int getSampleSize() { return this.sampleSize; }

    public long getDatasetSize() { return this.datasetSize; }

    public Methods getSampleMethod() { return this.sampleMethod; }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(sampleSize));
    }
}

