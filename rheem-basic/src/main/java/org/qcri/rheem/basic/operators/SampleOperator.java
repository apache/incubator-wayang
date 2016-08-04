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

    protected int sampleSize;
    protected long datasetSize;

    protected Methods sampleMethod;

    /**
     * Creates a new instance given the sample size.
     */
    public SampleOperator(int sampleSize, DataSetType<Type> type, Methods sampleMethod) {
        super(type, type,
                true);
        this.sampleSize = sampleSize;
        this.sampleMethod = sampleMethod;
    }

    /**
     *  Creates a new instance given the sample size and total dataset size.
     */
    public SampleOperator(int sampleSize, long datasetSize, DataSetType<Type> type, Methods sampleMethod) {
        this(sampleSize, type, sampleMethod);
        this.datasetSize = datasetSize;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SampleOperator(SampleOperator<Type> that) {
        super(that);
        this.sampleSize = that.getSampleSize();
        this.sampleMethod = that.getSampleMethod();
        this.datasetSize = that.getDatasetSize();
    }


    public DataSetType getType() { return this.getInputType(); }

    public int getSampleSize() { return this.sampleSize; }

    public long getDatasetSize() { return this.datasetSize; }

    public Methods getSampleMethod() { return this.sampleMethod; }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(sampleSize));
    }
}

