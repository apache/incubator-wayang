package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;
import java.util.Random;

/**
 * A random sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.
 */
public class SampleOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected int sampleSize;
    protected long datasetSize;
    protected Random rand;


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
    public SampleOperator(int sampleSize, long datasetSize, DataSetType type) {
        this(sampleSize, type);
        this.datasetSize = datasetSize;
    }


    public DataSetType getType() { return this.getInputType(); }

    public int getSampleSize() { return this.sampleSize; }

    public long getDatasetSize() { return this.datasetSize; }

    @Override
    public Optional<CardinalityEstimator> getCardinalityEstimator(//TODO
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(sampleSize));
    }
}
