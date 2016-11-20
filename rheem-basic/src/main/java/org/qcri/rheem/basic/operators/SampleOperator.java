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
        /**
         * Represents an arbitrary sampling method.
         */
        ANY,
        /**
         * Bernoulli sampling.
         */
        BERNOULLI,
        /**
         * Randomly pick a sample.
         */
        RANDOM,
        /**
         * Shuffle a data partition first, then sequentially take the sample from this partition.
         */
        SHUFFLE_PARTITION_FIRST,
        /**
         * Reservoir sampling.
         */
        RESERVOIR
    }

    /**
     * Special dataset size that represents "unknown".
     */
    // TODO: With 0 being a legal dataset size, it would be nice to use a different "null" value, e.g., -1.
    public static final long UNKNOWN_DATASET_SIZE = 0L;

    public static final UDFSampleSize UNKNOWN_UDF_SAMPLE_SIZE = null;

    /**
     * Default seed value.
     */
    public static final long DEFAULT_SEED = System.nanoTime();

    protected int sampleSize;

    protected UDFSampleSize udfSampleSize;

    /**
     * Optionally sets the seed for the sample.
     */
    protected long seed;

    /**
     * Size of the dataset to be sampled or {@code 0} if a dataset size is not known.
     */
    protected long datasetSize;

    private Methods sampleMethod;

    /**
     * Creates a new instance with any sampling method.
     *
     * @param udfSampleSize user-specified size of the sample
     * @param type       {@link DataSetType} of the sampled dataset
     */
    public SampleOperator(UDFSampleSize udfSampleSize, DataSetType<Type> type) {
        this(udfSampleSize, type, Methods.ANY);
    }

    /**
     * Creates a new instance with any sampling method.
     *
     * @param sampleSize size of the sample
     * @param type       {@link DataSetType} of the sampled dataset
     */
    public SampleOperator(int sampleSize, DataSetType<Type> type) {
        this(sampleSize, type, Methods.ANY);
    }

    /**
     * Creates a new instance given the sample size.
     */
    public SampleOperator(int sampleSize, DataSetType<Type> type, Methods sampleMethod) {
        this(sampleSize, UNKNOWN_DATASET_SIZE, type, sampleMethod);
    }

    /**
     * Creates a new instance given a user-defined sample size method.
     */
    public SampleOperator(UDFSampleSize udfSampleSize, DataSetType<Type> type, Methods sampleMethod) {
        this(udfSampleSize, UNKNOWN_DATASET_SIZE, type, sampleMethod);
    }

    /**
     * Creates a new instance given the sample size and total dataset size.
     */
    public SampleOperator(int sampleSize, long datasetSize, DataSetType<Type> type, Methods sampleMethod) {
        this(sampleSize, datasetSize, DEFAULT_SEED, type, sampleMethod);
    }

    /**
     * Creates a new instance given a user-defined sample size method and total dataset size.
     */
    public SampleOperator(UDFSampleSize udfSampleSize, long datasetSize, DataSetType<Type> type, Methods sampleMethod) {
        this(udfSampleSize, datasetSize, DEFAULT_SEED, type, sampleMethod);
    }

    /**
     * Creates a new instance given the sample size, total dataset size and seed.
     */
    public SampleOperator(int sampleSize, long datasetSize, long seed, DataSetType<Type> type, Methods sampleMethod) {
        super(type, type, true);
        this.sampleSize = sampleSize;
        this.datasetSize = datasetSize;
        this.seed = seed;
        this.sampleMethod = sampleMethod;
    }

    /**
     * Creates a new instance given a user-defined sample size method, total dataset size and seed.
     */
    public SampleOperator(UDFSampleSize udfSampleSize, long datasetSize, long seed, DataSetType<Type> type, Methods sampleMethod) {
        super(type, type, true);
        this.udfSampleSize = udfSampleSize;
        this.datasetSize = datasetSize;
        this.seed = seed;
        this.sampleMethod = sampleMethod;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SampleOperator(SampleOperator<Type> that) {
        super(that);
        this.sampleSize = that.getSampleSize();
        this.udfSampleSize = that.getUDFSampleSize();
        this.sampleMethod = that.getSampleMethod();
        this.datasetSize = that.getDatasetSize();
        this.seed = that.getSeed();
    }


    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public int getSampleSize() {
        return this.sampleSize;
    }

    public UDFSampleSize getUDFSampleSize() {
        return this.udfSampleSize;
    }

    public long getDatasetSize() {
        return this.datasetSize;
    }

    public void setDatasetSize(long datasetSize) { this.datasetSize = datasetSize; }

    public long getSeed() {
        return this.seed;
    }

    public void setSeed(long seed) { this.seed = seed; }

    /**
     * Find out whether this instance knows about the size of the incoming dataset.
     *
     * @return whether it knows the dataset size
     */
    protected boolean isDataSetSizeKnown() {
        return this.datasetSize > 0;
    }

    public Methods getSampleMethod() {
        return this.sampleMethod;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FixedSizeCardinalityEstimator(sampleSize));
    }
}

