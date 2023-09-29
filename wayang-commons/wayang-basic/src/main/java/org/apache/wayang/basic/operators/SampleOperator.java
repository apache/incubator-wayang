/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * A sample operator randomly selects its inputs from the input slot and pushes that element to the output slot.
 */
public class SampleOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final Logger logger = LogManager.getLogger(this.getClass());

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
        RESERVOIR;
    }

    /**
     * Special dataset size that represents "unknown".
     */
    public static final long UNKNOWN_DATASET_SIZE = -1L;

    /**
     * Generate a random seed.
     */
    public static long randomSeed() {
        return System.nanoTime();
    }

    /**
     * This function determines the sample size by the number of iterations.
     */
    protected IntUnaryOperator sampleSizeFunction;

    /**
     * This function optionally determines the seed by the number of iterations.
     */
    protected LongUnaryOperator seedFunction;

    /**
     * Size of the dataset to be sampled or {@value #UNKNOWN_DATASET_SIZE} if a dataset size is not known.
     */
    protected Long datasetSize = UNKNOWN_DATASET_SIZE;

    private Methods sampleMethod;

    /**
     * Creates a new instance with any sampling method.
     *
     * @param sampleSize size of the sample
     * @param type       {@link DataSetType} of the sampled dataset
     */
    public SampleOperator(Integer sampleSize, DataSetType<Type> type) {
        this(iterationNumber -> sampleSize, type);
    }

    /**
     * Creates a new instance with any sampling method.
     *
     * @param sampleSizeFunction user-specified size of the sample in dependence of the current iteration number
     * @param type               {@link DataSetType} of the sampled dataset
     */
    public SampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type) {
        this(sampleSizeFunction, type, Methods.ANY, iterationNumber -> randomSeed());
    }

    /**
     * Creates a new instance given the sample size and the seed.
     */
    public SampleOperator(Integer sampleSize, DataSetType<Type> type, Methods sampleMethod, long seed) {
        this(iterationNumber -> sampleSize, type, sampleMethod, iterationNumber -> seed);
    }

    /**
     * Creates a new instance given the sample size and the method.
     */
    public SampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, Methods sampleMethod) {
        this(sampleSizeFunction, type, sampleMethod, iterationNumber -> randomSeed());
    }

    /**
     * Creates a new instance given a user-defined sample size.
     */
    public SampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, Methods sampleMethod, long seed) {
        this(sampleSizeFunction, type, sampleMethod, iterationNumber -> seed);
    }

    /**
     * Creates a new instance given user-defined sample size and seed methods.
     */
    public SampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, Methods sampleMethod, LongUnaryOperator seedFunction) {
        super(type, type, true);
        this.sampleSizeFunction = sampleSizeFunction;
        this.sampleMethod = sampleMethod;
        this.seedFunction = seedFunction;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SampleOperator(SampleOperator<Type> that) {
        super(that);
        this.sampleSizeFunction = that.sampleSizeFunction;
        this.seedFunction = that.seedFunction;
        this.sampleMethod = that.getSampleMethod();
        this.datasetSize = that.getDatasetSize();
    }


    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public long getDatasetSize() {
        return this.datasetSize;
    }

    public void setDatasetSize(long datasetSize) {
        this.datasetSize = datasetSize;
    }

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

    public void setSampleMethod(Methods sampleMethod) {
        this.sampleMethod = sampleMethod;
    }

    public void setSeedFunction(LongUnaryOperator seedFunction) {
        this.seedFunction = seedFunction;
    }

    /**
     * Retrieve the sample size for this instance w.r.t. the current iteration.
     *
     * @param operatorContext provides the current iteration number
     * @return the sample size
     */
    protected int getSampleSize(OptimizationContext.OperatorContext operatorContext) {
        assert operatorContext.getOperator() == this;
        final int iterationNumber = operatorContext.getOptimizationContext().getIterationNumber();
        return this.sampleSizeFunction.applyAsInt(iterationNumber);
    }

    /**
     * Retrieve the seed for this instance w.r.t. the current iteration.
     *
     * @param operatorContext provides the current iteration number
     * @return the seed
     */
    protected long getSeed(OptimizationContext.OperatorContext operatorContext) {
        assert operatorContext.getOperator() == this;
        final int iterationNumber = operatorContext.getOptimizationContext().getIterationNumber();
        return this.seedFunction.applyAsLong(iterationNumber);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Incorporate OperatoContext would allow for precise estimation.
        return Optional.of(new FixedSizeCardinalityEstimator(this.sampleSizeFunction.applyAsInt(0)));
    }
}

