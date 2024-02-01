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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;


/**
 * Spark implementation of the {@link SparkBernoulliSampleOperator}.
 */
public class SparkBernoulliSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkBernoulliSampleOperator(FunctionDescriptor.SerializableIntUnaryOperator sampleSizeFunction, DataSetType<Type> type, FunctionDescriptor.SerializableLongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.BERNOULLI, seedFunction);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkBernoulliSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.BERNOULLI || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<Type> inputRdd = input.provideRdd();
        long datasetSize = this.isDataSetSizeKnown() ? this.getDatasetSize() : inputRdd.count();
        int sampleSize = this.getSampleSize(operatorContext);
        long seed = this.getSeed(operatorContext);

        double sampleFraction = ((double) sampleSize) / datasetSize;
        final JavaRDD<Type> outputRdd = inputRdd.sample(false, sampleFraction, seed);
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkBernoulliSampleOperator<>(this);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.bernoulli-sample.load";
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return this.isDataSetSizeKnown() ?
                    Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR) :
                    Collections.singletonList(RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }
}
