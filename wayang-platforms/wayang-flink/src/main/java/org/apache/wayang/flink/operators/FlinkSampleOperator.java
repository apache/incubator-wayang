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

package org.apache.wayang.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * Flink implementation of the {@link SampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)
 */
public class FlinkSampleOperator<Type>
        extends SampleOperator<Type>
        implements FlinkExecutionOperator {
    private Random rand;


    /**
     * Creates a new instance.
     */
    public FlinkSampleOperator(FunctionDescriptor.SerializableIntUnaryOperator sampleSizeFunction, DataSetType<Type> type, FunctionDescriptor.SerializableLongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.RANDOM, seedFunction);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.RANDOM
                || that.getSampleMethod() == Methods.BERNOULLI
                || that.getSampleMethod() == Methods.RESERVOIR
                || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();


        long size = Long.MAX_VALUE;
      /*  try {
            size = dataSetInput.count();
        } catch (Exception e) {
            throw new WayangException(e);
        }*/
        DataSet<Type> dataSetOutput;

        int sampleSize = this.getSampleSize(operatorContext);
        long seed = this.getSeed(operatorContext);

        if(this.getSampleSize(operatorContext) >= size){
            dataSetOutput = dataSetInput;
        }else {
            double faction = (size / sampleSize) + 0.01d;
            switch (this.getSampleMethod()) {
                case RANDOM:
                    dataSetOutput = DataSetUtils.sampleWithSize(dataSetInput, true, sampleSize, seed);
                    break;
                case ANY:
                    Random rand = new Random(seed);
                    dataSetOutput = dataSetInput.filter(a -> {return rand.nextBoolean();}).first(sampleSize);
                    break;
                case BERNOULLI:
                    dataSetOutput = DataSetUtils.sample(dataSetInput, false, faction, seed).first(sampleSize);
                    break;
                case RESERVOIR:
                    dataSetOutput = DataSetUtils.sampleWithSize(dataSetInput, true, sampleSize, seed);
                    break;
                default:
                    throw new WayangException("The option is not valid");
            }
        }

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkSampleOperator<Type>(this);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.sample.load";
    }


    @Override
    public boolean containsAction() {
        return true;
    }


}
