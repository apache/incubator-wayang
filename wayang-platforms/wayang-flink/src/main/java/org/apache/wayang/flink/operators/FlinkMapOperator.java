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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutionContext;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Flink implementation of the {@link MapOperator}.
 */
public class FlinkMapOperator<InputType, OutputType> extends MapOperator<InputType, OutputType> implements FlinkExecutionOperator {
    /**
     * Creates a new instance.
     */
    public FlinkMapOperator(DataSetType<InputType> inputType,
                           DataSetType<OutputType> outputType,
                           TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkMapOperator(MapOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();
        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<InputType>  dataSetInput  = input.provideDataSet();

        final DataSet<OutputType> dataSetOutput;
        if( this.getNumBroadcastInputs() > 0 ) {
            Tuple<String, DataSet> names = searchBroadcast(inputs);

            FlinkExecutionContext fex = new FlinkExecutionContext(this, inputs, 0);

            RichMapFunction<InputType, OutputType> richFunction = flinkExecutor.compiler
                    .compile(
                            this.functionDescriptor,
                            fex
                    );

            fex.setRichFunction(richFunction);

            dataSetOutput = dataSetInput
                    .map(richFunction)
                    .setParallelism(flinkExecutor.fee.getParallelism())
                    .returns(this.getOutputType().getDataUnitType().getTypeClass())
                    .name(this.getName())
                    .withBroadcastSet(names.field1, names.field0);

        }else {
            final MapFunction<InputType, OutputType> mapper = flinkExecutor.getCompiler().compile(this.functionDescriptor);
            dataSetOutput = dataSetInput.map(mapper)
                .setParallelism(flinkExecutor.fee.getParallelism())
                .returns(this.getOutputType().getDataUnitType().getTypeClass()).name(this.getName());
        }
        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Tuple<String, DataSet> searchBroadcast(ChannelInstance[] inputs) {
        for(int i = 0; i < this.inputSlots.length; i++){
            if( this.inputSlots[i].isBroadcast() ){
                DataSetChannel.Instance dataSetChannel = (DataSetChannel.Instance)inputs[inputSlots[i].getIndex()];
                return new Tuple<>(inputSlots[i].getName(), dataSetChannel.provideDataSet());
            }
        }
        return null;
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.map.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }
}
