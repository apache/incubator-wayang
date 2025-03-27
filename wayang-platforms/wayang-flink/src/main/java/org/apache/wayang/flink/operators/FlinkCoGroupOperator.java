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

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link CoGroupOperator}.
 */
public class FlinkCoGroupOperator<InputType0, InputType1, TypeKey>
        extends CoGroupOperator<InputType0, InputType1, TypeKey>
        implements FlinkExecutionOperator {
    /**
     * @see CoGroupOperator#CoGroupOperator(FunctionDescriptor.SerializableFunction, FunctionDescriptor.SerializableFunction, Class, Class, Class)
     */
    public FlinkCoGroupOperator(FunctionDescriptor.SerializableFunction<InputType0, TypeKey> keyExtractor0,
                                FunctionDescriptor.SerializableFunction<InputType1, TypeKey> keyExtractor1,
                                Class<InputType0> input0Class,
                                Class<InputType1> input1Class,
                                Class<TypeKey> keyClass) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, keyClass);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor, TransformationDescriptor)
     */
    public FlinkCoGroupOperator(TransformationDescriptor<InputType0, TypeKey> keyDescriptor0,
                                TransformationDescriptor<InputType1, TypeKey> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor, TransformationDescriptor, DataSetType, DataSetType)
     */
    public FlinkCoGroupOperator(TransformationDescriptor<InputType0, TypeKey> keyDescriptor0,
                                TransformationDescriptor<InputType1, TypeKey> keyDescriptor1,
                                DataSetType<InputType0> inputType0,
                                DataSetType<InputType1> inputType1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(CoGroupOperator)
     */
    public FlinkCoGroupOperator(CoGroupOperator<InputType0, InputType1, TypeKey> that) {
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

        final DataSetChannel.Instance input0 = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance input1 = (DataSetChannel.Instance) inputs[1];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        final DataSet<InputType0> datasetInput0 = input0.provideDataSet();
        final DataSet<InputType1> datasetInput1 = input1.provideDataSet();

        FunctionCompiler compiler = flinkExecutor.getCompiler();

        KeySelector<InputType0, TypeKey> function0 = compiler.compileKeySelector(this.keyDescriptor0);
        KeySelector<InputType1, TypeKey> function1 = compiler.compileKeySelector(this.keyDescriptor1);


        final DataSet<Tuple2<Iterable<InputType0>, Iterable<InputType1>>> datasetOutput = datasetInput0.coGroup(datasetInput1)
        .where(
            function0
        ).equalTo(
             function1
        ).with(
            new CoGroupFunction<InputType0, InputType1, Tuple2<Iterable<InputType0>, Iterable<InputType1>>>() {
                @Override
                public void coGroup (
                            Iterable<InputType0> iterable,
                            Iterable<InputType1> iterable1,
                            Collector< Tuple2<Iterable<InputType0>, Iterable<InputType1>> > collector
                ){
                    List<InputType0> list0 = new ArrayList<>();
                    List<InputType1> list1 = new ArrayList<>();
                    iterable.forEach(list0::add);
                    iterable1.forEach(list1::add);
                    collector.collect( new Tuple2<>(list0, list1));
                }
        })
        .setParallelism(flinkExecutor.fee.getParallelism())
        .returns(ReflectionUtils.specify(Tuple2.class));

        output.accept(datasetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCoGroupOperator<>(this);
    }

    public String getLoadProfileEstimatorConfigurationTypeKey() {
        return "wayang.flink.cogroup.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
