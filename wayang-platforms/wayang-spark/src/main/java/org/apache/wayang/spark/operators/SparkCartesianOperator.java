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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CartesianOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Spark implementation of the {@link CartesianOperator}.
 */
public class SparkCartesianOperator<InputType0, InputType1>
        extends CartesianOperator<InputType0, InputType1>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkCartesianOperator(DataSetType<InputType0> inputType0, DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkCartesianOperator(CartesianOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<InputType0> rdd0 = input0.provideRdd();
        final JavaRDD<InputType1> rdd1 = input1.provideRdd();
        final JavaPairRDD<InputType0, InputType1> cartesian = rdd0.cartesian(rdd1);
        this.name(cartesian);
        final JavaRDD<Tuple2<InputType0, InputType1>> crossProduct = cartesian
                .map(scalaTuple -> new Tuple2<>(scalaTuple._1, scalaTuple._2));
        this.name(crossProduct);

        output.accept(crossProduct, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCartesianOperator<>(this.getInputType0(), this.getInputType1());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.cartesian.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
