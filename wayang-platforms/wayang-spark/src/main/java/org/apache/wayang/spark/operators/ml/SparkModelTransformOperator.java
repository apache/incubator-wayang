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

package org.apache.wayang.spark.operators.ml;

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.ModelTransformOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.model.SparkMLModel;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Deprecated
public class SparkModelTransformOperator<X, Y> extends ModelTransformOperator<X, Y> implements SparkExecutionOperator {

    public SparkModelTransformOperator(DataSetType<X> inType, DataSetType<Tuple2<X, Y>> outType) {
        super(inType, outType);
    }

    public SparkModelTransformOperator(ModelTransformOperator<X, Y> that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        }
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final CollectionChannel.Instance inputModel = (CollectionChannel.Instance) inputs[0];
        final RddChannel.Instance inputData = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final SparkMLModel<X, Y> model = inputModel.<SparkMLModel<X, Y>>provideCollection().iterator().next();
        final JavaRDD<X> inputRdd = inputData.provideRdd();
        final JavaRDD<Tuple2<X, Y>> outputRdd = model.transform(inputRdd);

        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
