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

package org.apache.wayang.tensorflow.operators;

import org.apache.wayang.basic.operators.PredictOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.tensorflow.channels.TensorChannel;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;
import org.apache.wayang.tensorflow.model.TensorflowModel;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.types.family.TType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TensorflowPredictOperator extends PredictOperator<NdArray<?>, NdArray<?>> implements TensorflowExecutionOperator {

    public TensorflowPredictOperator() {
        super(DataSetType.createDefaultUnchecked(NdArray.class), DataSetType.createDefaultUnchecked(NdArray.class));
    }

    public TensorflowPredictOperator(PredictOperator<NdArray<?>, NdArray<?>> that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        }
        return Collections.singletonList(TensorChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(TensorChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            TensorflowExecutor tensorflowExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final CollectionChannel.Instance inputModel = (CollectionChannel.Instance) inputs[0];
        final TensorChannel.Instance inputData = (TensorChannel.Instance) inputs[1];
        final TensorChannel.Instance output = (TensorChannel.Instance) outputs[0];

        final TensorflowModel model = (TensorflowModel) inputModel.provideCollection().iterator().next();
        final NdArray<?> data = inputData.provideTensor();
        final TType predicted = model.predict(data);
        tensorflowExecutor.addResource((predicted));
        output.accept((NdArray<?>) predicted);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }
}
