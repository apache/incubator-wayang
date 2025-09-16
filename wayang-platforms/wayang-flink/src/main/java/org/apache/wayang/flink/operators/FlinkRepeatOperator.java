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
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.wayang.basic.operators.RepeatOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Flink implementation of the {@link RepeatOperator}.
 */
public class FlinkRepeatOperator<Type>
        extends RepeatOperator<Type>
        implements FlinkExecutionOperator  {
    /**
     * Keeps track of the current iteration number.
     */
    private int iterationCounter;

    /**
     * Creates a new instance.
     */
    public FlinkRepeatOperator(int numIterations, DataSetType<Type> type) {
        super(numIterations, type);
    }

    public FlinkRepeatOperator(RepeatOperator<Type> that) {
        super(that);
    }

    private IterativeDataSet<Type> iterativeDataSet;

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        assert inputs[INITIAL_INPUT_INDEX] != null;
        assert inputs[ITERATION_INPUT_INDEX] != null;

        assert outputs[ITERATION_OUTPUT_INDEX] != null;
        assert outputs[FINAL_OUTPUT_INDEX] != null;

        try {

            switch (this.getState()) {
                case NOT_STARTED:
                    DataSet<Type> input_initial = ((DataSetChannel.Instance) inputs[INITIAL_INPUT_INDEX]).provideDataSet();
                    DataSetChannel.Instance output_iteration = ((DataSetChannel.Instance) outputs[ITERATION_OUTPUT_INDEX]);

                    this.iterativeDataSet = input_initial
                                .iterate(this.getNumIterations())
                                .setParallelism(flinkExecutor.fee.getParallelism());

                    output_iteration.accept(this.iterativeDataSet, flinkExecutor);
                    outputs[FINAL_OUTPUT_INDEX] = null;
                    this.iterationCounter = 0;

                    this.setState(State.RUNNING);
                    break;
                case RUNNING:
                    assert this.iterativeDataSet != null;
                    this.iterationCounter = this.getNumIterations();
                    DataSet<Type> input_iteration = ((DataSetChannel.Instance) inputs[ITERATION_INPUT_INDEX]).provideDataSet();
                    DataSetChannel.Instance output_final = ((DataSetChannel.Instance) outputs[FINAL_OUTPUT_INDEX]);
                    output_final.accept(this.iterativeDataSet
                            .setParallelism(flinkExecutor.fee.getParallelism())
                            .closeWith(input_iteration), flinkExecutor);
                    outputs[ITERATION_OUTPUT_INDEX] = null;
                    this.setState(State.FINISHED);

                    break;
                default:
                    throw new IllegalStateException(String.format("%s is finished, yet executed.", this));
            }
        }catch (Exception e){
            e.printStackTrace();
            System.exit(0);
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.repeat.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkRepeatOperator<>(this);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Collections.singletonList(DataSetChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }


    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

}
