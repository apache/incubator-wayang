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

package org.apache.wayang.tensorflow.execution;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.*;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.tensorflow.operators.TensorflowExecutionOperator;
import org.apache.wayang.tensorflow.platform.TensorflowPlatform;

import java.util.*;

/**
 * {@link Executor} implementation for the {@link TensorflowPlatform}.
 */
public class TensorflowExecutor extends ExecutorTemplate {
    private final TensorflowContextReference tensorflowContextReference;
    private final TensorflowPlatform platform;
    private final Configuration configuration;
    private final Job job;

    public TensorflowExecutor(TensorflowPlatform platform, Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.tensorflowContextReference = this.platform.getTensorflowContext(job);
        this.tensorflowContextReference.noteObtainedReference();
        this.configuration = job.getConfiguration();
        this.job = job;
    }

    @Override
    public void execute(ExecutionStage stage, OptimizationContext optimizationContext, ExecutionState executionState) {
        Queue<ExecutionTask> scheduledTasks = new LinkedList<>(stage.getStartTasks());
        Set<ExecutionTask> executedTasks = new HashSet<>();

        while (!scheduledTasks.isEmpty()) {
            final ExecutionTask task = scheduledTasks.poll();
            if (executedTasks.contains(task)) continue;
            this.execute(task, optimizationContext, executionState);
            executedTasks.add(task);
            Arrays.stream(task.getOutputChannels())
                    .flatMap(channel -> channel.getConsumers().stream())
                    .filter(consumer -> consumer.getStage() == stage)
                    .forEach(scheduledTasks::add);
        }
    }

    /**
     * Brings the given {@code task} into execution.
     */
    private void execute(ExecutionTask task, OptimizationContext optimizationContext, ExecutionState executionState) {
        final TensorflowExecutionOperator tensorflowExecutionOperator = (TensorflowExecutionOperator) task.getOperator();

        ChannelInstance[] inputChannelInstances = new ChannelInstance[task.getNumInputChannels()];
        for (int i = 0; i < inputChannelInstances.length; i++) {
            inputChannelInstances[i] = executionState.getChannelInstance(task.getInputChannel(i));
        }
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(tensorflowExecutionOperator);
        ChannelInstance[] outputChannelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int i = 0; i < outputChannelInstances.length; i++) {
            outputChannelInstances[i] = task.getOutputChannel(i).createInstance(this, operatorContext, i);
        }

        long startTime = System.currentTimeMillis();
        final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                tensorflowExecutionOperator.evaluate(inputChannelInstances, outputChannelInstances, this, operatorContext);
        long endTime = System.currentTimeMillis();

        final Collection<ExecutionLineageNode> executionLineageNodes = results.getField0();
        final Collection<ChannelInstance> producedChannelInstances = results.getField1();

        for (ChannelInstance outputChannelInstance : outputChannelInstances) {
            if (outputChannelInstance != null) {
                executionState.register(outputChannelInstance);
            }
        }

        final PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, endTime - startTime);
        executionState.add(partialExecution);
        this.registerMeasuredCardinalities(producedChannelInstances);
    }


    public void addResource(AutoCloseable resource) {
        this.tensorflowContextReference.addResource(resource);
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    @Override
    public void dispose() {
        super.dispose();
        this.tensorflowContextReference.noteDiscardedReference(true);
    }
}
